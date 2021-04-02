/*
 * Copyright 2017 Google LLC
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google LLC nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.google.api.gax.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * A {@link ManagedChannel} that will send requests round robin via a set of channels.
 *
 * <p>Package-private for internal use.
 */
class ChannelPool extends ManagedChannel {
  private static final Logger logger = Logger.getLogger(ChannelPool.class.getName());
  // size greater than 1 to allow multiple channel to refresh at the same time
  // size not too large so refreshing channels doesn't use too many threads
  private static final int CHANNEL_REFRESH_EXECUTOR_SIZE = 2;
  private final ImmutableList<ManagedChannel> channels;
  private final AtomicInteger indexTicker = new AtomicInteger();
  private final String authority;
  // if set, ChannelPool will manage the life cycle of channelRefreshExecutorService
  @Nullable private ScheduledExecutorService channelRefreshExecutorService;
  // Map from an affinity key to a healthy (fallback) channel index.
  @GuardedBy("this")
  private final HashMap<Integer, Integer> fallbackMap;
  // Map from a healthy channel index to affinity keys which are temporarily remapped to this
  // channel.
  @GuardedBy("this")
  private final HashMap<Integer, Set<Integer>> hostedAffinities;
  // Map from a broken channel index to their affected affinity keys that were remapped.
  @GuardedBy("this")
  private final HashMap<Integer, Set<Integer>> brokenChannels;
  // Maximum number of channels allowed to redirect requests to other channels while reconnecting.
  private final int maxFallbackChannels;

  private void log(String message) {
    logger.fine(String.format("FALLBACK FEATURE: [%d] %s", this.hashCode(), message));
  }

  /**
   * ChannelStateMonitor subscribes to channel's state changes and informs {@link ChannelPool} on
   * any new state except SHUTDOWN. This monitor is used when `maxFallbackChannels` > 0 to detect
   * when channel is not ready and temporarily route requests via healthy channels.
   *
   * <p>SHUTDOWN state can happen in two cases:
   *
   * <ol>
   *   <li>When spanner client terminates. In this case we do not need to react to state change
   *       because all the channels are shutting down.
   *   <li>When {@link RefreshingManagedChannel} is used and the channel is refreshing. In this case
   *       we will start monitoring the new channel and we don't need to react to SHUTDOWN on the
   *       old channel.
   * </ol>
   */
  private class ChannelStateMonitor implements Runnable {
    private final int channelIndex;
    private ManagedChannel channel;

    private ChannelStateMonitor(int channelIndex) {
      this.channelIndex = channelIndex;
    }

    @Override
    public void run() {
      if (channel == null) {
        return;
      }
      ConnectivityState newState = channel.getState(true);
      log(String.format("Channel %d changed state to %s", channelIndex, newState));
      if (newState != ConnectivityState.SHUTDOWN) {
        processChannelStateChange(channelIndex, newState);
        channel.notifyWhenStateChanged(newState, this);
      }
    }

    public void startMonitoring(ManagedChannel channel) {
      this.channel = channel;
      run();
    }
  }

  private void processChannelStateChange(int index, ConnectivityState state) {
    if (state == ConnectivityState.READY) {
      if (brokenChannels.containsKey(index)) {
        channelRecovered(index);
      }
      return;
    }
    if (!brokenChannels.containsKey(index)) {
      channelBroke(index);
    }
  }

  private void logBrokenChannels() {
    StringBuilder list = new StringBuilder();
    for (Integer i : brokenChannels.keySet()) {
      list.append(" ");
      list.append(i.toString());
    }
    log(String.format("Currently broken channels: %s", list));
  }

  private void channelBroke(int index) {
    synchronized (this) {
      if (brokenChannels.containsKey(index)) {
        return;
      }
      log(String.format("Channel %d is considered broken", index));
      brokenChannels.put(index, new HashSet<Integer>());
      // If any affinities were temporarily remapped to this channel we need to clear that mapping
      // because this channel is not healthy anymore.
      Set<Integer> hosted = hostedAffinities.get(index);
      if (hosted == null) {
        return;
      }
      for (Integer integer : hosted) {
        fallbackMap.remove(integer);
      }
      hostedAffinities.remove(index);
      logBrokenChannels();
    }
  }

  private void channelRecovered(int index) {
    synchronized (this) {
      if (!brokenChannels.containsKey(index)) {
        return;
      }
      log(String.format("Channel %d is considered recovered", index));
      for (Integer affinity : brokenChannels.get(index)) {
        Integer hostChannel = fallbackMap.get(affinity);
        hostedAffinities.get(hostChannel).remove(affinity);
        fallbackMap.remove(affinity);
      }
      brokenChannels.remove(index);
      logBrokenChannels();
    }
  }

  /**
   * Factory method to create a non-refreshing channel pool
   *
   * @param poolSize number of channels in the pool
   * @param channelFactory method to create the channels
   * @return ChannelPool of non refreshing channels
   */
  static ChannelPool create(int poolSize, final ChannelFactory channelFactory) throws IOException {
    return create(poolSize, channelFactory, 0);
  }

  /**
   * Factory method to create a non-refreshing channel pool
   *
   * @param poolSize number of channels in the pool
   * @param channelFactory method to create the channels
   * @param maxFallbackChannels maximum number of channels allowed to redirect requests while
   *     reconnecting
   * @return ChannelPool of non refreshing channels
   */
  static ChannelPool create(
      int poolSize, final ChannelFactory channelFactory, int maxFallbackChannels)
      throws IOException {
    return new ChannelPool(poolSize, channelFactory, null, maxFallbackChannels);
  }

  /**
   * Factory method to create a refreshing channel pool
   *
   * <p>Package-private for testing purposes only
   *
   * @param poolSize number of channels in the pool
   * @param channelFactory method to create the channels
   * @param channelRefreshExecutorService periodically refreshes the channels; its life cycle will
   *     be managed by ChannelPool
   * @return ChannelPool of refreshing channels
   */
  @VisibleForTesting
  static ChannelPool createRefreshing(
      int poolSize,
      final ChannelFactory channelFactory,
      ScheduledExecutorService channelRefreshExecutorService)
      throws IOException {
    return new ChannelPool(poolSize, channelFactory, channelRefreshExecutorService, 0);
  }

  /**
   * Factory method to create a refreshing channel pool
   *
   * @param poolSize number of channels in the pool
   * @param channelFactory method to create the channels
   * @param maxFallbackChannels maximum number of channels allowed to redirect requests while
   *     reconnecting
   * @return ChannelPool of refreshing channels
   */
  static ChannelPool createRefreshing(
      int poolSize, final ChannelFactory channelFactory, int maxFallbackChannels)
      throws IOException {
    return new ChannelPool(
        poolSize,
        channelFactory,
        Executors.newScheduledThreadPool(CHANNEL_REFRESH_EXECUTOR_SIZE),
        maxFallbackChannels);
  }

  /**
   * Factory method to create a refreshing channel pool
   *
   * @param poolSize number of channels in the pool
   * @param channelFactory method to create the channels
   * @return ChannelPool of refreshing channels
   */
  static ChannelPool createRefreshing(int poolSize, final ChannelFactory channelFactory)
      throws IOException {
    return new ChannelPool(
        poolSize,
        channelFactory,
        Executors.newScheduledThreadPool(CHANNEL_REFRESH_EXECUTOR_SIZE),
        0);
  }

  /**
   * Initializes the channel pool. Assumes that all channels have the same authority.
   *
   * @param poolSize number of channels in the pool
   * @param channelFactory method to create the channels
   * @param channelRefreshExecutorService periodically refreshes the channels
   * @param maxFallbackChannels maximum number of channels allowed to redirect requests while
   *     reconnecting
   */
  private ChannelPool(
      int poolSize,
      final ChannelFactory channelFactory,
      @Nullable ScheduledExecutorService channelRefreshExecutorService,
      int maxFallbackChannels)
      throws IOException {
    logger.fine(String.format("CREATING NEW IMPROVED CHANNEL POOL WITH UPTO %d FALLBACK CHANNELS...", maxFallbackChannels));
    this.maxFallbackChannels = Math.min(maxFallbackChannels, poolSize - 1);
    this.channelRefreshExecutorService = channelRefreshExecutorService;
    fallbackMap = new HashMap<>();
    hostedAffinities = new HashMap<>(poolSize + 1, 1);
    brokenChannels = new HashMap<>(poolSize + 1, 1);

    List<ManagedChannel> channels = new ArrayList<>();
    for (int i = 0; i < poolSize; i++) {
      ManagedChannel channel;
      ChannelFactory factory = proxiedChannelFactory(i, channelFactory);
      if (channelRefreshExecutorService != null) {
        channel = new RefreshingManagedChannel(factory, channelRefreshExecutorService);
      } else {
        channel = factory.createSingleChannel();
      }
      channels.add(channel);
    }

    this.channels = ImmutableList.copyOf(channels);
    authority = channels.get(0).authority();
  }

  /**
   * If fallback channels allowed creates {@link ChannelStateMonitor} and injects monitoring after
   * channel creation.
   */
  private ChannelFactory proxiedChannelFactory(
      final int index, final ChannelFactory channelFactory) {
    if (maxFallbackChannels < 1) {
      return channelFactory;
    }
    return new ChannelFactory() {
      @Override
      public ManagedChannel createSingleChannel() throws IOException {
        ManagedChannel ch = channelFactory.createSingleChannel();
        ChannelStateMonitor monitor = new ChannelStateMonitor(index);
        monitor.startMonitoring(ch);
        return ch;
      }
    };
  }

  /** {@inheritDoc} */
  @Override
  public String authority() {
    return authority;
  }

  /**
   * Create a {@link ClientCall} on a Channel from the pool chosen in a round-robin fashion to the
   * remote operation specified by the given {@link MethodDescriptor}. The returned {@link
   * ClientCall} does not trigger any remote behavior until {@link
   * ClientCall#start(ClientCall.Listener, io.grpc.Metadata)} is invoked.
   */
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions) {
    return getNextChannel().newCall(methodDescriptor, callOptions);
  }

  /** {@inheritDoc} */
  @Override
  public ManagedChannel shutdown() {
    for (ManagedChannel channelWrapper : channels) {
      channelWrapper.shutdown();
    }
    if (channelRefreshExecutorService != null) {
      channelRefreshExecutorService.shutdown();
    }
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isShutdown() {
    for (ManagedChannel channel : channels) {
      if (!channel.isShutdown()) {
        return false;
      }
    }
    if (channelRefreshExecutorService != null && !channelRefreshExecutorService.isShutdown()) {
      return false;
    }
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isTerminated() {
    for (ManagedChannel channel : channels) {
      if (!channel.isTerminated()) {
        return false;
      }
    }
    if (channelRefreshExecutorService != null && !channelRefreshExecutorService.isTerminated()) {
      return false;
    }
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public ManagedChannel shutdownNow() {
    for (ManagedChannel channel : channels) {
      channel.shutdownNow();
    }
    if (channelRefreshExecutorService != null) {
      channelRefreshExecutorService.shutdownNow();
    }
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    long endTimeNanos = System.nanoTime() + unit.toNanos(timeout);
    for (ManagedChannel channel : channels) {
      long awaitTimeNanos = endTimeNanos - System.nanoTime();
      if (awaitTimeNanos <= 0) {
        break;
      }
      channel.awaitTermination(awaitTimeNanos, TimeUnit.NANOSECONDS);
    }
    if (channelRefreshExecutorService != null) {
      long awaitTimeNanos = endTimeNanos - System.nanoTime();
      channelRefreshExecutorService.awaitTermination(awaitTimeNanos, TimeUnit.NANOSECONDS);
    }
    return isTerminated();
  }

  /**
   * Performs a simple round robin on the list of {@link ManagedChannel}s in the {@code channels}
   * list.
   *
   * @return A {@link ManagedChannel} that can be used for a single RPC call.
   */
  private ManagedChannel getNextChannel() {
    return getChannel(indexTicker.getAndIncrement());
  }

  /** Converts an affinity key to a channel index. */
  private int getChannelIndex(int affinity) {
    int index = affinity % channels.size();
    index = Math.abs(index);
    // If index is the most negative int, abs(index) is still negative.
    if (index < 0) {
      index = 0;
    }
    return index;
  }

  /**
   * Returns one of the channels managed by this pool. The pool continues to "own" the channel, and
   * the caller should not shut it down.
   *
   * @param affinity Two calls to this method with the same affinity returns the same channel. The
   *     reverse is not true: Two calls with different affinities might return the same channel.
   *     However, the implementation should attempt to spread load evenly. If fallback channels
   *     allowed and the channel returned breaks the next call with the same affinity will return a
   *     different (fallback) channel and subsequent calls will return the same fallback channel (as
   *     long as it stays healthy) until the original channel recovers.
   * @return A {@link ManagedChannel} for the affinity key.
   */
  ManagedChannel getChannel(int affinity) {
    int index = getChannelIndex(affinity);
    if (maxFallbackChannels < 1) {
      return channels.get(index);
    }
    return getHealthyChannel(index, affinity);
  }

  /**
   * Tries to remap an affinity key of a broken channel.
   *
   * @return A healthy channel index or the original broken channel index if unsuccessful.
   */
  private int fallbackAffinity(int brokenIndex, int affinity) {
    synchronized (this) {
      Integer fallbackChannelIndex = fallbackMap.get(affinity);
      if (fallbackChannelIndex != null) {
        log(String.format("Fallback for channel %d, affinity %d found as %d", brokenIndex, affinity, fallbackChannelIndex));
        return fallbackChannelIndex;
      }
      if (brokenChannels.size() > maxFallbackChannels) {
        // To many broken channels -- do not remap.
        log(String.format("Cannot fallback for channel %d, affinity %d. Too many broken channels", brokenIndex, affinity));
        return brokenIndex;
      }
      int healthyIndex;
      do {
        healthyIndex = getChannelIndex(indexTicker.getAndIncrement());
      } while (brokenChannels.containsKey(healthyIndex));
      fallbackMap.put(affinity, healthyIndex);
      appendToMap(hostedAffinities, healthyIndex, affinity);
      appendToMap(brokenChannels, brokenIndex, affinity);
      log(String.format("Fallback for channel %d, affinity %d created as %d", brokenIndex, affinity, healthyIndex));
      return healthyIndex;
    }
  }

  private void appendToMap(HashMap<Integer, Set<Integer>> map, int key, int value) {
    Set<Integer> list = map.get(key);
    if (list != null) {
      list.add(value);
    } else {
      list = new HashSet<>();
      list.add(value);
      map.put(key, list);
    }
  }

  /**
   * Tries to get a healthy channel for an affinity key. If there are more broken channels than
   * allowed to have a fallback returns the original channel for the affinity key.
   */
  private ManagedChannel getHealthyChannel(int index, int affinity) {
    if (!brokenChannels.containsKey(index)) {
      return channels.get(index);
    }
    log(String.format("Look up fallback for channel %d, affinity %d", index, affinity));
    Integer fallbackChannelIndex = fallbackMap.get(affinity);
    if (fallbackChannelIndex != null) {
      log(String.format("Fallback for channel %d, affinity %d found as %d", index, affinity, fallbackChannelIndex));
      return channels.get(fallbackChannelIndex);
    }
    if (brokenChannels.size() > maxFallbackChannels) {
      // To many broken channels -- do not remap.
      log(String.format("Cannot fallback for channel %d, affinity %d. Too many broken channels", index, affinity));
      return channels.get(index);
    }
    fallbackChannelIndex = fallbackAffinity(index, affinity);
    return channels.get(fallbackChannelIndex);
  }
}
