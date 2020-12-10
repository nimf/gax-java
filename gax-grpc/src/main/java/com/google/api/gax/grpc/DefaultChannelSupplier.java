package com.google.api.gax.grpc;

import io.grpc.ManagedChannelBuilder;

class DefaultChannelSupplier implements ChannelSupplier {
  @Override
  public ManagedChannelBuilder<?> forAddress(String host, int port) {
    return ManagedChannelBuilder.forAddress(host, port);
  }
}
