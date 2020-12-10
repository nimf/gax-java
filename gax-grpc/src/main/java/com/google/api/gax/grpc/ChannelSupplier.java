package com.google.api.gax.grpc;

import io.grpc.ManagedChannelBuilder;

public interface ChannelSupplier {
  ManagedChannelBuilder<?> forAddress(String host, int port);
}
