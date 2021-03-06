package com.outbrain.ob1k.server.endpoints;

import com.outbrain.ob1k.common.filters.ServiceFilter;
import com.outbrain.ob1k.server.builder.ExtensionBuilder;
import com.outbrain.ob1k.server.builder.ServerBuilderState;

public class EndpointMappingServiceBuilder implements ExtensionBuilder {

  private final String path;
  private final ServiceFilter[] filters;

  public static EndpointMappingServiceBuilder registerMappingService(final String path, final ServiceFilter... filters) {
    return new EndpointMappingServiceBuilder(path, filters);
  }

  private EndpointMappingServiceBuilder(final String path, final ServiceFilter... filters) {
    this.path = path;
    this.filters = filters;
  }

  @Override
  public void apply(final ServerBuilderState state) {
    state.addServiceDescriptor(new EndpointMappingService(state.getRegistry()), path, filters);
  }
}
