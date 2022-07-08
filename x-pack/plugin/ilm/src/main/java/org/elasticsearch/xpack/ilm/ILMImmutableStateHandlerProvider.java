/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.immutablestate.ImmutableClusterStateHandler;
import org.elasticsearch.immutablestate.ImmutableClusterStateHandlerProvider;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * ILM Provider implementation for the {@link ImmutableClusterStateHandlerProvider} service interface
 */
public class ILMImmutableStateHandlerProvider implements ImmutableClusterStateHandlerProvider {
    private static volatile Collection<ImmutableClusterStateHandler<?>> handlers = Collections.emptyList();

    @Override
    public Collection<ImmutableClusterStateHandler<?>> handlers() {
        return handlers;
    }

    public static void registerHandlers(ImmutableClusterStateHandler<?>... stateHandlers) {
        handlers = List.of(stateHandlers);
    }
}
