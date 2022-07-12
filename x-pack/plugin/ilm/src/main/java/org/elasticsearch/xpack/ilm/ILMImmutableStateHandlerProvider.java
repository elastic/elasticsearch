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

/**
 * ILM Provider implementation for the {@link ImmutableClusterStateHandlerProvider} service interface
 */
public class ILMImmutableStateHandlerProvider implements ImmutableClusterStateHandlerProvider {
    private final IndexLifecycle plugin;

    public ILMImmutableStateHandlerProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public ILMImmutableStateHandlerProvider(IndexLifecycle plugin) {
        this.plugin = plugin;
    }

    @Override
    public Collection<ImmutableClusterStateHandler<?>> handlers() {
        return plugin.immutableClusterStateHandlers();
    }
}
