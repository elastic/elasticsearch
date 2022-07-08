/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.immutablestate.ImmutableClusterStateHandler;
import org.elasticsearch.immutablestate.ImmutableClusterStateHandlerProvider;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * ILM Provider implementation for the {@link ImmutableClusterStateHandlerProvider} service interface
 */
public class ILMImmutableStateHandlerProvider implements ImmutableClusterStateHandlerProvider {
    @Override
    public Collection<ImmutableClusterStateHandler<?>> handlers(Collection<? extends Plugin> loadedPlugins) {
        List<ImmutableClusterStateHandler<?>> result = new ArrayList<>();
        // this will be much nicer with switch expressions
        for (var plugin : loadedPlugins) {
            if (plugin instanceof IndexLifecycle indexLifecycle) {
                result.addAll(indexLifecycle.immutableClusterStateHandlers());
            }
        }
        return result;
    }

    @Override
    public Collection<Class<? extends Plugin>> providedPlugins() {
        return List.of(IndexLifecycle.class);
    }
}
