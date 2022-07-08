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

import java.util.Collection;
import java.util.Collections;

/**
 * ILM Provider implementation for the {@link ImmutableClusterStateHandlerProvider} service interface
 */
public class ILMImmutableStateHandlerProvider implements ImmutableClusterStateHandlerProvider {
    @Override
    public Collection<ImmutableClusterStateHandler<?>> handlers(Plugin loadedPlugin) {
        assert loadedPlugin instanceof IndexLifecycle;
        if (loadedPlugin instanceof IndexLifecycle indexLifecycle) {
            return indexLifecycle.immutableClusterStateHandlers();
        }

        return Collections.emptyList();
    }

    @Override
    public Class<? extends Plugin> parentPlugin() {
        return IndexLifecycle.class;
    }
}
