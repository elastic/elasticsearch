/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.elasticsearch.reservedstate.ReservedProjectStateHandler;
import org.elasticsearch.reservedstate.ReservedStateHandlerProvider;

import java.util.Collection;

/**
 * ILM Provider implementation for the {@link ReservedStateHandlerProvider} service interface
 */
public class ReservedLifecycleStateHandlerProvider implements ReservedStateHandlerProvider {
    private final IndexLifecycle plugin;

    public ReservedLifecycleStateHandlerProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public ReservedLifecycleStateHandlerProvider(IndexLifecycle plugin) {
        this.plugin = plugin;
    }

    @Override
    public Collection<ReservedProjectStateHandler<?>> projectHandlers() {
        return plugin.reservedProjectStateHandlers();
    }
}
