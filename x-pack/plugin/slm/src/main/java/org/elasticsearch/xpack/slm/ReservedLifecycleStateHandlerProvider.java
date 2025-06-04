/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.ReservedStateHandlerProvider;

import java.util.Collection;

/**
 * SLM Provider implementation for the {@link ReservedStateHandlerProvider} service interface
 */
public class ReservedLifecycleStateHandlerProvider implements ReservedStateHandlerProvider {
    private final SnapshotLifecycle plugin;

    public ReservedLifecycleStateHandlerProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public ReservedLifecycleStateHandlerProvider(SnapshotLifecycle plugin) {
        this.plugin = plugin;
    }

    @Override
    public Collection<ReservedClusterStateHandler<?>> clusterHandlers() {
        return plugin.reservedClusterStateHandlers();
    }
}
