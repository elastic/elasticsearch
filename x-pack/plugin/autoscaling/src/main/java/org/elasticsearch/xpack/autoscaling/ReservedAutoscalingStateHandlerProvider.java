/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.ReservedStateHandlerProvider;

import java.util.Collection;

/**
 * Autoscaling provider implementation for the {@link ReservedStateHandlerProvider} service interface
 */
public class ReservedAutoscalingStateHandlerProvider implements ReservedStateHandlerProvider {
    private final Autoscaling plugin;

    public ReservedAutoscalingStateHandlerProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public ReservedAutoscalingStateHandlerProvider(Autoscaling plugin) {
        this.plugin = plugin;
    }

    @Override
    public Collection<ReservedClusterStateHandler<?>> clusterHandlers() {
        return plugin.reservedClusterStateHandlers();
    }
}
