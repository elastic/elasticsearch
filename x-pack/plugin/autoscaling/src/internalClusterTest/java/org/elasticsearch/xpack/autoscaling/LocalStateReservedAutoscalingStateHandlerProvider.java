/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.reservedstate.ReservedClusterStateHandler;
import org.elasticsearch.reservedstate.ReservedClusterStateHandlerProvider;

import java.util.Collection;
import java.util.Objects;

/**
 * Autoscaling provider implementation for the {@link ReservedClusterStateHandlerProvider} service interface
 */
public class LocalStateReservedAutoscalingStateHandlerProvider implements ReservedClusterStateHandlerProvider {
    private final LocalStateAutoscaling plugin;

    public LocalStateReservedAutoscalingStateHandlerProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public LocalStateReservedAutoscalingStateHandlerProvider(LocalStateAutoscaling plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalStateReservedAutoscalingStateHandlerProvider that = (LocalStateReservedAutoscalingStateHandlerProvider) o;
        return plugin.equals(that.plugin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(plugin);
    }

    @Override
    public Collection<ReservedClusterStateHandler<?>> handlers() {
        return plugin.testPlugin().reservedClusterStateHandlers();
    }
}
