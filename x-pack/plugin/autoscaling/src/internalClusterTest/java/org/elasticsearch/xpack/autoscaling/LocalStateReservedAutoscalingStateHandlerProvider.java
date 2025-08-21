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
 * Mock autoscaling provider implementation for the {@link ReservedStateHandlerProvider} service interface
 * <p>
 * This class is a test version of the {@link ReservedAutoscalingStateHandlerProvider}. When we load handler providers through
 * our custom SPI interface, we must match the plugin type exactly. With MockNode, when we run
 * {@link org.elasticsearch.test.ESIntegTestCase} test cases, the version of the {@link Autoscaling} plugin
 * is {@link LocalStateAutoscaling}, therefore we need to provide a test version of this class.
 */
public class LocalStateReservedAutoscalingStateHandlerProvider implements ReservedStateHandlerProvider {
    private final LocalStateAutoscaling plugin;

    public LocalStateReservedAutoscalingStateHandlerProvider() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public LocalStateReservedAutoscalingStateHandlerProvider(LocalStateAutoscaling plugin) {
        this.plugin = plugin;
    }

    @Override
    public Collection<ReservedClusterStateHandler<?>> clusterHandlers() {
        return plugin.testPlugin().reservedClusterStateHandlers();
    }
}
