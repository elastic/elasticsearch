/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.plugin;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Set;

/**
 * Plugin that provides shards availability health indicator functionality to Elasticsearch.
 * <p>
 * This plugin registers a health indicator service that monitors the availability of shards
 * across the cluster and reports their health status through the Health API.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Plugin is automatically loaded by Elasticsearch plugin system
 * // Access health status via Health API:
 * GET /_health_report/shards_availability
 * }</pre>
 */
public class ShardsAvailabilityPlugin extends Plugin implements HealthPlugin {

    private final SetOnce<ShardsAvailabilityHealthIndicatorService> shardHealthService = new SetOnce<>();

    /**
     * Constructs a new ShardsAvailabilityPlugin instance.
     * <p>
     * This constructor is called by the Elasticsearch plugin system during plugin initialization.
     * </p>
     */
    public ShardsAvailabilityPlugin() {}

    /**
     * Creates and initializes plugin components required for shards availability monitoring.
     * <p>
     * This method instantiates the {@link ShardsAvailabilityHealthIndicatorService} using the
     * cluster service, allocation service, system indices, and project resolver provided by
     * the plugin framework.
     * </p>
     *
     * @param services the plugin services providing access to core Elasticsearch functionality
     * @return a collection containing the initialized shards availability health indicator service
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called automatically by Elasticsearch during plugin initialization
     * Collection<?> components = plugin.createComponents(pluginServices);
     * }</pre>
     */
    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.shardHealthService.set(
            new ShardsAvailabilityHealthIndicatorService(
                services.clusterService(),
                services.allocationService(),
                services.systemIndices(),
                services.projectResolver()
            )
        );
        return Set.of(this.shardHealthService.get());
    }

    /**
     * Returns the collection of health indicator services provided by this plugin.
     * <p>
     * This method provides the shards availability health indicator service to the Health API,
     * enabling monitoring of shard availability across the Elasticsearch cluster.
     * </p>
     *
     * @return a collection containing the shards availability health indicator service
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Called by Elasticsearch Health API framework
     * Collection<HealthIndicatorService> indicators = plugin.getHealthIndicatorServices();
     * for (HealthIndicatorService indicator : indicators) {
     *     HealthIndicatorResult result = indicator.calculate();
     * }
     * }</pre>
     */
    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        return Set.of(this.shardHealthService.get());
    }
}
