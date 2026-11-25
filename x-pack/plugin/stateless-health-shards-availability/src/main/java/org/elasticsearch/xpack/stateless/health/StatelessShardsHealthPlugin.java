/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.health;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.plugins.HealthPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.Collection;
import java.util.Set;

public class StatelessShardsHealthPlugin extends Plugin implements HealthPlugin {

    private final SetOnce<ShardsAvailabilityHealthIndicatorService> shardHealthService = new SetOnce<>();

    public StatelessShardsHealthPlugin() {}

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.shardHealthService.set(
            new StatelessShardsAvailabilityHealthIndicatorService(
                services.clusterService(),
                services.allocationService(),
                services.systemIndices(),
                services.projectResolver()
            )
        );
        return Set.of(this.shardHealthService.get());
    }

    @Override
    public Collection<HealthIndicatorService> getHealthIndicatorServices() {
        return Set.of(this.shardHealthService.get());
    }
}
