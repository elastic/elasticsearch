/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.function.Supplier;

public class TestStatelessCommitService extends StatelessCommitService {

    private volatile Strategy strategy = new Strategy();

    public TestStatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        IndicesService indicesService,
        Client client,
        StatelessCommitCleaner commitCleaner,
        StatelessSharedBlobCacheService cacheService,
        SharedBlobCacheWarmingService cacheWarmingService,
        TelemetryProvider telemetryProvider
    ) {
        super(
            settings,
            objectStoreService,
            clusterService,
            indicesService,
            client,
            commitCleaner,
            cacheService,
            cacheWarmingService,
            telemetryProvider
        );
    }

    public Strategy getStrategy() {
        return strategy;
    }

    public void setStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public ActionListener<Void> markRelocating(ShardId shardId, long minRelocatedGeneration, ActionListener<Void> listener) {
        return getStrategy().markRelocating(
            () -> super.markRelocating(shardId, minRelocatedGeneration, listener),
            shardId,
            minRelocatedGeneration,
            listener
        );
    }

    public static class Strategy {
        public ActionListener<Void> markRelocating(
            Supplier<ActionListener<Void>> originalSupplier,
            ShardId shardId,
            long minRelocatedGeneration,
            ActionListener<Void> listener
        ) {
            return originalSupplier.get();
        }
    }
}
