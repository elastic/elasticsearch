/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancingWeightsFactory;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * An extension point for {@link Plugin} implementations to customer behavior of cluster management.
 */
public interface ClusterPlugin {

    /**
     * Return deciders used to customize where shards are allocated.
     *
     * @param settings Settings for the node
     * @param clusterSettings Settings for the cluster
     * @return Custom {@link AllocationDecider} instances
     */
    default Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return Collections.emptyList();
    }

    /**
     * Return {@link ShardsAllocator} implementations added by this plugin.
     *
     * The key of the returned {@link Map} is the name of the allocator, and the value
     * is a function to construct the allocator.
     *
     * @param settings Settings for the node
     * @param clusterSettings Settings for the cluster
     * @return A map of allocator implementations
     */
    @Deprecated(forRemoval = true)
    default Map<String, Supplier<ShardsAllocator>> getShardsAllocators(Settings settings, ClusterSettings clusterSettings) {
        return Collections.emptyMap();
    }

    /**
     * Return {@link ExistingShardsAllocator} implementations added by this plugin; the index setting
     * {@link ExistingShardsAllocator#EXISTING_SHARDS_ALLOCATOR_SETTING} sets the key of the allocator to use to allocate its shards. The
     * default allocator is {@link org.elasticsearch.gateway.GatewayAllocator}.
     */
    default Map<String, ExistingShardsAllocator> getExistingShardsAllocators() {
        return Collections.emptyMap();
    }

    default Collection<WriteLoadForecaster> createWriteLoadForecasters(
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        return Collections.emptyList();
    }

    default ShardRoutingRoleStrategy getShardRoutingRoleStrategy() {
        return null;
    }

    default BalancingWeightsFactory getBalancingWeightsFactory(BalancerSettings balancerSettings, ClusterSettings clusterSettings) {
        return null;
    }

    /**
     * Called when the node is started
     */
    default void onNodeStarted() {}

}
