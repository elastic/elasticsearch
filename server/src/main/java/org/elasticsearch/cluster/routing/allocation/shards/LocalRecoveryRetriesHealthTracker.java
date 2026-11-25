/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardAllocationId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.health.node.tracker.HealthTracker;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/// A [HealthTracker] which captures the [LocalRecoveryRetriesHealthInfo] for a node.
public class LocalRecoveryRetriesHealthTracker extends HealthTracker<LocalRecoveryRetriesHealthInfo> {

    // TODO: Resolve any potential discrepancies associated with the fact that the health indicator consumes localRetries the same as
    // failedAllocations – but, at time of writing, the former is reset by direct cancellation and the latter is not.
    // See elastic/elasticsearch-team#5557

    private final ClusterService clusterService;
    private final Supplier<IndicesClusterStateService> indicesClusterStateService;

    public LocalRecoveryRetriesHealthTracker(
        ClusterService clusterService,
        Supplier<IndicesClusterStateService> indicesClusterStateService
    ) {
        this.clusterService = clusterService;
        this.indicesClusterStateService = indicesClusterStateService;
    }

    @Override
    protected LocalRecoveryRetriesHealthInfo determineCurrentHealth() {
        assert indicesClusterStateService.get() != null
            : "the supplier is only read once health monitoring starts, which is driven by a cluster state "
                + "update and so cannot happen before node construction (and hence the injector) completes";
        IndicesClusterStateService clusterStateService = indicesClusterStateService.get();

        ClusterState state = clusterService.state();
        RoutingNode localRoutingNode = state.getRoutingNodes().node(state.nodes().getLocalNodeId());
        if (localRoutingNode == null) {
            return LocalRecoveryRetriesHealthInfo.EMPTY;
        }

        Map<ShardAllocationId, Integer> localRetriesByRecovery = new HashMap<>();
        for (ShardRouting routing : localRoutingNode) {
            if (routing.active() == false) {
                int localRetries = clusterStateService.getLocalRecoveryRetryCount(routing);
                if (localRetries > 0) {
                    localRetriesByRecovery.put(ShardAllocationId.fromRouting(routing), localRetries);
                }
            }
        }
        return new LocalRecoveryRetriesHealthInfo(localRetriesByRecovery);
    }

    @Override
    protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, LocalRecoveryRetriesHealthInfo healthInfo) {
        builder.localRecoveryRetriesHealthInfo(healthInfo);
    }
}
