/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.shards.LocalRecoveryRetriesHealthInfo.RecoveryKey;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.health.node.tracker.HealthTracker;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;

import java.util.HashMap;
import java.util.Map;

/// A [HealthTracker] which captures the [LocalRecoveryRetriesHealthInfo] for a node.
public class LocalRecoveryRetriesHealthTracker extends HealthTracker<LocalRecoveryRetriesHealthInfo> {

    // TODO: Resolve any potential discrepancies associated with the fact that the health indicator consumes localRetries the same as
    // failedAllocations – but, at time of writing, the former is reset by direct cancellation and the latter is not.
    // See elastic/elasticsearch-team#5557

    private final IndicesService indicesService;

    public LocalRecoveryRetriesHealthTracker(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    protected LocalRecoveryRetriesHealthInfo determineCurrentHealth() {
        Map<RecoveryKey, Integer> localRetriesByRecovery = new HashMap<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                // The retry count is retained after the recovery completes, but the health indicator only reads the retry count for shards
                // which are not active in the routing table, so only those need to be reported:
                ShardRouting routing = shard.routingEntry();
                if (routing.active() == false) {
                    int localRetries = shard.recoveryState().getLocalRetries();
                    if (localRetries > 0) {
                        localRetriesByRecovery.put(RecoveryKey.of(routing), localRetries);
                    }
                }
            }
        }
        // TODO: Once either #160891 or whatever ends up doing that job merges, we should check IndicesClusterStateService.retryingShards
        // or shardCreationStates or whatever it ends up being, either as well as or instead of IndexShard.recoveryState.localRetries.
        return new LocalRecoveryRetriesHealthInfo(localRetriesByRecovery);
    }

    @Override
    protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, LocalRecoveryRetriesHealthInfo healthInfo) {
        builder.localRecoveryRetriesHealthInfo(healthInfo);
    }
}
