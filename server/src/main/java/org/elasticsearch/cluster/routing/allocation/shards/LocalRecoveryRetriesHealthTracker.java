/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.health.node.tracker.HealthTracker;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;

import java.util.HashMap;
import java.util.Map;

/// A [HealthTracker] which captures the [LocalRecoveryRetriesHealthInfo] for a node.
public class LocalRecoveryRetriesHealthTracker extends HealthTracker<LocalRecoveryRetriesHealthInfo> {

    private final IndicesService indicesService;

    public LocalRecoveryRetriesHealthTracker(IndicesService indicesService) {
        this.indicesService = indicesService;
    }

    @Override
    protected LocalRecoveryRetriesHealthInfo determineCurrentHealth() {
        Map<ShardId, Integer> localRetriesByShard = new HashMap<>();
        for (IndexService indexService : indicesService) {
            for (IndexShard shard : indexService) {
                // The retry count is retained after the recovery completes, but the health indicator only reads the retry count for shards
                // which are not active in the routing table, so only those need to be reported:
                if (shard.routingEntry().active() == false) {
                    int localRetries = shard.recoveryState().getLocalRetries();
                    if (localRetries > 0) {
                        localRetriesByShard.put(shard.shardId(), localRetries);
                    }
                }
            }
        }
        return new LocalRecoveryRetriesHealthInfo(localRetriesByShard);
    }

    @Override
    protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, LocalRecoveryRetriesHealthInfo healthInfo) {
        builder.localRecoveryRetriesHealthInfo(healthInfo);
    }
}
