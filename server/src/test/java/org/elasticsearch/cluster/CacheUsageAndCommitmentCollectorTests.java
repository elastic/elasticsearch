/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CacheUsageAndCommitmentCollectorTests extends ESTestCase {

    public void testEmptyCollector() {
        final PlainActionFuture<Map<ShardId, BoostedAndUnboostedCacheSizes>> shardCacheSizesFuture = new PlainActionFuture<>();
        CacheUsageAndCommitmentCollector.EMPTY.collectShardCacheSizes(ClusterState.EMPTY_STATE, shardCacheSizesFuture);
        assertThat(safeGet(shardCacheSizesFuture), equalTo(Map.of()));

        final PlainActionFuture<Map<String, CurrentCacheUsage>> nodeCacheUsageFuture = new PlainActionFuture<>();
        CacheUsageAndCommitmentCollector.EMPTY.collectNodeCacheUsage(ClusterState.EMPTY_STATE, nodeCacheUsageFuture);
        assertThat(safeGet(nodeCacheUsageFuture), equalTo(Map.of()));
    }
}
