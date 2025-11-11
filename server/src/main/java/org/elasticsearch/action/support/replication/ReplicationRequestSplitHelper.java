/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public final class ReplicationRequestSplitHelper {
    private ReplicationRequestSplitHelper() {}

    // We are here because there was a mismatch between the SplitShardCountSummary in the request
    // and that on the primary shard node. We assume that the request is exactly 1 reshard split behind
    // the current state.
    public static <T extends ReplicationRequest> Map<ShardId, T> splitRequestCommon(
        T request,
        ProjectMetadata project,
        BiFunction<ShardId, SplitShardCountSummary, T> targetRequestFactory
    ) {
        final ShardId sourceShard = request.shardId();
        IndexMetadata indexMetadata = project.getIndexSafe(sourceShard.getIndex());
        SplitShardCountSummary shardCountSummary = SplitShardCountSummary.forIndexing(indexMetadata, sourceShard.getId());

        Map<ShardId, T> requestsByShard = new HashMap<>();
        requestsByShard.put(sourceShard, request);

        // Create a request for original source shard and for each target shard.
        // New requests that are to be handled by target shards should contain the
        // latest ShardCountSummary.
        int targetShardId = indexMetadata.getReshardingMetadata().getSplit().targetShard(sourceShard.id());
        ShardId targetShard = new ShardId(sourceShard.getIndex(), targetShardId);

        requestsByShard.put(targetShard, targetRequestFactory.apply(targetShard, shardCountSummary));
        return requestsByShard;
    }
}
