/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * This class implements the logic to "split", rather forward, Broadcast requests like refresh and flush
 * to the target nodes after a Resharding operation. It also implements the logic to combine responses from
 * source and target shards.
 *
 * The logic to split indexing requests is different because each document id has to be inspected to route it
 * to the correct shard post Resharding. That logic is implemented in
 * {@link org.elasticsearch.action.bulk.ShardBulkSplitHelper}
 */
public final class BroadcastRequestSplitHelper {
    private BroadcastRequestSplitHelper() {}

    /**
     * Given a stale Replication Request, like flush or refresh, split it into multiple requests,
     * one for the source shard and one for the target shard.
     * See {@link org.elasticsearch.action.bulk.ShardBulkSplitHelper} for how we split
     * {@link org.elasticsearch.action.bulk.BulkShardRequest}
     * We are here because there was a mismatch between the SplitShardCountSummary in the request
     * and that on the primary shard node.
     *
     * Note that {@link org.elasticsearch.cluster.metadata.IndexReshardingMetadata} cannot be NULL here
     * because it is evaluated in {@link ReplicationSplitHelper#needsSplitCoordination(Logger, ReplicationRequest, IndexMetadata)}
     * with the same metadata before arriving here. The request would have been rejected there if there was no Resharding metadata found.
     *
     * TODO:
     * We assume here that the request is exactly 1 reshard split behind
     * the current state. We might either revise this assumption or enforce it
     * in a follow up
     */
    public static <T extends ReplicationRequest<T>> Map<ShardId, T> splitRequest(
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
        assert indexMetadata.getReshardingMetadata() != null;
        int targetShardId = indexMetadata.getReshardingMetadata().getSplit().targetShard(sourceShard.id());
        ShardId targetShard = new ShardId(sourceShard.getIndex(), targetShardId);

        requestsByShard.put(targetShard, targetRequestFactory.apply(targetShard, shardCountSummary));
        return requestsByShard;
    }

    public static <T extends ReplicationRequest<T>> Tuple<ReplicationResponse, Exception> combineSplitResponses(
        T originalRequest,
        Map<ShardId, T> splitRequests,
        Map<ShardId, Tuple<ReplicationResponse, Exception>> responses
    ) {
        int failed = 0;
        int successful = 0;
        int total = 0;
        List<ReplicationResponse.ShardInfo.Failure> failures = new ArrayList<>();

        // If the action fails on either one of the shards, we return an exception.
        // Case 1: Both source and target shards return a response: Add up total, successful, failures
        // Case 2: Both source and target shards return an exception : return exception
        // Case 3: One shard returns a response, the other returns an exception : return exception
        for (Map.Entry<ShardId, Tuple<ReplicationResponse, Exception>> entry : responses.entrySet()) {
            Tuple<ReplicationResponse, Exception> value = entry.getValue();
            Exception exception = value.v2();

            if (exception != null) {
                return new Tuple<>(null, exception);
            }

            ReplicationResponse response = value.v1();
            failed += response.getShardInfo().getFailed();
            successful += response.getShardInfo().getSuccessful();
            total += response.getShardInfo().getTotal();
            Collections.addAll(failures, response.getShardInfo().getFailures());
        }

        ReplicationResponse.ShardInfo.Failure[] failureArray = failures.toArray(new ReplicationResponse.ShardInfo.Failure[0]);
        assert failureArray.length == failed;

        ReplicationResponse.ShardInfo shardInfo = ReplicationResponse.ShardInfo.of(total, successful, failureArray);

        ReplicationResponse response = new ReplicationResponse();
        response.setShardInfo(shardInfo);
        return new Tuple<>(response, null);
    }
}
