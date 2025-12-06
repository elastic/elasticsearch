/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ShardBulkSplitHelper {

    private ShardBulkSplitHelper() {}

    /**
     * Splits a bulk request into multiple requests for each shard. If the items in the request only route to the source shard it will
     * return the original request. If the items only route to the target shard it will return a map with one request. If the requests
     * route to both the map will have a request for each shard.
     */
    public static Map<ShardId, BulkShardRequest> splitRequests(BulkShardRequest request, ProjectMetadata project) {
        final ShardId sourceShardId = request.shardId();
        final Index index = sourceShardId.getIndex();
        IndexMetadata indexMetadata = project.getIndexSafe(index);
        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
        SplitShardCountSummary shardCountSummary = SplitShardCountSummary.forIndexing(indexMetadata, request.shardId().getId());

        Map<ShardId, List<BulkItemRequest>> requestsByShard = new HashMap<>();
        Map<ShardId, BulkShardRequest> bulkRequestsPerShard = new HashMap<>();

        // Iterate through the items in the input request and split them based on the
        // current resharding-split state.
        BulkItemRequest[] items = request.items();
        if (items.length == 0) {  // Nothing to split
            return Map.of(sourceShardId, request);
        }

        for (BulkItemRequest bulkItemRequest : items) {
            DocWriteRequest<?> docWriteRequest = bulkItemRequest.request();
            int newShardId = docWriteRequest.rerouteAtSourceDuringResharding(indexRouting);
            List<BulkItemRequest> shardRequests = requestsByShard.computeIfAbsent(
                new ShardId(index, newShardId),
                shardNum -> new ArrayList<>()
            );
            shardRequests.add(new BulkItemRequest(bulkItemRequest.id(), bulkItemRequest.request()));
        }

        // All items belong to either the source shard or target shard.
        if (requestsByShard.size() == 1) {
            // Return the original request if no items were split to target. Note that
            // this original request still contains the stale SplitShardCountSummary.
            // This is alright because we hold primary indexing permits while calling this split
            // method and we execute this request on the primary without letting go of the indexing permits.
            // This means that a second split cannot occur in the meantime.
            if (requestsByShard.containsKey(sourceShardId)) {
                return Map.of(sourceShardId, request);
            }
        }

        // Create a new BulkShardRequest(s) with the updated SplitShardCountSummary. This is because
        // we do not hold primary permits on the target shard, and hence it can proceed with
        // a second split operation while this request is still pending. We must verify the
        // SplitShardCountSummary again on the target.
        for (Map.Entry<ShardId, List<BulkItemRequest>> entry : requestsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final List<BulkItemRequest> requests = entry.getValue();
            BulkShardRequest bulkShardRequest = new BulkShardRequest(
                shardId,
                shardCountSummary,
                request.getRefreshPolicy(),
                requests.toArray(new BulkItemRequest[0]),
                request.isSimulated()
            );
            bulkRequestsPerShard.put(shardId, bulkShardRequest);
        }
        return bulkRequestsPerShard;
    }

    public static Tuple<BulkShardResponse, Exception> combineResponses(
        BulkShardRequest originalRequest,
        Map<ShardId, BulkShardRequest> splitRequests,
        Map<ShardId, Tuple<BulkShardResponse, Exception>> responses
    ) {
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[originalRequest.items().length];
        for (Map.Entry<ShardId, Tuple<BulkShardResponse, Exception>> entry : responses.entrySet()) {
            ShardId shardId = entry.getKey();
            Tuple<BulkShardResponse, Exception> value = entry.getValue();
            Exception exception = value.v2();
            if (exception != null) {
                BulkShardRequest bulkShardRequest = splitRequests.get(shardId);
                for (BulkItemRequest item : bulkShardRequest.items()) {
                    DocWriteRequest<?> request = item.request();
                    BulkItemResponse.Failure failure = new BulkItemResponse.Failure(item.index(), request.id(), exception);
                    bulkItemResponses[item.id()] = BulkItemResponse.failure(item.id(), request.opType(), failure);
                }
            } else {
                for (BulkItemResponse bulkItemResponse : value.v1().getResponses()) {
                    bulkItemResponses[bulkItemResponse.getItemId()] = bulkItemResponse;
                }
            }
        }
        BulkShardResponse bulkShardResponse = new BulkShardResponse(originalRequest.shardId(), bulkItemResponses);
        // TODO: Decide how to handle
        bulkShardResponse.setShardInfo(responses.get(originalRequest.shardId()).v1().getShardInfo());
        return new Tuple<>(bulkShardResponse, null);
    }
}
