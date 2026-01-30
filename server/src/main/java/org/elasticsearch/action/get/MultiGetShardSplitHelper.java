/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.get;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static org.elasticsearch.core.Strings.format;

/**
 * Helper class for coordinating multi-get shard requests during index resharding/splitting.
 * Similar to ReplicationSplitHelper, this handles the logic for detecting stale requests
 * and splitting them across multiple shards when the coordinator's view is one split behind.
 *
 * This class provides both utility methods for splitting/combining requests and coordination
 * logic for executing split requests.
 */
public final class MultiGetShardSplitHelper {

    private final Logger logger;
    private final ProjectMetadata projectMetadata;
    private final BiConsumer<MultiGetShardRequest, ActionListener<MultiGetShardResponse>> executeRequest;

    private MultiGetShardSplitHelper() {
        throw new UnsupportedOperationException("Use constructor with parameters");
    }

    public MultiGetShardSplitHelper(
        Logger logger,
        ProjectMetadata projectMetadata,
        BiConsumer<MultiGetShardRequest, ActionListener<MultiGetShardResponse>> executeRequest
    ) {
        this.logger = logger;
        this.projectMetadata = projectMetadata;
        this.executeRequest = executeRequest;
    }

    /**
     * Checks if the request needs split coordination based on shard count mismatch.
     */
    public static boolean needsSplitCoordination(MultiGetShardRequest request, IndexMetadata indexMetadata, int shardId) {
        SplitShardCountSummary requestSplitSummary = request.getSplitShardCountSummary();
        if (requestSplitSummary.isUnset()) {
            return false;
        }

        // First check using forSearch (SPLIT minState) as this is what the coordinator uses
        SplitShardCountSummary currentSearchSummary = SplitShardCountSummary.forSearch(indexMetadata, shardId);
        if (requestSplitSummary.equals(currentSearchSummary) == false) {
            return true;
        }

        // If they match using forSearch, also check using forIndexing (HANDOFF minState).
        // This handles the case where targets are in HANDOFF state (ready for indexing)
        // but not yet in SPLIT state (ready for search). Documents may have been indexed
        // to targets, so we need to coordinate with them even though they're not "search ready".
        SplitShardCountSummary currentIndexingSummary = SplitShardCountSummary.forIndexing(indexMetadata, shardId);
        return requestSplitSummary.equals(currentIndexingSummary) == false;
    }

    /**
     * Coordinates the split multi-get request execution.
     */
    public void coordinateSplitRequest(
        MultiGetShardRequest request,
        ShardId shardId,
        IndexMetadata indexMetadata,
        ActionListener<MultiGetShardResponse> listener
    ) {
        new SplitCoordinator(request, shardId, indexMetadata, listener).coordinate();
    }

    /**
     * Splits a multi-get shard request into multiple requests for each shard. If the items in the request only route to the source
     * shard it will return the original request. If the items only route to the target shard it will return a map with one request.
     * If the requests route to both the map will have a request for each shard.
     */
    public static Map<ShardId, MultiGetShardRequest> splitRequests(MultiGetShardRequest request, ProjectMetadata project) {
        final IndexMetadata indexMetadata = project.index(request.index());
        final Index index = indexMetadata.getIndex();
        final ShardId sourceShardId = new ShardId(index, request.shardId());

        IndexRouting indexRouting = IndexRouting.fromIndexMetadata(indexMetadata);
        // Use forIndexing (HANDOFF minState) when splitting requests to ensure we can coordinate
        // with targets that may have received forwarded index operations
        SplitShardCountSummary shardCountSummary = SplitShardCountSummary.forIndexing(indexMetadata, sourceShardId.getId());

        Map<ShardId, List<Tuple<Integer, MultiGetRequest.Item>>> itemsByShard = new HashMap<>();
        Map<ShardId, MultiGetShardRequest> requestsPerShard = new HashMap<>();

        // Iterate through the items in the input request and split them based on the
        // current resharding-split state.
        List<MultiGetRequest.Item> items = request.items;
        if (items.isEmpty()) {  // Nothing to split
            return Map.of(sourceShardId, request);
        }

        for (int i = 0; i < items.size(); i++) {
            MultiGetRequest.Item item = items.get(i);
            int newShardId = indexRouting.getShard(item.id(), item.routing());
            ShardId targetShardId = new ShardId(index, newShardId);
            List<Tuple<Integer, MultiGetRequest.Item>> shardItems = itemsByShard.computeIfAbsent(
                targetShardId,
                shardId -> new ArrayList<>()
            );
            shardItems.add(new Tuple<>(request.locations.get(i), item));
        }

        // All items belong to either the source shard or target shard.
        if (itemsByShard.size() == 1) {
            // Return the original request if no items were split to target. Note that
            // this original request still contains the stale SplitShardCountSummary.
            if (itemsByShard.containsKey(sourceShardId)) {
                return Map.of(sourceShardId, request);
            }
        }

        // Create a new MultiGetShardRequest(s) with the updated SplitShardCountSummary.
        for (Map.Entry<ShardId, List<Tuple<Integer, MultiGetRequest.Item>>> entry : itemsByShard.entrySet()) {
            final ShardId shardId = entry.getKey();
            final List<Tuple<Integer, MultiGetRequest.Item>> shardItems = entry.getValue();

            MultiGetShardRequest shardRequest = new MultiGetShardRequest(
                new MultiGetRequest()
                    .preference(request.preference())
                    .realtime(request.realtime())
                    .refresh(request.refresh())
                    .setForceSyntheticSource(request.isForceSyntheticSource()),
                shardId.getIndexName(),
                shardId.getId(),
                shardCountSummary
            );

            for (Tuple<Integer, MultiGetRequest.Item> locationAndItem : shardItems) {
                shardRequest.add(locationAndItem.v1(), locationAndItem.v2());
            }

            requestsPerShard.put(shardId, shardRequest);
        }
        return requestsPerShard;
    }

    /**
     * Combines responses from multiple split requests back into a single response preserving
     * the original request order.
     */
    public static Tuple<MultiGetShardResponse, Exception> combineResponses(
        MultiGetShardRequest originalRequest,
        Map<ShardId, MultiGetShardRequest> splitRequests,
        Map<ShardId, Tuple<MultiGetShardResponse, Exception>> responses
    ) {
        MultiGetShardResponse combinedResponse = new MultiGetShardResponse();
        Map<Integer, Tuple<GetResponse, MultiGetResponse.Failure>> responsesByLocation = new HashMap<>();

        responses.forEach((shardId, responseTuple) -> {
            Exception exception = responseTuple.v2();
            MultiGetShardRequest splitRequest = splitRequests.get(shardId);

            if (exception != null) {
                // Create failures for all items in this shard request
                for (int i = 0; i < splitRequest.locations.size(); i++) {
                    int location = splitRequest.locations.get(i);
                    MultiGetRequest.Item item = splitRequest.items.get(i);
                    MultiGetResponse.Failure failure = new MultiGetResponse.Failure(
                        splitRequest.index(),
                        item.id(),
                        exception
                    );
                    responsesByLocation.put(location, new Tuple<>(null, failure));
                }
            } else {
                // Add successful responses
                MultiGetShardResponse shardResponse = responseTuple.v1();
                for (int i = 0; i < shardResponse.locations.size(); i++) {
                    int location = shardResponse.locations.get(i);
                    GetResponse getResponse = shardResponse.responses.get(i);
                    MultiGetResponse.Failure failure = shardResponse.failures.get(i);
                    responsesByLocation.put(location, new Tuple<>(getResponse, failure));
                }
            }
        });

        // Add responses in the original order
        for (int location : originalRequest.locations) {
            Tuple<GetResponse, MultiGetResponse.Failure> responseOrFailure = responsesByLocation.get(location);
            if (responseOrFailure != null) {
                if (responseOrFailure.v1() != null) {
                    combinedResponse.add(location, responseOrFailure.v1());
                } else {
                    combinedResponse.add(location, responseOrFailure.v2());
                }
            }
        }

        return new Tuple<>(combinedResponse, null);
    }

    /**
     * Inner class that handles the coordination of split requests.
     */
    public class SplitCoordinator {
        private final MultiGetShardRequest originalRequest;
        private final ShardId shardId;
        private final IndexMetadata indexMetadata;
        private final ActionListener<MultiGetShardResponse> listener;

        public SplitCoordinator(
            MultiGetShardRequest originalRequest,
            ShardId shardId,
            IndexMetadata indexMetadata,
            ActionListener<MultiGetShardResponse> listener
        ) {
            this.originalRequest = originalRequest;
            this.shardId = shardId;
            this.indexMetadata = indexMetadata;
            this.listener = listener;
        }

        public void coordinate() {
            // Validate that we're only one split behind (we always double the shard count)
            SplitShardCountSummary requestSummary = originalRequest.getSplitShardCountSummary();

            // Check against forIndexing (HANDOFF minState) since that's what determines
            // whether documents could have been forwarded to targets
            SplitShardCountSummary currentIndexingSummary = SplitShardCountSummary.forIndexing(indexMetadata, shardId.getId());

            int requestCount = requestSummary.asInt();
            int currentIndexingCount = currentIndexingSummary.asInt();

            // Check if we're more than one split behind based on indexing state
            if (currentIndexingCount != requestCount && currentIndexingCount != requestCount * 2) {
                listener.onFailure(
                    new IllegalStateException(
                        format(
                            "Multi-get request is too stale. Coordinating node has shard count %d but current shard count is %d. "
                                + "Expected at most one split difference (count should be %d or %d).",
                            requestCount,
                            currentIndexingCount,
                            requestCount,
                            requestCount * 2
                        )
                    )
                );
                return;
            }

            // We're exactly one split behind - proceed with splitting and forwarding
            Map<ShardId, MultiGetShardRequest> splitRequests = MultiGetShardSplitHelper.splitRequests(originalRequest, projectMetadata);

            logger.debug(
                "Multi-get request for shard {} needs split coordination. Request count: {}, current count: {}. Split into {} requests.",
                shardId,
                requestCount,
                currentIndexingCount,
                splitRequests.size()
            );

            if (splitRequests.size() == 1) {
                // All items route to one shard, execute directly
                Map.Entry<ShardId, MultiGetShardRequest> entry = splitRequests.entrySet().iterator().next();
                MultiGetShardRequest splitRequest = entry.getValue();
                executeRequest.accept(splitRequest, listener);
            } else {
                // Need to execute on multiple shards
                coordinateMultipleShardRequests(splitRequests);
            }
        }

        private void coordinateMultipleShardRequests(Map<ShardId, MultiGetShardRequest> splitRequests) {
            Map<ShardId, Tuple<MultiGetShardResponse, Exception>> results = new ConcurrentHashMap<>(splitRequests.size());
            CountDown countDown = new CountDown(splitRequests.size());

            for (Map.Entry<ShardId, MultiGetShardRequest> entry : splitRequests.entrySet()) {
                ShardId targetShardId = entry.getKey();
                MultiGetShardRequest splitRequest = entry.getValue();

                ActionListener<MultiGetShardResponse> shardListener = new ActionListener<>() {
                    @Override
                    public void onResponse(MultiGetShardResponse response) {
                        results.put(targetShardId, new Tuple<>(response, null));
                        if (countDown.countDown()) {
                            finish();
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        results.put(targetShardId, new Tuple<>(null, e));
                        if (countDown.countDown()) {
                            finish();
                        }
                    }

                    private void finish() {
                        Tuple<MultiGetShardResponse, Exception> finalResponse = MultiGetShardSplitHelper.combineResponses(
                            originalRequest,
                            splitRequests,
                            results
                        );
                        if (finalResponse.v1() != null) {
                            listener.onResponse(finalResponse.v1());
                        } else {
                            listener.onFailure(finalResponse.v2());
                        }
                    }
                };

                try {
                    executeRequest.accept(splitRequest, shardListener);
                } catch (Exception e) {
                    shardListener.onFailure(e);
                }
            }
        }
    }
}