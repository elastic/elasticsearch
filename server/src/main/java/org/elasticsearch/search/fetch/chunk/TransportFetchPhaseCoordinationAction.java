/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportFetchPhaseCoordinationAction extends HandledTransportAction<
    TransportFetchPhaseCoordinationAction.Request,
    TransportFetchPhaseCoordinationAction.Response> {

    /*
     * Transport action that coordinates chunked fetch operations from the coordinator node.
     * <p>
     * This action orchestrates the chunked fetch flow by:
     * <ol>
     *   <li>Registering a {@link FetchPhaseResponseStream} for accumulating chunks</li>
     *   <li>Setting coordinator information on the fetch request</li>
     *   <li>Sending the request to the data node via the standard fetch transport action</li>
     *   <li>Building the final result from accumulated chunks when the data node completes</li>
     * </ol>
     * <p>
     * +-------------------+                  +-------------+                          +-----------+
     * | FetchSearchPhase  |                  | Coordinator |                          | Data Node |
     * +-------------------+                  +-------------+                          +-----------+
     *      |                                     |                                          |
     *      |- execute(request, dataNode)-------->|                                          | --[Initialization Phase]
     *      |                                     |---[ShardFetchRequest]------------------->|
     *      |                                     |                                          | --[[Chunked Streaming Phase]
     *      |                                     |<---[HITS chunk 1]------------------------|
     *      |                                     |----[ACK (Empty)]------------------------>|
     *      |                                     |       ....                               |
     *      |                                     |<---[HITS chunk N]------------------------|
     *      |                                     |----[ACK (Empty)]------------------------>|
     *      |                                     |                                          | --[Completion Phase]
     *      |                                     |<--FetchSearchResult----------------------|
     *      |                                     |   (final response)                       |
     *      |                                     |                                          |
     *      |                                     |--[Build final result]                    |
     *      |                                     |  (from accumulated chunks)               |
     *      |<-- FetchSearchResult (complete) ----|                                          |
     */

    public static final ActionType<Response> TYPE = new ActionType<>("internal:data/read/search/fetch/coordination");

    public static final TransportVersion CHUNKED_FETCH_PHASE = TransportVersion.fromName("chunked_fetch_phase");

    private final TransportService transportService;
    private final ActiveFetchPhaseTasks activeFetchPhaseTasks;
    private final CircuitBreakerService circuitBreakerService;

    public static class Request extends ActionRequest {
        private final ShardFetchSearchRequest shardFetchRequest;
        private final DiscoveryNode dataNode;

        public Request(ShardFetchSearchRequest shardFetchRequest, DiscoveryNode dataNode) {
            this.shardFetchRequest = shardFetchRequest;
            this.dataNode = dataNode;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shardFetchRequest = new ShardFetchSearchRequest(in);
            this.dataNode = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardFetchRequest.writeTo(out);
            dataNode.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ShardFetchSearchRequest getShardFetchRequest() {
            return shardFetchRequest;
        }

        public DiscoveryNode getDataNode() {
            return dataNode;
        }
    }

    public static class Response extends ActionResponse {
        private final FetchSearchResult result;

        public Response(FetchSearchResult result) {
            this.result = result;
        }

        public Response(StreamInput in) throws IOException {
            this.result = new FetchSearchResult(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            result.writeTo(out);
        }

        public FetchSearchResult getResult() {
            return result;
        }
    }

    @Inject
    public TransportFetchPhaseCoordinationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ActiveFetchPhaseTasks activeFetchPhaseTasks,
        CircuitBreakerService circuitBreakerService
    ) {
        super(TYPE.name(), transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.activeFetchPhaseTasks = activeFetchPhaseTasks;
        this.circuitBreakerService = circuitBreakerService;
    }

    // Creates and registers a response stream for the coordinating task
    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final long coordinatingTaskId = task.getId();

        // Set coordinator information on the request
        final ShardFetchSearchRequest fetchReq = request.getShardFetchRequest();
        fetchReq.setCoordinatingNode(transportService.getLocalNode());
        fetchReq.setCoordinatingTaskId(coordinatingTaskId);

        // Create and register response stream
        assert fetchReq.getShardSearchRequest() != null;
        ShardId shardId = fetchReq.getShardSearchRequest().shardId();
        int expectedDocs = fetchReq.docIds().length;

        CircuitBreaker circuitBreaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);
        FetchPhaseResponseStream responseStream = new FetchPhaseResponseStream(shardId.getId(), expectedDocs, circuitBreaker);
        Releasable registration = activeFetchPhaseTasks.registerResponseBuilder(coordinatingTaskId, shardId, responseStream);

        // Listener that builds final result from accumulated chunks
        ActionListener<FetchSearchResult> childListener = ActionListener.wrap(dataNodeResult -> {
            try {
                // Process the embedded last chunk if present
                SearchHits lastChunk = dataNodeResult.hits();
                if (lastChunk != null && lastChunk.getHits().length > 0) {

                    if (logger.isTraceEnabled()) {
                        logger.info(
                            "Received final chunk [{}] for shard [{}]",
                            lastChunk.getHits().length,
                            request.shardFetchRequest.getShardSearchRequest().shardId()
                        );
                    }

                    // Add last chunk hits to the stream
                    for (SearchHit hit : lastChunk.getHits()) {
                        hit.incRef();
                        responseStream.addHit(hit);

                        // Track circuit breaker for last chunk
                        BytesReference sourceRef = hit.getSourceRef();
                        if (sourceRef != null) {
                            int hitBytes = sourceRef.length() * 2;
                            circuitBreaker.addEstimateBytesAndMaybeBreak(hitBytes, "fetch_last_chunk");
                            responseStream.trackBreakerBytes(hitBytes);
                        }
                    }
                }

                // Build final result from all accumulated hits
                FetchSearchResult finalResult = responseStream.buildFinalResult(
                    dataNodeResult.getContextId(),
                    dataNodeResult.getSearchShardTarget(),
                    dataNodeResult.profileResult()
                );

                ActionListener.respondAndRelease(listener.map(Response::new), finalResult);
            } catch (Exception e) {
                listener.onFailure(e);
            } finally {
                registration.close();
                responseStream.decRef();
            }
        }, e -> {
            try {
                listener.onFailure(e);
            } finally {
                registration.close();
                responseStream.decRef();
            }
        });

        // Forward request to data node using the existing FETCH_ID_ACTION_NAME
        transportService.sendChildRequest(
            request.getDataNode(),
            "indices:data/read/search[phase/fetch/id]",
            fetchReq,
            task,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(childListener, FetchSearchResult::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
        );
    }
}
