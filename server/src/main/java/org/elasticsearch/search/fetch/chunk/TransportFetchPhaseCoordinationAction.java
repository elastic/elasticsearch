/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.search.SearchTransportService.FETCH_ID_ACTION_NAME;

public class TransportFetchPhaseCoordinationAction extends HandledTransportAction<
    TransportFetchPhaseCoordinationAction.Request,
    TransportFetchPhaseCoordinationAction.Response> {

    /*
     * Transport action that coordinates chunked fetch operations from the coordinator node.
     * Handles receiving chunks, accumulating them in order, and building the final result.
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
    private static final Logger LOGGER = LogManager.getLogger(TransportFetchPhaseCoordinationAction.class);

    public static final ActionType<Response> TYPE = new ActionType<>("internal:data/read/search/fetch/coordination");

    public static final TransportVersion CHUNKED_FETCH_PHASE = TransportVersion.fromName("chunked_fetch_phase");

    private final TransportService transportService;
    private final ActiveFetchPhaseTasks activeFetchPhaseTasks;
    private final CircuitBreakerService circuitBreakerService;

    /**
     * Required for deserializing SearchHits from chunk bytes that may contain NamedWriteable
     * fields (e.g., LookupField from lookup runtime fields). See {@link NamedWriteableAwareStreamInput}.
     */
    private final NamedWriteableRegistry namedWriteableRegistry;

    public static class Request extends ActionRequest {
        private final ShardFetchSearchRequest shardFetchRequest;
        private final DiscoveryNode dataNode;
        private final Map<String, String> headers;

        public Request(ShardFetchSearchRequest shardFetchRequest, DiscoveryNode dataNode, Map<String, String> headers) {
            this.shardFetchRequest = shardFetchRequest;
            this.dataNode = dataNode;
            this.headers = headers;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shardFetchRequest = new ShardFetchSearchRequest(in);
            this.dataNode = new DiscoveryNode(in);
            this.headers = in.readMap(StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardFetchRequest.writeTo(out);
            dataNode.writeTo(out);
            out.writeMap(headers, StreamOutput::writeString);
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

        public Map<String, String> getHeaders() {
            return headers;
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
        CircuitBreakerService circuitBreakerService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(TYPE.name(), transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.transportService = transportService;
        this.activeFetchPhaseTasks = activeFetchPhaseTasks;
        this.circuitBreakerService = circuitBreakerService;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    // Creates and registers a response stream for the coordinating task
    @Override
    public void doExecute(Task task, Request request, ActionListener<Response> listener) {
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
                BytesReference lastChunkBytes = dataNodeResult.getLastChunkBytes();
                int hitCount = dataNodeResult.getLastChunkHitCount();
                long lastChunkSequenceStart = dataNodeResult.getLastChunkSequenceStart();

                // Process the embedded last chunk if present
                if (lastChunkBytes != null && hitCount > 0) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(
                            "Received final chunk [{}] for shard [{}]",
                            hitCount,
                            request.shardFetchRequest.getShardSearchRequest().shardId()
                        );
                    }

                    // Track memory usage
                    int bytesSize = lastChunkBytes.length();
                    circuitBreaker.addEstimateBytesAndMaybeBreak(bytesSize, "fetch_chunk_accumulation");
                    responseStream.trackBreakerBytes(bytesSize);

                    try (StreamInput in = new NamedWriteableAwareStreamInput(lastChunkBytes.streamInput(), namedWriteableRegistry)) {
                        for (int i = 0; i < hitCount; i++) {
                            SearchHit hit = SearchHit.readFrom(in, false);

                            // Add with explicit sequence number
                            long hitSequence = lastChunkSequenceStart + i;
                            responseStream.addHitWithSequence(hit, hitSequence);
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

        final ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            for (var e : request.getHeaders().entrySet()) {
                final String key = e.getKey();
                final String value = e.getValue();
                final String existing = threadContext.getHeader(key);
                if (existing == null) {
                    threadContext.putHeader(key, value);
                } else {
                    assert existing.equals(value) : "header [" + key + "] already present with different value";
                }
            }

            final TaskId parent = task.getParentTaskId();
            if (parent != null && parent.isSet()) {
                fetchReq.setParentTask(parent);
            }

            transportService.sendRequest(
                request.getDataNode(),
                FETCH_ID_ACTION_NAME,
                fetchReq,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(childListener, FetchSearchResult::new, EsExecutors.DIRECT_EXECUTOR_SERVICE)
            );
        }
    }
}
