/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * The coordinating transport action for fetch
 * Receives a ShardFetchSearchRequest wrapped in TransportFetchPhaseCoordinationAction.Request.
 */
public class TransportFetchPhaseCoordinationAction extends HandledTransportAction<
    TransportFetchPhaseCoordinationAction.Request,
    TransportFetchPhaseCoordinationAction.Response> {

    public static final ActionType<Response> TYPE = new ActionType<>("internal:data/read/search/fetch/coordination");

    private final TransportService transportService;
    private final ActiveFetchPhaseTasks activeFetchPhaseTasks;

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
            //super(in);
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
        ActiveFetchPhaseTasks activeFetchPhaseTasks
    ) {
        super(
            TYPE.name(),
            transportService,
            actionFilters,
            Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.transportService = transportService;
        this.activeFetchPhaseTasks = activeFetchPhaseTasks;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {

        // Creates and registers a response stram for the coordinating task
        ShardFetchSearchRequest fetchReq = request.getShardFetchRequest();
        DiscoveryNode dataNode = request.getDataNode();
        long coordinatingTaskId = task.getId();

        // Set coordinator information on the request
        fetchReq.setCoordinatingNode(transportService.getLocalNode());
        fetchReq.setCoordinatingTaskId(coordinatingTaskId);

        // Create and register response stream
        int shardId = fetchReq.getShardSearchRequest().shardId().id();
        int expectedDocs = fetchReq.docIds().length;
        FetchPhaseResponseStream responseStream = new FetchPhaseResponseStream(shardId, expectedDocs);
        responseStream.incRef();

        Releasable registration = activeFetchPhaseTasks.registerResponseBuilder(
            coordinatingTaskId,
            shardId,
            responseStream
        );

        // Create listener that builds final result from accumulated chunks
        ActionListener<FetchSearchResult> childListener = ActionListener.wrap(
            dataNodeResult -> {
                try {
                    // Data node has finished - build final result from chunks
                    ShardSearchContextId ctxId = dataNodeResult.getContextId();
                    SearchShardTarget shardTarget = dataNodeResult.getSearchShardTarget();
                    FetchSearchResult finalResult = responseStream.buildFinalResult(ctxId, shardTarget);
                    listener.onResponse(new Response(finalResult));
                } catch (Exception e) {
                    listener.onFailure(e);
                } finally {
                    registration.close();
                    responseStream.decRef();
                }
            },
            e -> {
                try {
                    listener.onFailure(e);
                } finally {
                    registration.close();
                    responseStream.decRef();
                }
            }
        );

        // Forward request to data node using the existing FETCH_ID_ACTION_NAME
        // The data node will see coordinator info and automatically use chunking
        transportService.sendChildRequest(
            dataNode,
            "indices:data/read/search[phase/fetch/id]", // FETCH_ID_ACTION_NAME
            fetchReq,
            task,
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                childListener,
                FetchSearchResult::new,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            )
        );
    }
}
