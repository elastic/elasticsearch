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
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * The coordinating transport action for fetch
 * Receives a ShardFetchSearchRequest wrapped in TransportFetchPhaseCoordinationAction.Request.
 */
public class TransportFetchPhaseCoordinationAction extends TransportAction<
    TransportFetchPhaseCoordinationAction.Request,
    TransportFetchPhaseCoordinationAction.Response> {


    public static final ActionType<TransportFetchPhaseCoordinationAction.Response> INSTANCE = new ActionType<>(
        "internal:search/fetch/coordination"
    );

    private final TransportService transportService;
    private final ActiveFetchPhaseTasks activeFetchPhaseTasks;

    public static class Request extends LegacyActionRequest {
        private final ShardFetchSearchRequest shardFetchRequest;

        public Request(ShardFetchSearchRequest shardFetchRequest) {
            this.shardFetchRequest = shardFetchRequest;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.shardFetchRequest = new ShardFetchSearchRequest(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardFetchRequest.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ShardFetchSearchRequest getShardFetchRequest() {
            return shardFetchRequest;
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
            //super.writeTo(out);
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
            INSTANCE.name(),
            actionFilters,
            transportService.getTaskManager(),
            transportService.getThreadPool().executor(ThreadPool.Names.SEARCH)
        );
        this.transportService = transportService;
        this.activeFetchPhaseTasks = activeFetchPhaseTasks;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {

        // Creates and registers a response stram for the coordinating task
        ShardFetchSearchRequest fetchReq = request.getShardFetchRequest();
        long coordinatingTaskID = task.getId();
        fetchReq.setCoordinatingNode(transportService.getLocalNode());
        fetchReq.setCoordinatingTaskId(coordinatingTaskID);

        int shardIndex = fetchReq.getShardSearchRequest().shardId().id();
        int expectedDocs = fetchReq.docIds().length;
        FetchPhaseResponseStream responseStream = new FetchPhaseResponseStream(shardIndex, expectedDocs);
        responseStream.incRef();
        Releasable registration = activeFetchPhaseTasks.registerResponseBuilder(coordinatingTaskID, responseStream);

        DiscoveryNode dataNode = transportService.getLocalNode(); // TODO replace with real routing

        // When the data node finishes (normal fetch response arrives), build a final FetchSearchResult
        // from the accumulated chunks and reply
        ActionListener<FetchSearchResult> childListener = ActionListener.wrap(
            dataNodeResult -> {
                try {
                    ShardSearchContextId ctxId = dataNodeResult.getContextId();
                    SearchShardTarget shardTarget = dataNodeResult.getSearchShardTarget();
                    listener.onResponse(new Response(responseStream.buildFinalResult(ctxId, shardTarget)));
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

        // Forward the fetch work to the data node
        transportService.sendChildRequest(
            dataNode,
            SearchTransportService.FETCH_ID_ACTION_NAME,
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
