/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal action used by TransportPersistentAction to add the record for the persistent action to the cluster state.
 */
public class StartPersistentTaskAction extends Action<StartPersistentTaskAction.Request,
        PersistentActionResponse,
        StartPersistentTaskAction.RequestBuilder> {

    public static final StartPersistentTaskAction INSTANCE = new StartPersistentTaskAction();
    public static final String NAME = "cluster:admin/persistent/start";

    private StartPersistentTaskAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public PersistentActionResponse newResponse() {
        return new PersistentActionResponse();
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String action;

        private PersistentActionRequest request;

        public Request() {

        }

        public Request(String action, PersistentActionRequest request) {
            this.action = action;
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            action = in.readString();
            request = in.readOptionalNamedWriteable(PersistentActionRequest.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(action);
            out.writeOptionalNamedWriteable(request);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request1 = (Request) o;
            return Objects.equals(action, request1.action) &&
                    Objects.equals(request, request1.request);
        }

        @Override
        public int hashCode() {
            return Objects.hash(action, request);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<StartPersistentTaskAction.Request,
            PersistentActionResponse, StartPersistentTaskAction.RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, StartPersistentTaskAction action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, PersistentActionResponse> {

        private final PersistentTaskClusterService persistentTaskClusterService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               PersistentTaskClusterService persistentTaskClusterService,
                               PersistentActionRegistry persistentActionRegistry,
                               PersistentActionService persistentActionService,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, StartPersistentTaskAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.persistentTaskClusterService = persistentTaskClusterService;
            PersistentActionExecutor executor = new PersistentActionExecutor(threadPool);
            clusterService.addListener(new PersistentActionCoordinator(settings, persistentActionService, persistentActionRegistry,
                    transportService.getTaskManager(), executor));
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        protected PersistentActionResponse newResponse() {
            return new PersistentActionResponse();
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // Cluster is not affected but we look up repositories in metadata
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected final void masterOperation(final Request request, ClusterState state,
                                             final ActionListener<PersistentActionResponse> listener) {
            persistentTaskClusterService.createPersistentTask(request.action, request.request, new ActionListener<Long>() {
                @Override
                public void onResponse(Long newTaskId) {
                    listener.onResponse(new PersistentActionResponse(newTaskId));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }
}


