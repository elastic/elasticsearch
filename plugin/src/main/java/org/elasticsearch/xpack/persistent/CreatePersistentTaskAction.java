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
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 *  This action can be used to add the record for the persistent action to the cluster state.
 */
public class CreatePersistentTaskAction extends Action<CreatePersistentTaskAction.Request,
        PersistentTaskResponse,
        CreatePersistentTaskAction.RequestBuilder> {

    public static final CreatePersistentTaskAction INSTANCE = new CreatePersistentTaskAction();
    public static final String NAME = "cluster:admin/persistent/create";

    private CreatePersistentTaskAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public PersistentTaskResponse newResponse() {
        return new PersistentTaskResponse();
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String action;

        private PersistentTaskRequest request;

        public Request() {

        }

        public Request(String action, PersistentTaskRequest request) {
            this.action = action;
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            action = in.readString();
            request = in.readNamedWriteable(PersistentTaskRequest.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(action);
            out.writeNamedWriteable(request);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (this.action == null) {
                validationException = addValidationError("action must be specified", validationException);
            }
            if (this.request == null) {
                validationException = addValidationError("request must be specified", validationException);
            }
            return validationException;
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

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        public PersistentTaskRequest getRequest() {
            return request;
        }

        public void setRequest(PersistentTaskRequest request) {
            this.request = request;
        }

    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<CreatePersistentTaskAction.Request,
            PersistentTaskResponse, CreatePersistentTaskAction.RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, CreatePersistentTaskAction action) {
            super(client, action, new Request());
        }

        public RequestBuilder setAction(String action) {
            request.setAction(action);
            return this;
        }

        public RequestBuilder setRequest(PersistentTaskRequest persistentTaskRequest) {
            request.setRequest(persistentTaskRequest);
            return this;
        }

    }

    public static class TransportAction extends TransportMasterNodeAction<Request, PersistentTaskResponse> {

        private final PersistentTasksClusterService persistentTasksClusterService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               PersistentTasksClusterService persistentTasksClusterService,
                               PersistentTasksExecutorRegistry persistentTasksExecutorRegistry,
                               PersistentTasksService persistentTasksService,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, CreatePersistentTaskAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.persistentTasksClusterService = persistentTasksClusterService;
            NodePersistentTasksExecutor executor = new NodePersistentTasksExecutor(threadPool);
            clusterService.addListener(new PersistentTasksNodeService(settings, persistentTasksService, persistentTasksExecutorRegistry,
                    transportService.getTaskManager(), threadPool, executor));
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.GENERIC;
        }

        @Override
        protected PersistentTaskResponse newResponse() {
            return new PersistentTaskResponse();
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // Cluster is not affected but we look up repositories in metadata
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected final void masterOperation(final Request request, ClusterState state,
                                             final ActionListener<PersistentTaskResponse> listener) {
            persistentTasksClusterService.createPersistentTask(request.action, request.request,
                    new ActionListener<PersistentTask<?>>() {

                @Override
                public void onResponse(PersistentTask<?> task) {
                    listener.onResponse(new PersistentTaskResponse(task));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }
}


