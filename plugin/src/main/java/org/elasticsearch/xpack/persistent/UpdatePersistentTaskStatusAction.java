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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

public class UpdatePersistentTaskStatusAction extends Action<UpdatePersistentTaskStatusAction.Request,
        UpdatePersistentTaskStatusAction.Response,
        UpdatePersistentTaskStatusAction.RequestBuilder> {

    public static final UpdatePersistentTaskStatusAction INSTANCE = new UpdatePersistentTaskStatusAction();
    public static final String NAME = "cluster:admin/persistent/update_status";

    private UpdatePersistentTaskStatusAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends MasterNodeRequest<Request> {

        private long taskId;
        
        private Task.Status status;

        public Request() {

        }

        public Request(long taskId, Task.Status status) {
            this.taskId = taskId;
            this.status = status;
        }

        public void setTaskId(long taskId) {
            this.taskId = taskId;
        }

        public void setStatus(Task.Status status) {
            this.status = status;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            taskId = in.readLong();
            status = in.readOptionalNamedWriteable(Task.Status.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(taskId);
            out.writeOptionalNamedWriteable(status);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return taskId == request.taskId && 
                    Objects.equals(status, request.status);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, status);
        }
    }

    public static class Response extends AcknowledgedResponse {
        public Response() {
            super();
        }

        public Response(boolean acknowledged) {
            super(acknowledged);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            readAcknowledged(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            writeAcknowledged(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AcknowledgedResponse that = (AcknowledgedResponse) o;
            return isAcknowledged() == that.isAcknowledged();
        }

        @Override
        public int hashCode() {
            return Objects.hash(isAcknowledged());
        }

    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<UpdatePersistentTaskStatusAction.Request,
            UpdatePersistentTaskStatusAction.Response, UpdatePersistentTaskStatusAction.RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, UpdatePersistentTaskStatusAction action) {
            super(client, action, new Request());
        }

        public final RequestBuilder setTaskId(long taskId) {
            request.setTaskId(taskId);
            return this;
        }

        public final RequestBuilder setStatus(Task.Status status) {
            request.setStatus(status);
            return this;
        }

    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final PersistentTaskClusterService persistentTaskClusterService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               PersistentTaskClusterService persistentTaskClusterService,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, UpdatePersistentTaskStatusAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.persistentTaskClusterService = persistentTaskClusterService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.MANAGEMENT;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            // Cluster is not affected but we look up repositories in metadata
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected final void masterOperation(final Request request, ClusterState state, final ActionListener<Response> listener) {
            persistentTaskClusterService.updatePersistentTaskStatus(request.taskId, request.status, new ActionListener<Empty>() {
                @Override
                public void onResponse(Empty empty) {
                    listener.onResponse(new Response(true));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }
}
