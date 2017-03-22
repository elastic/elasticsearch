/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.persistent;

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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * This action can be used to start a persistent task previously created using {@link CreatePersistentTaskAction}
 */
public class StartPersistentTaskAction extends Action<StartPersistentTaskAction.Request,
        StartPersistentTaskAction.Response,
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
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends MasterNodeRequest<Request> {

        private long taskId;

        public Request() {

        }

        public Request(long taskId) {
            this.taskId = taskId;
        }

        public void setTaskId(long taskId) {
            this.taskId = taskId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            taskId = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(taskId);
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
            return taskId == request.taskId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId);
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

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<StartPersistentTaskAction.Request,
            StartPersistentTaskAction.Response, StartPersistentTaskAction.RequestBuilder> {

        protected RequestBuilder(ElasticsearchClient client, StartPersistentTaskAction action) {
            super(client, action, new Request());
        }

        public final RequestBuilder setTaskId(long taskId) {
            request.setTaskId(taskId);
            return this;
        }

    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final PersistentTasksClusterService persistentTasksClusterService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               PersistentTasksClusterService persistentTasksClusterService,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, StartPersistentTaskAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.persistentTasksClusterService = persistentTasksClusterService;
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
            persistentTasksClusterService.startPersistentTask(request.taskId, new ActionListener<Empty>() {
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


