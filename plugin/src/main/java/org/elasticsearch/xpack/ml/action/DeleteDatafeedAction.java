/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksService;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.IOException;
import java.util.Objects;

public class DeleteDatafeedAction extends Action<DeleteDatafeedAction.Request, DeleteDatafeedAction.Response,
        DeleteDatafeedAction.RequestBuilder> {

    public static final DeleteDatafeedAction INSTANCE = new DeleteDatafeedAction();
    public static final String NAME = "cluster:admin/xpack/ml/datafeeds/delete";

    private DeleteDatafeedAction() {
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

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentFragment {

        public static final ParseField FORCE = new ParseField("force");

        private String datafeedId;
        private boolean force;

        public Request(String datafeedId) {
            this.datafeedId = ExceptionsHelper.requireNonNull(datafeedId, DatafeedConfig.ID.getPreferredName());
        }

        Request() {
        }

        public String getDatafeedId() {
            return datafeedId;
        }

        public boolean isForce() {
            return force;
        }

         public void setForce(boolean force) {
            this.force = force;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeedId = in.readString();
            if (in.getVersion().onOrAfter(Version.V_5_5_0)) {
                force = in.readBoolean();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(datafeedId);
            if (out.getVersion().onOrAfter(Version.V_5_5_0)) {
                out.writeBoolean(force);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(DatafeedConfig.ID.getPreferredName(), datafeedId);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request other = (Request) o;
            return Objects.equals(datafeedId, other.datafeedId) && Objects.equals(force, other.force);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeedId, force);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, DeleteDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        private Response() {
        }

        private Response(boolean acknowledged) {
            super(acknowledged);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private InternalClient client;
        private PersistentTasksService persistentTasksService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               InternalClient internalClient, PersistentTasksService persistentTasksService) {
            super(settings, DeleteDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.client = internalClient;
            this.persistentTasksService = persistentTasksService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            if (request.isForce()) {
                forceDeleteDatafeed(request, state, listener);
            } else {
                deleteDatafeedFromMetadata(request, listener);
            }
        }

        private void forceDeleteDatafeed(Request request, ClusterState state, ActionListener<Response> listener) {
            ActionListener<Boolean> finalListener = ActionListener.wrap(
                    response -> deleteDatafeedFromMetadata(request, listener),
                    listener::onFailure
            );

            ActionListener<IsolateDatafeedAction.Response> isolateDatafeedHandler = ActionListener.wrap(
                    response -> removeDatafeedTask(request, state, finalListener),
                    listener::onFailure
            );

            IsolateDatafeedAction.Request isolateDatafeedRequest = new IsolateDatafeedAction.Request(request.getDatafeedId());
            client.execute(IsolateDatafeedAction.INSTANCE, isolateDatafeedRequest, isolateDatafeedHandler);
        }

        private void removeDatafeedTask(Request request, ClusterState state, ActionListener<Boolean> listener) {
            PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            PersistentTasksCustomMetaData.PersistentTask<?> datafeedTask = MlMetadata.getDatafeedTask(request.getDatafeedId(), tasks);
            if (datafeedTask == null) {
                listener.onResponse(true);
            } else {
                persistentTasksService.cancelPersistentTask(datafeedTask.getId(),
                        new ActionListener<PersistentTasksCustomMetaData.PersistentTask<?>>() {
                            @Override
                            public void onResponse(PersistentTasksCustomMetaData.PersistentTask<?> persistentTask) {
                                listener.onResponse(Boolean.TRUE);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (e instanceof ResourceNotFoundException) {
                                    // the task has been removed in between
                                    listener.onResponse(true);
                                } else {
                                    listener.onFailure(e);
                                }
                            }
                        });
            }
        }

        private void deleteDatafeedFromMetadata(Request request, ActionListener<Response> listener) {
            clusterService.submitStateUpdateTask("delete-datafeed-" + request.getDatafeedId(),
                    new AckedClusterStateUpdateTask<Response>(request, listener) {

                        @Override
                        protected Response newResponse(boolean acknowledged) {
                            return new Response(acknowledged);
                        }

                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            MlMetadata currentMetadata = currentState.getMetaData().custom(MlMetadata.TYPE);
                            PersistentTasksCustomMetaData persistentTasks =
                                    currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                            MlMetadata newMetadata = new MlMetadata.Builder(currentMetadata)
                                    .removeDatafeed(request.getDatafeedId(), persistentTasks).build();
                            return ClusterState.builder(currentState).metaData(
                                    MetaData.builder(currentState.getMetaData()).putCustom(MlMetadata.TYPE, newMetadata).build())
                                    .build();
                        }
                    });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}
