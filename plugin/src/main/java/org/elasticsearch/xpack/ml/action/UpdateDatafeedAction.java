/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;

import java.io.IOException;
import java.util.Objects;

public class UpdateDatafeedAction extends Action<UpdateDatafeedAction.Request, PutDatafeedAction.Response,
        UpdateDatafeedAction.RequestBuilder> {

    public static final UpdateDatafeedAction INSTANCE = new UpdateDatafeedAction();
    public static final String NAME = "cluster:admin/ml/datafeeds/update";

    private UpdateDatafeedAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, this);
    }

    @Override
    public PutDatafeedAction.Response newResponse() {
        return new PutDatafeedAction.Response();
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContent {

        public static Request parseRequest(String datafeedId, XContentParser parser) {
            DatafeedUpdate.Builder update = DatafeedUpdate.PARSER.apply(parser, null);
            update.setId(datafeedId);
            return new Request(update.build());
        }

        private DatafeedUpdate update;

        public Request(DatafeedUpdate update) {
            this.update = update;
        }

        Request() {
        }

        public DatafeedUpdate getUpdate() {
            return update;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            update = new DatafeedUpdate(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            update.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            update.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(update, request.update);
        }

        @Override
        public int hashCode() {
            return Objects.hash(update);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, PutDatafeedAction.Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, UpdateDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, PutDatafeedAction.Response> {

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, UpdateDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected PutDatafeedAction.Response newResponse() {
            return new PutDatafeedAction.Response();
        }

        @Override
        protected void masterOperation(Request request, ClusterState state, ActionListener<PutDatafeedAction.Response> listener)
                throws Exception {
            clusterService.submitStateUpdateTask("update-datafeed-" + request.getUpdate().getId(),
                    new AckedClusterStateUpdateTask<PutDatafeedAction.Response>(request, listener) {
                        private DatafeedConfig updatedDatafeed;

                        @Override
                        protected PutDatafeedAction.Response newResponse(boolean acknowledged) {
                            if (acknowledged) {
                                logger.info("Updated datafeed [{}]", request.getUpdate().getId());
                            }
                            return new PutDatafeedAction.Response(acknowledged, updatedDatafeed);
                        }

                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            DatafeedUpdate update = request.getUpdate();
                            MlMetadata currentMetadata = state.getMetaData().custom(MlMetadata.TYPE);
                            PersistentTasksInProgress persistentTasksInProgress =
                                    state.getMetaData().custom(PersistentTasksInProgress.TYPE);
                            MlMetadata newMetadata = new MlMetadata.Builder(currentMetadata)
                                    .updateDatafeed(update, persistentTasksInProgress).build();
                            updatedDatafeed = newMetadata.getDatafeed(update.getId());
                            return ClusterState.builder(currentState).metaData(
                                    MetaData.builder(currentState.getMetaData()).putCustom(MlMetadata.TYPE, newMetadata).build()).build();
                        }
                    });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}
