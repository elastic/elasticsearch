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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;

import java.io.IOException;
import java.util.Objects;

public class PutDatafeedAction extends Action<PutDatafeedAction.Request, PutDatafeedAction.Response, PutDatafeedAction.RequestBuilder> {

    public static final PutDatafeedAction INSTANCE = new PutDatafeedAction();
    public static final String NAME = "cluster:admin/ml/datafeeds/put";

    private PutDatafeedAction() {
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

    public static class Request extends AcknowledgedRequest<Request> implements ToXContent {

        public static Request parseRequest(String datafeedId, XContentParser parser) {
            DatafeedConfig.Builder datafeed = DatafeedConfig.PARSER.apply(parser, null);
            datafeed.setId(datafeedId);
            return new Request(datafeed.build());
        }

        private DatafeedConfig datafeed;

        public Request(DatafeedConfig datafeed) {
            this.datafeed = datafeed;
        }

        Request() {
        }

        public DatafeedConfig getDatafeed() {
            return datafeed;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            datafeed = new DatafeedConfig(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            datafeed.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            datafeed.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(datafeed, request.datafeed);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeed);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, PutDatafeedAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        private DatafeedConfig datafeed;

        public Response(boolean acked, DatafeedConfig datafeed) {
            super(acked);
            this.datafeed = datafeed;
        }

        Response() {
        }

        public DatafeedConfig getResponse() {
            return datafeed;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
            datafeed = new DatafeedConfig(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
            datafeed.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            datafeed.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(datafeed, response.datafeed);
        }

        @Override
        public int hashCode() {
            return Objects.hash(datafeed);
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private XPackLicenseState licenseState;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, XPackLicenseState licenseState, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, PutDatafeedAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.licenseState = licenseState;
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
            clusterService.submitStateUpdateTask("put-datafeed-" + request.getDatafeed().getId(),
                    new AckedClusterStateUpdateTask<Response>(request, listener) {

                        @Override
                        protected Response newResponse(boolean acknowledged) {
                            if (acknowledged) {
                                logger.info("Created datafeed [{}]", request.getDatafeed().getId());
                            }
                            return new Response(acknowledged, request.getDatafeed());
                        }

                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            return putDatafeed(request, currentState);
                        }
                    });
        }

        private ClusterState putDatafeed(Request request, ClusterState clusterState) {
            MlMetadata currentMetadata = clusterState.getMetaData().custom(MlMetadata.TYPE);
            MlMetadata newMetadata = new MlMetadata.Builder(currentMetadata)
                    .putDatafeed(request.getDatafeed()).build();
            return ClusterState.builder(clusterState).metaData(
                    MetaData.builder(clusterState.getMetaData()).putCustom(MlMetadata.TYPE, newMetadata).build())
                    .build();
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            if (licenseState.isMachineLearningAllowed()) {
                super.doExecute(task, request, listener);
            } else {
                listener.onFailure(LicenseUtils.newComplianceException(XPackPlugin.MACHINE_LEARNING));
            }
        }
    }
}
