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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;

import java.io.IOException;
import java.util.Objects;

public class PutSchedulerAction extends Action<PutSchedulerAction.Request, PutSchedulerAction.Response, PutSchedulerAction.RequestBuilder> {

    public static final PutSchedulerAction INSTANCE = new PutSchedulerAction();
    public static final String NAME = "cluster:admin/ml/scheduler/put";

    private PutSchedulerAction() {
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

        public static Request parseRequest(String schedulerId, XContentParser parser) {
            SchedulerConfig.Builder scheduler = SchedulerConfig.PARSER.apply(parser, null);
            scheduler.setId(schedulerId);
            return new Request(scheduler.build());
        }

        private SchedulerConfig scheduler;

        public Request(SchedulerConfig scheduler) {
            this.scheduler = scheduler;
        }

        Request() {
        }

        public SchedulerConfig getScheduler() {
            return scheduler;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            scheduler = new SchedulerConfig(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            scheduler.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            scheduler.toXContent(builder, params);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(scheduler, request.scheduler);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scheduler);
        }
    }

    public static class RequestBuilder extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, PutSchedulerAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {

        private SchedulerConfig scheduler;

        public Response(boolean acked, SchedulerConfig scheduler) {
            super(acked);
            this.scheduler = scheduler;
        }

        Response() {
        }

        public SchedulerConfig getResponse() {
            return scheduler;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            readAcknowledged(in);
            scheduler = new SchedulerConfig(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            writeAcknowledged(out);
            scheduler.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            scheduler.doXContentBody(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return Objects.equals(scheduler, response.scheduler);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scheduler);
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                               ThreadPool threadPool, ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, PutSchedulerAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
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
            clusterService.submitStateUpdateTask("put-scheduler-" + request.getScheduler().getId(),
                    new AckedClusterStateUpdateTask<Response>(request, listener) {

                        @Override
                        protected Response newResponse(boolean acknowledged) {
                            if (acknowledged) {
                                logger.info("Created scheduler [{}]", request.getScheduler().getId());
                            }
                            return new Response(acknowledged, request.getScheduler());
                        }

                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            return putScheduler(request, currentState);
                        }
                    });
        }

        private ClusterState putScheduler(Request request, ClusterState clusterState) {
            MlMetadata currentMetadata = clusterState.getMetaData().custom(MlMetadata.TYPE);
            MlMetadata newMetadata = new MlMetadata.Builder(currentMetadata)
                    .putScheduler(request.getScheduler()).build();
            return ClusterState.builder(clusterState).metaData(
                    MetaData.builder(clusterState.getMetaData()).putCustom(MlMetadata.TYPE, newMetadata).build())
                    .build();
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }
}
