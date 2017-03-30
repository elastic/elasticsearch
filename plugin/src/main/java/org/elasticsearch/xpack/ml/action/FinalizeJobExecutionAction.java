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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.job.config.Job;

import java.io.IOException;
import java.util.Date;

public class FinalizeJobExecutionAction extends Action<FinalizeJobExecutionAction.Request,
        FinalizeJobExecutionAction.Response,FinalizeJobExecutionAction.RequestBuilder> {

    public static final FinalizeJobExecutionAction INSTANCE = new FinalizeJobExecutionAction();
    public static final String NAME = "cluster:internal/ml/job/finalize_job_execution";

    private FinalizeJobExecutionAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client, INSTANCE);
    }

    @Override
    public Response newResponse() {
        return new Response();
    }

    public static class Request extends MasterNodeRequest<Request> {

        private String jobId;

        public Request(String jobId) {
            this.jobId = jobId;
        }

        Request() {
        }

        public String getJobId() {
            return jobId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class RequestBuilder
            extends MasterNodeOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, FinalizeJobExecutionAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        Response(boolean acknowledged) {
            super(acknowledged);
        }

        Response() {
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

        @Inject
        public TransportAction(Settings settings, TransportService transportService,
                               ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters,
                               IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, NAME, transportService, clusterService, threadPool, actionFilters,
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
        protected void masterOperation(Request request, ClusterState state,
                                       ActionListener<Response> listener) throws Exception {
            String jobId = request.getJobId();
            String source = "finalize_job_execution [" + jobId + "]";
            logger.debug("finalizing job [{}]", request.getJobId());
            clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MlMetadata mlMetadata = currentState.metaData().custom(MlMetadata.TYPE);
                    Job.Builder jobBuilder = new Job.Builder(mlMetadata.getJobs().get(jobId));
                    jobBuilder.setFinishedTime(new Date());
                    MlMetadata.Builder mlMetadataBuilder = new MlMetadata.Builder(mlMetadata);
                    mlMetadataBuilder.putJob(jobBuilder.build(), true);

                    ClusterState.Builder builder = ClusterState.builder(currentState);
                    return builder.metaData(new MetaData.Builder(currentState.metaData())
                            .putCustom(MlMetadata.TYPE, mlMetadataBuilder.build()))
                            .build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState,
                                                  ClusterState newState) {
                    logger.debug("finalized job [{}]", request.getJobId());
                    listener.onResponse(new Response(true));
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

}
