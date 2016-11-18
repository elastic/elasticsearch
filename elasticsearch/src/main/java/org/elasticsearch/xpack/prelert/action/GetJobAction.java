/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.manager.AutodetectProcessManager;
import org.elasticsearch.xpack.prelert.job.manager.JobManager;
import org.elasticsearch.xpack.prelert.job.persistence.ElasticsearchJobProvider;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;
import org.elasticsearch.xpack.prelert.utils.SingleDocument;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public class GetJobAction extends Action<GetJobAction.Request, GetJobAction.Response, GetJobAction.RequestBuilder> {

    public static final GetJobAction INSTANCE = new GetJobAction();
    public static final String NAME = "cluster:admin/prelert/job/get";

    private GetJobAction() {
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

    public static class Request extends MasterNodeReadRequest<Request> {

        private String jobId;
        private boolean config;
        private boolean dataCounts;
        private boolean modelSizeStats;

        Request() {
        }

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public Request all() {
            this.config = true;
            this.dataCounts = true;
            this.modelSizeStats = true;
            return this;
        }

        public boolean config() {
            return config;
        }

        public Request config(boolean config) {
            this.config = config;
            return this;
        }

        public boolean dataCounts() {
            return dataCounts;
        }

        public Request dataCounts(boolean dataCounts) {
            this.dataCounts = dataCounts;
            return this;
        }

        public boolean modelSizeStats() {
            return modelSizeStats;
        }

        public Request modelSizeStats(boolean modelSizeStats) {
            this.modelSizeStats = modelSizeStats;
            return this;
        }


        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            config = in.readBoolean();
            dataCounts = in.readBoolean();
            modelSizeStats = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeBoolean(config);
            out.writeBoolean(dataCounts);
            out.writeBoolean(modelSizeStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, config, dataCounts, modelSizeStats);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(jobId, other.jobId) && this.config == other.config
                    && this.dataCounts == other.dataCounts && this.modelSizeStats == other.modelSizeStats;
        }
    }

    public static class RequestBuilder extends MasterNodeReadOperationRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, GetJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements StatusToXContent {

        static class JobInfo implements ToXContent, Writeable {
            @Nullable
            private Job jobConfig;
            @Nullable
            private DataCounts dataCounts;
            @Nullable
            private ModelSizeStats modelSizeStats;

            JobInfo(@Nullable Job job, @Nullable DataCounts dataCounts, @Nullable ModelSizeStats modelSizeStats) {
                this.jobConfig = job;
                this.dataCounts = dataCounts;
                this.modelSizeStats = modelSizeStats;
            }

            JobInfo(StreamInput in) throws IOException {
                jobConfig = in.readOptionalWriteable(Job::new);
                dataCounts = in.readOptionalWriteable(DataCounts::new);
                modelSizeStats = in.readOptionalWriteable(ModelSizeStats::new);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                if (jobConfig != null) {
                    builder.field("config", jobConfig);
                }
                if (dataCounts != null) {
                    builder.field("data_counts", dataCounts);
                }
                if (modelSizeStats != null) {
                    builder.field("model_size_stats", modelSizeStats);
                }
                builder.endObject();

                return builder;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeOptionalWriteable(jobConfig);
                out.writeOptionalWriteable(dataCounts);
                out.writeOptionalWriteable(modelSizeStats);
            }

            @Override
            public int hashCode() {
                return Objects.hash(jobConfig, dataCounts, modelSizeStats);
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (getClass() != obj.getClass()) {
                    return false;
                }
                JobInfo other = (JobInfo) obj;
                return Objects.equals(jobConfig, other.jobConfig)
                        && Objects.equals(this.dataCounts, other.dataCounts)
                        && Objects.equals(this.modelSizeStats, other.modelSizeStats);
            }
        }

        private SingleDocument<JobInfo> jobResponse;

        public Response() {
            jobResponse = SingleDocument.empty(Job.TYPE);
        }

        public Response(JobInfo jobResponse) {
            this.jobResponse = new SingleDocument<>(Job.TYPE, jobResponse);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobResponse = new SingleDocument<>(in, JobInfo::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            jobResponse.writeTo(out);
        }

        @Override
        public RestStatus status() {
            return jobResponse.status();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return jobResponse.toXContent(builder, params);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobResponse);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(jobResponse, other.jobResponse);
        }

        @SuppressWarnings("deprecation")
        @Override
        public final String toString() {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.prettyPrint();
                builder.startObject();
                toXContent(builder, EMPTY_PARAMS);
                builder.endObject();
                return builder.string();
            } catch (Exception e) {
                // So we have a stack trace logged somewhere
                return "{ \"error\" : \"" + org.elasticsearch.ExceptionsHelper.detailedMessage(e) + "\"}";
            }
        }
    }


    public static class TransportAction extends TransportMasterNodeReadAction<Request, Response> {

        private final JobManager jobManager;
        private final AutodetectProcessManager processManager;
        private final ElasticsearchJobProvider jobProvider;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ClusterService clusterService,
                ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                JobManager jobManager, AutodetectProcessManager processManager, ElasticsearchJobProvider jobProvider) {
            super(settings, GetJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.jobManager = jobManager;
            this.processManager = processManager;
            this.jobProvider = jobProvider;
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
            logger.debug("Get job '{}', config={}, data_counts={}, model_size_stats={}",
                    request.getJobId(), request.config(), request.dataCounts(), request.modelSizeStats());

            // always get the job regardless of the request.config param because if the job
            // can't be found a different response is returned.
            Optional<Job> optionalJob = jobManager.getJob(request.getJobId(), state);
            if (optionalJob.isPresent() == false) {
                logger.debug(String.format(Locale.ROOT, "Cannot find job '%s'", request.getJobId()));
                listener.onResponse(new Response());
                return;
            }

            logger.debug("Returning job '" + optionalJob.get().getJobId() + "'");

            Job job = request.config() && optionalJob.isPresent() ? optionalJob.get() : null;
            DataCounts dataCounts = readDataCounts(request);
            ModelSizeStats modelSizeStats = readModelSizeStats(request);
            listener.onResponse(new Response(new Response.JobInfo(job, dataCounts, modelSizeStats)));
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        }

        private DataCounts readDataCounts(Request request) {
            if (request.dataCounts()) {
                Optional<DataCounts> counts = processManager.getDataCounts(request.getJobId());
                return counts.orElseGet(() -> jobProvider.dataCounts(request.getJobId()));
            }

            return null;
        }

        private ModelSizeStats readModelSizeStats(Request request) {
            if (request.modelSizeStats()) {
                Optional<ModelSizeStats> sizeStats = processManager.getModelSizeStats(request.getJobId());
                return sizeStats.orElseGet(() -> jobProvider.modelSizeStats(request.getJobId()).orElse(null));
            }

            return null;
        }
    }

}
