/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobStatus;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.JobStatusObserver;

import java.io.IOException;
import java.util.Objects;

public class OpenJobAction extends Action<OpenJobAction.Request, OpenJobAction.Response, OpenJobAction.RequestBuilder> {

    public static final OpenJobAction INSTANCE = new OpenJobAction();
    public static final String NAME = "cluster:admin/ml/job/open";

    private OpenJobAction() {
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

    public static class Request extends ActionRequest {

        private String jobId;
        private boolean ignoreDowntime;
        private TimeValue openTimeout = TimeValue.timeValueSeconds(20);

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

       Request() {}

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public boolean isIgnoreDowntime() {
            return ignoreDowntime;
        }

        public void setIgnoreDowntime(boolean ignoreDowntime) {
            this.ignoreDowntime = ignoreDowntime;
        }

        public TimeValue getOpenTimeout() {
            return openTimeout;
        }

        public void setOpenTimeout(TimeValue openTimeout) {
            this.openTimeout = openTimeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            ignoreDowntime = in.readBoolean();
            openTimeout = TimeValue.timeValueMillis(in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeBoolean(ignoreDowntime);
            out.writeVLong(openTimeout.millis());
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, ignoreDowntime, openTimeout);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            OpenJobAction.Request other = (OpenJobAction.Request) obj;
            return Objects.equals(jobId, other.jobId) &&
                    Objects.equals(ignoreDowntime, other.ignoreDowntime) &&
                    Objects.equals(openTimeout, other.openTimeout);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, OpenJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private boolean opened;

        Response() {}

        Response(boolean opened) {
            this.opened = opened;
        }

        public boolean isOpened() {
            return opened;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            opened = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(opened);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("opened", opened);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return opened == response.opened;
        }

        @Override
        public int hashCode() {
            return Objects.hash(opened);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final JobStatusObserver observer;
        private final ClusterService clusterService;
        private final InternalOpenJobAction.TransportAction internalOpenJobAction;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, InternalOpenJobAction.TransportAction internalOpenJobAction) {
            super(settings, OpenJobAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.clusterService = clusterService;
            this.observer = new JobStatusObserver(threadPool, clusterService);
            this.internalOpenJobAction = internalOpenJobAction;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            // This validation happens also in InternalOpenJobAction, the reason we do it here too is that if it fails there
            // we are unable to provide the user immediate feedback. We would create the task and the validation would fail
            // in the background, whereas now the validation failure is part of the response being returned.
            MlMetadata mlMetadata = clusterService.state().metaData().custom(MlMetadata.TYPE);
            validate(mlMetadata, request.getJobId());

            InternalOpenJobAction.Request internalRequest = new InternalOpenJobAction.Request(request.jobId);
            internalRequest.setIgnoreDowntime(internalRequest.isIgnoreDowntime());
            internalOpenJobAction.execute(internalRequest, LoggingTaskListener.instance());
            observer.waitForStatus(request.getJobId(), request.openTimeout, JobStatus.OPENED, e -> {
                if (e != null) {
                    listener.onFailure(e);
                } else {
                    listener.onResponse(new Response(true));
                }
            });
        }

        /**
         * Fail fast before trying to update the job status on master node if the job doesn't exist or its status
         * is not what it should be.
         */
        public static void validate(MlMetadata mlMetadata, String jobId) {
            MlMetadata.Builder builder = new MlMetadata.Builder(mlMetadata);
            builder.updateStatus(jobId, JobStatus.OPENING, null);
        }
    }
}
