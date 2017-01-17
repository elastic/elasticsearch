/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
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
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.JobStatus;
import org.elasticsearch.xpack.ml.job.metadata.Allocation;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.JobStatusObserver;

import java.io.IOException;
import java.util.Objects;

public class CloseJobAction extends Action<CloseJobAction.Request, CloseJobAction.Response, CloseJobAction.RequestBuilder> {

    public static final CloseJobAction INSTANCE = new CloseJobAction();
    public static final String NAME = "cluster:admin/ml/job/close";

    private CloseJobAction() {
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
        private TimeValue closeTimeout = TimeValue.timeValueMinutes(30);

        Request() {}

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public TimeValue getCloseTimeout() {
            return closeTimeout;
        }

        public void setCloseTimeout(TimeValue closeTimeout) {
            this.closeTimeout = closeTimeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            closeTimeout = new TimeValue(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            closeTimeout.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, closeTimeout);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || obj.getClass() != getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(jobId, other.jobId) &&
                    Objects.equals(closeTimeout, other.closeTimeout);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, CloseJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private boolean closed;

        Response() {
        }

        Response(boolean closed) {
            this.closed = closed;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            closed = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(closed);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("closed", closed);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return closed == response.closed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(closed);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final ClusterService clusterService;
        private final JobStatusObserver jobStatusObserver;
        private final TransportListTasksAction listTasksAction;
        private final TransportCancelTasksAction cancelTasksAction;

        @Inject
        public TransportAction(Settings settings, TransportService transportService,  ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, TransportCancelTasksAction cancelTasksAction,
                               TransportListTasksAction listTasksAction) {
            super(settings, CloseJobAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, Request::new);
            this.clusterService = clusterService;
            this.jobStatusObserver = new JobStatusObserver(threadPool, clusterService);
            this.cancelTasksAction = cancelTasksAction;
            this.listTasksAction = listTasksAction;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            MlMetadata mlMetadata = clusterService.state().metaData().custom(MlMetadata.TYPE);
            validate(request.jobId, mlMetadata);

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setActions(InternalOpenJobAction.NAME);
            listTasksRequest.setDetailed(true);
            listTasksAction.execute(listTasksRequest, ActionListener.wrap(listTasksResponse -> {
                String expectedJobDescription = "job-" + request.jobId;
                for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                    if (expectedJobDescription.equals(taskInfo.getDescription())) {
                        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                        cancelTasksRequest.setTaskId(taskInfo.getTaskId());
                        cancelTasksAction.execute(cancelTasksRequest, ActionListener.wrap(
                            cancelTasksResponse -> {
                                jobStatusObserver.waitForStatus(request.jobId, request.closeTimeout, JobStatus.CLOSED,
                                    e -> {
                                        if (e != null) {
                                            listener.onFailure(e);
                                        } else {
                                            listener.onResponse(new CloseJobAction.Response(true));
                                        }
                                    }
                                );
                            },
                            listener::onFailure)
                        );
                        return;
                    }
                }
                listener.onFailure(new ResourceNotFoundException("No job [" + request.jobId + "] running"));
            }, listener::onFailure));
        }

        static void validate(String jobId, MlMetadata mlMetadata) {
            Allocation allocation = mlMetadata.getAllocations().get(jobId);
            if (allocation == null) {
                throw ExceptionsHelper.missingJobException(jobId);
            }

            if (allocation.getStatus() != JobStatus.OPENED) {
                throw new ElasticsearchStatusException("job not opened, expected job status [{}], but got [{}]",
                        RestStatus.CONFLICT, JobStatus.OPENED, allocation.getStatus());
            }
        }
    }
}

