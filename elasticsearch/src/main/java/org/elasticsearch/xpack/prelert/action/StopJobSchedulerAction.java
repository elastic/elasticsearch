/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.manager.JobManager;
import org.elasticsearch.xpack.prelert.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class StopJobSchedulerAction
extends Action<StopJobSchedulerAction.Request, StopJobSchedulerAction.Response, StopJobSchedulerAction.RequestBuilder> {

    public static final StopJobSchedulerAction INSTANCE = new StopJobSchedulerAction();
    public static final String NAME = "cluster:admin/prelert/job/scheduler/stop";

    private StopJobSchedulerAction() {
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

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        Request() {
        }

        public String getJobId() {
            return jobId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
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
        public int hashCode() {
            return Objects.hash(jobId);
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
            return Objects.equals(jobId, other.jobId);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, StopJobSchedulerAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse {

        private Response() {
            super(true);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final TransportCancelTasksAction cancelTasksAction;
        private final TransportListTasksAction listTasksAction;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               TransportCancelTasksAction cancelTasksAction, TransportListTasksAction listTasksAction) {
            super(settings, StopJobSchedulerAction.NAME, threadPool, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.cancelTasksAction = cancelTasksAction;
            this.listTasksAction = listTasksAction;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            String jobId = request.getJobId();
            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setActions(StartJobSchedulerAction.NAME);
            listTasksRequest.setDetailed(true);
            listTasksAction.execute(listTasksRequest, new ActionListener<ListTasksResponse>() {
                @Override
                public void onResponse(ListTasksResponse listTasksResponse) {
                    String expectedJobDescription = "job-scheduler-" + jobId;
                    for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                        if (expectedJobDescription.equals(taskInfo.getDescription())) {
                            CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                            cancelTasksRequest.setTaskId(taskInfo.getTaskId());
                            cancelTasksAction.execute(cancelTasksRequest, new ActionListener<CancelTasksResponse>() {
                                @Override
                                public void onResponse(CancelTasksResponse cancelTasksResponse) {
                                    listener.onResponse(new Response());
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    listener.onFailure(e);
                                }
                            });
                            return;
                        }
                    }
                    listener.onFailure(new ResourceNotFoundException("No scheduler running for job [" + jobId + "]"));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }
}
