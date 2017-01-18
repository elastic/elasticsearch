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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.scheduler.Scheduler;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.ml.scheduler.SchedulerStatus;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.SchedulerStatusObserver;

import java.io.IOException;
import java.util.Objects;

public class StopSchedulerAction
        extends Action<StopSchedulerAction.Request, StopSchedulerAction.Response, StopSchedulerAction.RequestBuilder> {

    public static final StopSchedulerAction INSTANCE = new StopSchedulerAction();
    public static final String NAME = "cluster:admin/ml/scheduler/stop";

    private StopSchedulerAction() {
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

        private String schedulerId;
        private TimeValue stopTimeout = TimeValue.timeValueSeconds(30);

        public Request(String jobId) {
            this.schedulerId = ExceptionsHelper.requireNonNull(jobId, SchedulerConfig.ID.getPreferredName());
        }

        Request() {
        }

        public String getSchedulerId() {
            return schedulerId;
        }

        public void setStopTimeout(TimeValue stopTimeout) {
            this.stopTimeout = stopTimeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            schedulerId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(schedulerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(schedulerId);
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
            return Objects.equals(schedulerId, other.schedulerId);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, StopSchedulerAction action) {
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

        private final ClusterService clusterService;
        private final TransportListTasksAction listTasksAction;
        private final TransportCancelTasksAction cancelTasksAction;
        private final SchedulerStatusObserver schedulerStatusObserver;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, TransportCancelTasksAction cancelTasksAction,
                               TransportListTasksAction listTasksAction) {
            super(settings, StopSchedulerAction.NAME, threadPool, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.clusterService = clusterService;
            this.listTasksAction = listTasksAction;
            this.cancelTasksAction = cancelTasksAction;
            this.schedulerStatusObserver = new SchedulerStatusObserver(threadPool, clusterService);
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            String schedulerId = request.getSchedulerId();
            MlMetadata mlMetadata = clusterService.state().metaData().custom(MlMetadata.TYPE);
            validate(schedulerId, mlMetadata);

            ListTasksRequest listTasksRequest = new ListTasksRequest();
            listTasksRequest.setActions(InternalStartSchedulerAction.NAME);
            listTasksRequest.setDetailed(true);
            listTasksAction.execute(listTasksRequest, new ActionListener<ListTasksResponse>() {
                @Override
                public void onResponse(ListTasksResponse listTasksResponse) {
                    String expectedJobDescription = "scheduler-" + schedulerId;
                    for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                        if (expectedJobDescription.equals(taskInfo.getDescription())) {
                            CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                            cancelTasksRequest.setTaskId(taskInfo.getTaskId());
                            cancelTasksAction.execute(cancelTasksRequest, new ActionListener<CancelTasksResponse>() {
                                @Override
                                public void onResponse(CancelTasksResponse cancelTasksResponse) {
                                    schedulerStatusObserver.waitForStatus(schedulerId, request.stopTimeout, SchedulerStatus.STOPPED, e -> {
                                        if (e != null) {
                                            listener.onFailure(e);
                                        } else {
                                            listener.onResponse(new Response());
                                        }
                                    });
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    listener.onFailure(e);
                                }
                            });
                            return;
                        }
                    }
                    listener.onFailure(new ResourceNotFoundException("No scheduler [" + schedulerId + "] running"));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }


    }

    static void validate(String schedulerId, MlMetadata mlMetadata) {
        Scheduler scheduler = mlMetadata.getScheduler(schedulerId);
        if (scheduler == null) {
            throw new ResourceNotFoundException(Messages.getMessage(Messages.SCHEDULER_NOT_FOUND, schedulerId));
        }

        if (scheduler.getStatus() == SchedulerStatus.STOPPED) {
            throw new ElasticsearchStatusException("scheduler already stopped, expected scheduler status [{}], but got [{}]",
                    RestStatus.CONFLICT, SchedulerStatus.STARTED, scheduler.getStatus());
        }
    }
}
