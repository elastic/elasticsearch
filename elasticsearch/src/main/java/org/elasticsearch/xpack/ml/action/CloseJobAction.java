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
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.ActionFilters;
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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.persistent.PersistentTaskClusterService;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

public class CloseJobAction extends Action<CloseJobAction.Request, CloseJobAction.Response, CloseJobAction.RequestBuilder> {

    public static final CloseJobAction INSTANCE = new CloseJobAction();
    public static final String NAME = "cluster:admin/ml/anomaly_detectors/close";

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

    public static class Request extends MasterNodeRequest<Request> implements ToXContent {

        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setJobId, Job.ID);
            PARSER.declareString((request, val) ->
                    request.setTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private String jobId;
        private TimeValue timeout = TimeValue.timeValueMinutes(20);

        Request() {}

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public void setTimeout(TimeValue timeout) {
            this.timeout = timeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            jobId = in.readString();
            timeout = new TimeValue(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            timeout.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, timeout);
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
                    Objects.equals(timeout, other.timeout);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, CloseJobAction action) {
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

    public static class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final ClusterService clusterService;
        private final TransportListTasksAction listTasksAction;
        private final TransportCancelTasksAction cancelTasksAction;
        private final PersistentTaskClusterService persistentTaskClusterService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, TransportListTasksAction listTasksAction,
                               TransportCancelTasksAction cancelTasksAction, PersistentTaskClusterService persistentTaskClusterService) {
            super(settings, CloseJobAction.NAME, transportService, clusterService, threadPool, actionFilters,
                    indexNameExpressionResolver, Request::new);
            this.clusterService = clusterService;
            this.listTasksAction = listTasksAction;
            this.cancelTasksAction = cancelTasksAction;
            this.persistentTaskClusterService = persistentTaskClusterService;
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
            PersistentTaskInProgress<?> task = validateAndFindTask(request.getJobId(), state);
            clusterService.submitStateUpdateTask("closing job [" + request.getJobId() + "]", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    return moveJobToClosingState(request.getJobId(), currentState);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    ListTasksRequest listTasksRequest = new ListTasksRequest();
                    listTasksRequest.setDetailed(true);
                    listTasksRequest.setActions(OpenJobAction.NAME + "[c]");
                    listTasksAction.execute(listTasksRequest, ActionListener.wrap(listTasksResponse -> {
                        String expectedDescription = "job-" + request.getJobId();
                        for (TaskInfo taskInfo : listTasksResponse.getTasks()) {
                            if (expectedDescription.equals(taskInfo.getDescription())) {
                                CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
                                cancelTasksRequest.setTaskId(taskInfo.getTaskId());
                                cancelTasksAction.execute(cancelTasksRequest, ActionListener.wrap(cancelTaskResponse -> {
                                    persistentTaskClusterService.completeOrRestartPersistentTask(task.getId(), null,
                                            ActionListener.wrap(
                                                    empty -> listener.onResponse(new CloseJobAction.Response(true)),
                                                    listener::onFailure
                                            )
                                    );
                                }, listener::onFailure));
                                return;
                            }
                        }
                        listener.onFailure(new ResourceNotFoundException("task not found for job [" + request.getJobId() + "]"));
                    }, listener::onFailure));
                }
            });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

    static PersistentTaskInProgress<?> validateAndFindTask(String jobId, ClusterState state) {
        MlMetadata mlMetadata = state.metaData().custom(MlMetadata.TYPE);
        if (mlMetadata.getJobs().containsKey(jobId) == false) {
            throw ExceptionsHelper.missingJobException(jobId);
        }

        PersistentTasksInProgress tasks = state.custom(PersistentTasksInProgress.TYPE);
        if (tasks != null) {
            Predicate<PersistentTaskInProgress<?>> p = t -> {
                OpenJobAction.Request storedRequest = (OpenJobAction.Request) t.getRequest();
                return storedRequest.getJobId().equals(jobId);
            };
            for (PersistentTaskInProgress<?> task : tasks.findTasks(OpenJobAction.NAME, p)) {
                OpenJobAction.Request storedRequest = (OpenJobAction.Request) task.getRequest();
                if (storedRequest.getJobId().equals(jobId)) {
                    JobState jobState = (JobState) task.getStatus();
                    if (jobState != JobState.OPENED) {
                        throw new ElasticsearchStatusException("cannot close job, expected job state [{}], but got [{}]",
                                RestStatus.CONFLICT, JobState.OPENED, jobState);
                    }
                    return task;
                }
            }
        }
        throw new ElasticsearchStatusException("cannot close job, expected job state [{}], but got [{}]",
                RestStatus.CONFLICT, JobState.OPENED, JobState.CLOSED);
    }

    static ClusterState moveJobToClosingState(String jobId, ClusterState currentState) {
        PersistentTaskInProgress<?> task = validateAndFindTask(jobId, currentState);
        PersistentTasksInProgress currentTasks = currentState.custom(PersistentTasksInProgress.TYPE);
        Map<Long, PersistentTaskInProgress<?>> updatedTasks = new HashMap<>(currentTasks.taskMap());
        for (PersistentTaskInProgress<?> taskInProgress : currentTasks.tasks()) {
            if (taskInProgress.getId() == task.getId()) {
                updatedTasks.put(taskInProgress.getId(), new PersistentTaskInProgress<>(taskInProgress, JobState.CLOSING));
            }
        }
        PersistentTasksInProgress newTasks = new PersistentTasksInProgress(currentTasks.getCurrentId(), updatedTasks);

        MlMetadata mlMetadata = currentState.metaData().custom(MlMetadata.TYPE);
        Job.Builder jobBuilder = new Job.Builder(mlMetadata.getJobs().get(jobId));
        jobBuilder.setFinishedTime(new Date());
        MlMetadata.Builder mlMetadataBuilder = new MlMetadata.Builder(mlMetadata);
        mlMetadataBuilder.putJob(jobBuilder.build(), true);

        ClusterState.Builder builder = ClusterState.builder(currentState);
        return builder
                .putCustom(PersistentTasksInProgress.TYPE, newTasks)
                .metaData(new MetaData.Builder(currentState.metaData())
                        .putCustom(MlMetadata.TYPE, mlMetadataBuilder.build()))
                .build();
    }
}

