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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.persistent.PersistentTasksService;
import org.elasticsearch.xpack.persistent.PersistentTasksService.WaitForPersistentTaskStatusListener;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class CloseJobAction extends Action<CloseJobAction.Request, CloseJobAction.Response, CloseJobAction.RequestBuilder> {

    public static final CloseJobAction INSTANCE = new CloseJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/close";

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

    public static class Request extends TransportJobTaskAction.JobTaskRequest<Request> implements ToXContent {

        public static final ParseField TIMEOUT = new ParseField("timeout");
        public static final ParseField FORCE = new ParseField("force");
        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString(Request::setJobId, Job.ID);
            PARSER.declareString((request, val) ->
                    request.setCloseTimeout(TimeValue.parseTimeValue(val, TIMEOUT.getPreferredName())), TIMEOUT);
            PARSER.declareBoolean(Request::setForce, FORCE);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private TimeValue timeout = TimeValue.timeValueMinutes(20);
        private boolean force = false;

        Request() {}

        public Request(String jobId) {
            super(jobId);
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public TimeValue getCloseTimeout() {
            return timeout;
        }

        public void setCloseTimeout(TimeValue timeout) {
            this.timeout = timeout;
        }

        public boolean isForce() {
            return force;
        }

        public void setForce(boolean force) {
            this.force = force;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            timeout = new TimeValue(in);
            force = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            timeout.writeTo(out);
            out.writeBoolean(force);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(TIMEOUT.getPreferredName(), timeout.getStringRep());
            builder.field(FORCE.getPreferredName(), force);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, timeout, force);
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
                    Objects.equals(timeout, other.timeout) &&
                    Objects.equals(force, other.force);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, CloseJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private boolean closed;

        Response() {
        }

        Response(StreamInput in) throws IOException {
            readFrom(in);
        }

        Response(boolean closed) {
            super(null, null);
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

    public static class TransportAction extends TransportJobTaskAction<OpenJobAction.JobTask, Request, Response> {

        private final InternalClient client;
        private final ClusterService clusterService;
        private final Auditor auditor;
        private final PersistentTasksService persistentTasksService;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, AutodetectProcessManager manager, InternalClient client,
                               Auditor auditor, PersistentTasksService persistentTasksService) {
            super(settings, CloseJobAction.NAME, threadPool, clusterService, transportService, actionFilters,
                    indexNameExpressionResolver, Request::new, Response::new, ThreadPool.Names.MANAGEMENT, manager);
            this.client = client;
            this.clusterService = clusterService;
            this.auditor = auditor;
            this.persistentTasksService = persistentTasksService;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            ClusterState currentState = clusterService.state();
            if (request.isForce()) {
                PersistentTasksCustomMetaData tasks = currentState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
                String jobId = request.getJobId();
                PersistentTask<?> jobTask = MlMetadata.getJobTask(jobId, tasks);
                if (jobTask == null) {
                    throw new ElasticsearchStatusException("cannot force close job, because job [" + jobId + "] is not open",
                            RestStatus.CONFLICT);
                }
                forceCloseJob(jobTask.getId(), jobId, listener);
            } else {
                PersistentTask<?> jobTask = validateAndReturnJobTask(request.getJobId(), currentState);
                normalCloseJob(task, jobTask.getId(), request, listener);
            }
        }

        @Override
        protected void innerTaskOperation(Request request, OpenJobAction.JobTask task, ActionListener<Response> listener,
                                          ClusterState state) {
            task.closeJob("close job (api)");
            listener.onResponse(new Response(true));
        }

        @Override
        protected Response readTaskResponse(StreamInput in) throws IOException {
            return new Response(in);
        }

        private void forceCloseJob(long persistentTaskId, String jobId, ActionListener<Response> listener) {
            auditor.info(jobId, Messages.JOB_AUDIT_FORCE_CLOSING);
            persistentTasksService.cancelPersistentTask(persistentTaskId, new ActionListener<PersistentTask<?>>() {
                @Override
                public void onResponse(PersistentTask<?> task) {
                    listener.onResponse(new Response(true));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private void normalCloseJob(Task task, long persistentTaskId, Request request, ActionListener<Response> listener) {
            auditor.info(request.getJobId(), Messages.JOB_AUDIT_CLOSING);
            ActionListener<Response> finalListener =
                    ActionListener.wrap(r -> waitForJobClosed(persistentTaskId, request, r, listener), listener::onFailure);
            super.doExecute(task, request, finalListener);
        }

        // Wait for job to be marked as closed in cluster state, which means the job persistent task has been removed
        // This api returns when job has been closed, but that doesn't mean the persistent task has been removed from cluster state,
        // so wait for that to happen here.
        void waitForJobClosed(long persistentTaskId, Request request, Response response, ActionListener<Response> listener) {
            persistentTasksService.waitForPersistentTaskStatus(persistentTaskId, Objects::isNull, request.timeout,
                    new WaitForPersistentTaskStatusListener<OpenJobAction.Request>() {
                @Override
                public void onResponse(PersistentTask<OpenJobAction.Request> task) {
                    logger.debug("finalizing job [{}]", request.getJobId());
                    FinalizeJobExecutionAction.Request finalizeRequest =
                            new FinalizeJobExecutionAction.Request(request.getJobId());
                    client.execute(FinalizeJobExecutionAction.INSTANCE, finalizeRequest,
                            ActionListener.wrap(r-> listener.onResponse(response), listener::onFailure));
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

    }

    static PersistentTask<?> validateAndReturnJobTask(String jobId, ClusterState state) {
        MlMetadata mlMetadata = state.metaData().custom(MlMetadata.TYPE);
        Job job = mlMetadata.getJobs().get(jobId);
        if (job == null) {
            throw new ResourceNotFoundException("cannot close job, because job [" + jobId + "] does not exist");
        }

        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTask<?> jobTask = MlMetadata.getJobTask(jobId, tasks);
        if (jobTask == null) {
            throw new ElasticsearchStatusException("cannot close job, because job [" + jobId + "] is not open", RestStatus.CONFLICT);
        }

        Optional<DatafeedConfig> datafeed = mlMetadata.getDatafeedByJobId(jobId);
        if (datafeed.isPresent()) {
            DatafeedState datafeedState = MlMetadata.getDatafeedState(datafeed.get().getId(), tasks);
            if (datafeedState != DatafeedState.STOPPED) {
                throw new ElasticsearchStatusException("cannot close job [{}], datafeed hasn't been stopped",
                RestStatus.CONFLICT, jobId);
            }
        }
        return jobTask;
    }
}

