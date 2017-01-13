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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.scheduler.ScheduledJobRunner;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class StartSchedulerAction
        extends Action<StartSchedulerAction.Request, StartSchedulerAction.Response, StartSchedulerAction.RequestBuilder> {

    public static final ParseField START_TIME = new ParseField("start");
    public static final ParseField END_TIME = new ParseField("end");

    public static final StartSchedulerAction INSTANCE = new StartSchedulerAction();
    public static final String NAME = "cluster:admin/ml/scheduler/start";

    private StartSchedulerAction() {
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

    public static class Request extends ActionRequest implements ToXContent {

        public static ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, schedulerId) -> request.schedulerId = schedulerId, SchedulerConfig.ID);
            PARSER.declareLong((request, startTime) -> request.startTime = startTime, START_TIME);
            PARSER.declareLong(Request::setEndTime, END_TIME);
        }

        public static Request parseRequest(String schedulerId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (schedulerId != null) {
                request.schedulerId = schedulerId;
            }
            return request;
        }

        private String schedulerId;
        private long startTime;
        private Long endTime;

        public Request(String schedulerId, long startTime) {
            this.schedulerId = ExceptionsHelper.requireNonNull(schedulerId, SchedulerConfig.ID.getPreferredName());
            this.startTime = startTime;
        }

        Request() {
        }

        public String getSchedulerId() {
            return schedulerId;
        }

        public long getStartTime() {
            return startTime;
        }

        public Long getEndTime() {
            return endTime;
        }

        public void setEndTime(Long endTime) {
            this.endTime = endTime;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId) {
            return new SchedulerTask(id, type, action, parentTaskId, schedulerId);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            schedulerId = in.readString();
            startTime = in.readVLong();
            endTime = in.readOptionalLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(schedulerId);
            out.writeVLong(startTime);
            out.writeOptionalLong(endTime);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SchedulerConfig.ID.getPreferredName(), schedulerId);
            builder.field(START_TIME.getPreferredName(), startTime);
            if (endTime != null) {
                builder.field(END_TIME.getPreferredName(), endTime);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(schedulerId, startTime, endTime);
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
            return Objects.equals(schedulerId, other.schedulerId) &&
                    Objects.equals(startTime, other.startTime) &&
                    Objects.equals(endTime, other.endTime);
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, StartSchedulerAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends ActionResponse {

        Response() {
        }

    }

    public static class SchedulerTask extends CancellableTask {

        private volatile ScheduledJobRunner.Holder holder;

        public SchedulerTask(long id, String type, String action, TaskId parentTaskId, String schedulerId) {
            super(id, type, action, "scheduler-" + schedulerId, parentTaskId);
        }

        public void setHolder(ScheduledJobRunner.Holder holder) {
            this.holder = holder;
        }

        @Override
        protected void onCancelled() {
            stop();
        }

        /* public for testing */
        public void stop() {
            if (holder != null) {
                holder.stop(null);
            }
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final ScheduledJobRunner scheduledJobRunner;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ScheduledJobRunner scheduledJobRunner) {
            super(settings, StartSchedulerAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                    Request::new);
            this.scheduledJobRunner = scheduledJobRunner;
        }

        @Override
        protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
            SchedulerTask schedulerTask = (SchedulerTask) task;
            scheduledJobRunner.run(request.getSchedulerId(), request.getStartTime(), request.getEndTime(), schedulerTask, (error) -> {
                if (error != null) {
                    listener.onFailure(error);
                } else {
                    listener.onResponse(new Response());
                }
            });
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            throw new UnsupportedOperationException("the task parameter is required");
        }
    }
}
