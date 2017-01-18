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
import org.elasticsearch.tasks.LoggingTaskListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.ml.scheduler.ScheduledJobRunner;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.ml.scheduler.SchedulerStatus;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.SchedulerStatusObserver;

import java.io.IOException;
import java.util.Objects;

public class StartSchedulerAction
        extends Action<StartSchedulerAction.Request, StartSchedulerAction.Response, StartSchedulerAction.RequestBuilder> {

    public static final ParseField START_TIME = new ParseField("start");
    public static final ParseField END_TIME = new ParseField("end");
    public static final ParseField START_TIMEOUT = new ParseField("start_timeout");

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
            PARSER.declareString((request, val) -> request.setStartTimeout(TimeValue.parseTimeValue(val,
                    START_TIME.getPreferredName())), START_TIMEOUT);
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
        private TimeValue startTimeout = TimeValue.timeValueSeconds(30);

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

        public TimeValue getStartTimeout() {
            return startTimeout;
        }

        public void setStartTimeout(TimeValue startTimeout) {
            this.startTimeout = startTimeout;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            schedulerId = in.readString();
            startTime = in.readVLong();
            endTime = in.readOptionalLong();
            startTimeout = new TimeValue(in.readVLong());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(schedulerId);
            out.writeVLong(startTime);
            out.writeOptionalLong(endTime);
            out.writeVLong(startTimeout.millis());
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

    public static class Response extends ActionResponse implements ToXContentObject {

        private boolean started;

        Response(boolean started) {
            this.started = started;
        }

        Response() {
        }

        public boolean isStarted() {
            return started;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            started = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(started);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("started", started);
            builder.endObject();
            return builder;
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final ClusterService clusterService;
        private final SchedulerStatusObserver schedulerStatusObserver;
        private final InternalStartSchedulerAction.TransportAction transportAction;

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               ClusterService clusterService, InternalStartSchedulerAction.TransportAction transportAction) {
            super(settings, StartSchedulerAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                    Request::new);
            this.clusterService = clusterService;
            this.schedulerStatusObserver = new SchedulerStatusObserver(threadPool, clusterService);
            this.transportAction = transportAction;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            // This validation happens also in ScheduledJobRunner, the reason we do it here too is that if it fails there
            // we are unable to provide the user immediate feedback. We would create the task and the validation would fail
            // in the background, whereas now the validation failure is part of the response being returned.
            MlMetadata mlMetadata = clusterService.state().metaData().custom(MlMetadata.TYPE);
            ScheduledJobRunner.validate(request.schedulerId, mlMetadata);

            InternalStartSchedulerAction.Request internalRequest =
                    new InternalStartSchedulerAction.Request(request.schedulerId, request.startTime);
            internalRequest.setEndTime(request.endTime);
            transportAction.execute(internalRequest, LoggingTaskListener.instance());
            schedulerStatusObserver.waitForStatus(request.schedulerId, request.startTimeout, SchedulerStatus.STARTED, e -> {
                if (e != null) {
                    listener.onFailure(e);
                } else {
                    listener.onResponse(new Response(true));
                }

            });
        }
    }
}
