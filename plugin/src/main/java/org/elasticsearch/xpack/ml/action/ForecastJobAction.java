/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xpack.ml.action.ForecastJobAction.Request.END_TIME;

public class ForecastJobAction extends Action<ForecastJobAction.Request, ForecastJobAction.Response, ForecastJobAction.RequestBuilder> {

    public static final ForecastJobAction INSTANCE = new ForecastJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/forecast";

    private ForecastJobAction() {
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

    public static class Request extends TransportJobTaskAction.JobTaskRequest<Request> implements ToXContentObject {

        public static final ParseField END_TIME = new ParseField("end");
        public static final ParseField DURATION = new ParseField("duration");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareString(Request::setEndTime, END_TIME);
            PARSER.declareString(Request::setDuration, DURATION);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private String endTime;
        private TimeValue duration;

        Request() {
        }

        public Request(String jobId) {
            super(jobId);
        }

        public String getEndTime() {
            return endTime;
        }

        public void setEndTime(String endTime) {
            this.endTime = endTime;
        }

        public TimeValue getDuration() {
            return duration;
        }

        public void setDuration(String duration) {
            this.duration = TimeValue.parseTimeValue(duration, DURATION.getPreferredName());
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.endTime = in.readOptionalString();
            this.duration = in.readOptionalWriteable(TimeValue::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(endTime);
            out.writeOptionalWriteable(duration);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, endTime, duration);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(jobId, other.jobId) && Objects.equals(endTime, other.endTime) && Objects.equals(duration, other.duration);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            if (endTime != null) {
                builder.field(END_TIME.getPreferredName(), endTime);
            }
            if (duration != null) {
                builder.field(DURATION.getPreferredName(), duration.getStringRep());
            }
            builder.endObject();
            return builder;
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, ForecastJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private boolean acknowledged;
        private long id;

        Response() {
            super(null, null);
        }

        Response(boolean acknowledged, long id) {
            super(null, null);
            this.acknowledged = acknowledged;
            this.id = id;
        }

        public boolean isacknowledged() {
            return acknowledged;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            acknowledged = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(acknowledged);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("acknowledged", acknowledged);
            builder.field("id", id);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Response response = (Response) o;
            return acknowledged == response.acknowledged;
        }

        @Override
        public int hashCode() {
            return Objects.hash(acknowledged);
        }
    }

    public static class TransportAction extends TransportJobTaskAction<Request, Response> {

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, ClusterService clusterService,
                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                AutodetectProcessManager processManager) {
            super(settings, ForecastJobAction.NAME, threadPool, clusterService, transportService, actionFilters,
                    indexNameExpressionResolver, ForecastJobAction.Request::new, ForecastJobAction.Response::new, ThreadPool.Names.SAME,
                    processManager);
            // ThreadPool.Names.SAME, because operations is executed by
            // autodetect worker thread
        }

        @Override
        protected ForecastJobAction.Response readTaskResponse(StreamInput in) throws IOException {
            Response response = new Response();
            response.readFrom(in);
            return response;
        }

        @Override
        protected void taskOperation(Request request, OpenJobAction.JobTask task, ActionListener<Response> listener) {
            ForecastParams.Builder paramsBuilder = ForecastParams.builder();
            if (request.getEndTime() != null) {
                paramsBuilder.endTime(request.getEndTime(), END_TIME);
            }
            if (request.getDuration() != null) {
                paramsBuilder.duration(request.getDuration());
            }

            ForecastParams params = paramsBuilder.build();
            processManager.forecastJob(task, params, e -> {
                if (e == null) {
                    listener.onResponse(new Response(true, params.getId()));
                } else {
                    listener.onFailure(e);
                }
            });
        }
    }
}

