/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.ForecastParams;
import org.elasticsearch.xpack.ml.job.results.Forecast;
import org.elasticsearch.xpack.ml.job.results.ForecastRequestStats; 
import org.elasticsearch.xpack.ml.job.results.ForecastRequestStats.ForecastRequestStatus;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.ml.action.ForecastJobAction.Request.DURATION;

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

        public static final ParseField DURATION = new ParseField("duration");
        public static final ParseField EXPIRES_IN = new ParseField("expires_in");

        // Max allowed duration: 8 weeks
        private static final TimeValue MAX_DURATION = TimeValue.parseTimeValue("56d", "");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareString(Request::setDuration, DURATION);
            PARSER.declareString(Request::setExpiresIn, EXPIRES_IN);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private TimeValue duration;
        private TimeValue expiresIn;

        Request() {
        }

        public Request(String jobId) {
            super(jobId);
        }

        public TimeValue getDuration() {
            return duration;
        }

        public void setDuration(String duration) {
            setDuration(TimeValue.parseTimeValue(duration, DURATION.getPreferredName()));
        }

        public void setDuration(TimeValue duration) {
            this.duration = duration;
            if (this.duration.compareTo(TimeValue.ZERO) <= 0) {
                throw new IllegalArgumentException("[" + DURATION.getPreferredName() + "] must be positive: ["
                        + duration.getStringRep() + "]");
            }
            if (this.duration.compareTo(MAX_DURATION) > 0) {
                throw new IllegalArgumentException("[" + DURATION.getPreferredName() + "] must be " + MAX_DURATION.getStringRep()
                        + " or less: [" + duration.getStringRep() + "]");
            }
        }

        public TimeValue getExpiresIn() {
            return expiresIn;
        }

        public void setExpiresIn(String expiration) {
            setExpiresIn(TimeValue.parseTimeValue(expiration, EXPIRES_IN.getPreferredName()));
        }

        public void setExpiresIn(TimeValue expiresIn) {
            this.expiresIn = expiresIn;
            if (this.expiresIn.compareTo(TimeValue.ZERO) < 0) {
                throw new IllegalArgumentException("[" + EXPIRES_IN.getPreferredName() + "] must be non-negative: ["
                        + expiresIn.getStringRep() + "]");
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.duration = in.readOptionalWriteable(TimeValue::new);
            this.expiresIn = in.readOptionalWriteable(TimeValue::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalWriteable(duration);
            out.writeOptionalWriteable(expiresIn);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, duration, expiresIn);
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
            return Objects.equals(jobId, other.jobId)
                    && Objects.equals(duration, other.duration)
                    && Objects.equals(expiresIn, other.expiresIn);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            if (duration != null) {
                builder.field(DURATION.getPreferredName(), duration.getStringRep());
            }
            if (expiresIn != null) {
                builder.field(EXPIRES_IN.getPreferredName(), expiresIn.getStringRep());
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
        private String forecastId;

        Response() {
            super(null, null);
        }

        Response(boolean acknowledged, String forecastId) {
            super(null, null);
            this.acknowledged = acknowledged;
            this.forecastId = forecastId;
        }

        public boolean isAcknowledged() {
            return acknowledged;
        }

        public String getForecastId() {
            return forecastId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            acknowledged = in.readBoolean();
            forecastId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(acknowledged);
            out.writeString(forecastId);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("acknowledged", acknowledged);
            builder.field(Forecast.FORECAST_ID.getPreferredName(), forecastId);
            builder.endObject();
            return builder;
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
            return this.acknowledged == other.acknowledged && Objects.equals(this.forecastId, other.forecastId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(acknowledged, forecastId);
        }
    }

    public static class TransportAction extends TransportJobTaskAction<Request, Response> {

        private final JobProvider jobProvider;
        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, ClusterService clusterService,
                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                JobProvider jobProvider, AutodetectProcessManager processManager) {
            super(settings, ForecastJobAction.NAME, threadPool, clusterService, transportService, actionFilters,
                    indexNameExpressionResolver, ForecastJobAction.Request::new, ForecastJobAction.Response::new, ThreadPool.Names.SAME,
                    processManager);
            this.jobProvider = jobProvider;
            // ThreadPool.Names.SAME, because operations is executed by autodetect worker thread
        }

        @Override
        protected ForecastJobAction.Response readTaskResponse(StreamInput in) throws IOException {
            Response response = new Response();
            response.readFrom(in);
            return response;
        }

        @Override
        protected void taskOperation(Request request, OpenJobAction.JobTask task, ActionListener<Response> listener) {
            ClusterState state = clusterService.state();
            Job job = JobManager.getJobOrThrowIfUnknown(task.getJobId(), state);
            validate(job, request);

            ForecastParams.Builder paramsBuilder = ForecastParams.builder();

            if (request.getDuration() != null) {
                paramsBuilder.duration(request.getDuration());
            }

            if (request.getExpiresIn() != null) {
                paramsBuilder.expiresIn(request.getExpiresIn());
            }

            ForecastParams params = paramsBuilder.build();
            processManager.forecastJob(task, params, e -> {
                if (e == null) {
                    Consumer<ForecastRequestStats> forecastRequestStatsHandler = forecastRequestStats -> {
                        if (forecastRequestStats == null) {
                            // paranoia case, it should not happen that we do not retrieve a result
                            listener.onFailure(new ElasticsearchException("Cannot run forecast: internal error, please check the logs"));
                        } else if (forecastRequestStats.getStatus() == ForecastRequestStatus.FAILED) {
                            List<String> messages = forecastRequestStats.getMessages();
                            if (messages.size() > 0) {
                                listener.onFailure(ExceptionsHelper.badRequestException("Cannot run forecast: " + messages.get(0)));
                            } else {
                                // paranoia case, it should not be possible to have an empty message list
                                listener.onFailure(
                                        new ElasticsearchException("Cannot run forecast: internal error, please check the logs"));
                            }
                        } else {
                            listener.onResponse(new Response(true, params.getForecastId()));
                        }
                    };

                    jobProvider.getForecastRequestStats(request.getJobId(), params.getForecastId(), forecastRequestStatsHandler,
                            listener::onFailure);
                } else {
                    listener.onFailure(e);
                }
            });
        }

        static void validate(Job job, Request request) {
            if (job.getJobVersion() == null || job.getJobVersion().before(Version.V_6_1_0)) {
                throw ExceptionsHelper.badRequestException(
                        "Cannot run forecast because jobs created prior to version 6.1 are not supported");
            }

            if (request.getDuration() != null) {
                TimeValue duration = request.getDuration();
                TimeValue bucketSpan = job.getAnalysisConfig().getBucketSpan();

                if (duration.compareTo(bucketSpan) < 0) {
                    throw ExceptionsHelper.badRequestException(
                            "[" + DURATION.getPreferredName() + "] must be greater or equal to the bucket span: [" + duration.getStringRep()
                                    + "/" + bucketSpan.getStringRep() + "]");
                }
            }
        }
    }
}

