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
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.InterimResultsParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.TimeRange;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;

public class FlushJobAction extends Action<FlushJobAction.Request, FlushJobAction.Response, FlushJobAction.RequestBuilder> {

    public static final FlushJobAction INSTANCE = new FlushJobAction();
    public static final String NAME = "cluster:admin/ml/anomaly_detectors/flush";

    private FlushJobAction() {
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

        public static final ParseField CALC_INTERIM = new ParseField("calc_interim");
        public static final ParseField START = new ParseField("start");
        public static final ParseField END = new ParseField("end");
        public static final ParseField ADVANCE_TIME = new ParseField("advance_time");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareBoolean(Request::setCalcInterim, CALC_INTERIM);
            PARSER.declareString(Request::setStart, START);
            PARSER.declareString(Request::setEnd, END);
            PARSER.declareString(Request::setAdvanceTime, ADVANCE_TIME);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private boolean calcInterim = false;
        private String start;
        private String end;
        private String advanceTime;

        Request() {
        }

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public boolean getCalcInterim() {
            return calcInterim;
        }

        public void setCalcInterim(boolean calcInterim) {
            this.calcInterim = calcInterim;
        }

        public String getStart() {
            return start;
        }

        public void setStart(String start) {
            this.start = start;
        }

        public String getEnd() {
            return end;
        }

        public void setEnd(String end) {
            this.end = end;
        }

        public String getAdvanceTime() { return advanceTime; }

        public void setAdvanceTime(String advanceTime) {
            this.advanceTime = advanceTime;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            calcInterim = in.readBoolean();
            start = in.readOptionalString();
            end = in.readOptionalString();
            advanceTime = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(calcInterim);
            out.writeOptionalString(start);
            out.writeOptionalString(end);
            out.writeOptionalString(advanceTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, calcInterim, start, end, advanceTime);
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
            return Objects.equals(jobId, other.jobId) &&
                    calcInterim == other.calcInterim &&
                    Objects.equals(start, other.start) &&
                    Objects.equals(end, other.end) &&
                    Objects.equals(advanceTime, other.advanceTime);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(CALC_INTERIM.getPreferredName(), calcInterim);
            if (start != null) {
                builder.field(START.getPreferredName(), start);
            }
            if (end != null) {
                builder.field(END.getPreferredName(), end);
            }
            if (advanceTime != null) {
                builder.field(ADVANCE_TIME.getPreferredName(), advanceTime);
            }
            builder.endObject();
            return builder;
        }
    }

    static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        RequestBuilder(ElasticsearchClient client, FlushJobAction action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private boolean flushed;

        Response() {
        }

        Response(boolean flushed) {
            super(null, null);
            this.flushed = flushed;
        }

        public boolean isFlushed() {
            return flushed;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            flushed = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(flushed);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("flushed", flushed);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return flushed == response.flushed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(flushed);
        }
    }

    public static class TransportAction extends TransportJobTaskAction<OpenJobAction.JobTask, Request, Response> {

        @Inject
        public TransportAction(Settings settings, TransportService transportService, ThreadPool threadPool, ClusterService clusterService,
                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                AutodetectProcessManager processManager) {
            super(settings, FlushJobAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                    FlushJobAction.Request::new, FlushJobAction.Response::new, MachineLearning.THREAD_POOL_NAME, processManager);
        }

        @Override
        protected FlushJobAction.Response readTaskResponse(StreamInput in) throws IOException {
            Response response = new Response();
            response.readFrom(in);
            return response;
        }

        @Override
        protected void innerTaskOperation(Request request, OpenJobAction.JobTask task,
                                     ActionListener<FlushJobAction.Response> listener) {
            InterimResultsParams.Builder paramsBuilder = InterimResultsParams.builder();
            paramsBuilder.calcInterim(request.getCalcInterim());
            if (request.getAdvanceTime() != null) {
                paramsBuilder.advanceTime(request.getAdvanceTime());
            }
            TimeRange.Builder timeRangeBuilder = TimeRange.builder();
            if (request.getStart() != null) {
                timeRangeBuilder.startTime(request.getStart());
            }
            if (request.getEnd() != null) {
                timeRangeBuilder.endTime(request.getEnd());
            }
            paramsBuilder.forTimeRange(timeRangeBuilder.build());
            processManager.flushJob(request.getJobId(), paramsBuilder.build());
            listener.onResponse(new Response(true));
        }
    }
}


