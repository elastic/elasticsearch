/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class FlushJobAction extends ActionType<FlushJobAction.Response> {

    public static final FlushJobAction INSTANCE = new FlushJobAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/flush";

    private FlushJobAction() {
        super(NAME, FlushJobAction.Response::new);
    }

    public static class Request extends JobTaskRequest<Request> implements ToXContentObject {

        public static final ParseField CALC_INTERIM = new ParseField("calc_interim");
        public static final ParseField START = new ParseField("start");
        public static final ParseField END = new ParseField("end");
        public static final ParseField ADVANCE_TIME = new ParseField("advance_time");
        public static final ParseField SKIP_TIME = new ParseField("skip_time");

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareBoolean(Request::setCalcInterim, CALC_INTERIM);
            PARSER.declareString(Request::setStart, START);
            PARSER.declareString(Request::setEnd, END);
            PARSER.declareString(Request::setAdvanceTime, ADVANCE_TIME);
            PARSER.declareString(Request::setSkipTime, SKIP_TIME);
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private boolean calcInterim = false;
        private boolean waitForNormalization = true;
        private boolean refreshRequired = true;
        private String start;
        private String end;
        private String advanceTime;
        private String skipTime;

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            calcInterim = in.readBoolean();
            start = in.readOptionalString();
            end = in.readOptionalString();
            advanceTime = in.readOptionalString();
            skipTime = in.readOptionalString();
            waitForNormalization = in.readBoolean();
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_012)) {
                refreshRequired = in.readBoolean();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(calcInterim);
            out.writeOptionalString(start);
            out.writeOptionalString(end);
            out.writeOptionalString(advanceTime);
            out.writeOptionalString(skipTime);
            out.writeBoolean(waitForNormalization);
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_012)) {
                out.writeBoolean(refreshRequired);
            }
        }

        public Request(String jobId) {
            super(jobId);
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

        public String getAdvanceTime() {
            return advanceTime;
        }

        public void setAdvanceTime(String advanceTime) {
            this.advanceTime = advanceTime;
        }

        public String getSkipTime() {
            return skipTime;
        }

        public void setSkipTime(String skipTime) {
            this.skipTime = skipTime;
        }

        public boolean isWaitForNormalization() {
            return waitForNormalization;
        }

        public boolean isRefreshRequired() {
            return refreshRequired;
        }

        /**
         * Used internally. Datafeeds do not need to wait for renormalization to complete before continuing.
         *
         * For large jobs, renormalization can take minutes, causing datafeeds to needlessly pause execution.
         */
        public void setWaitForNormalization(boolean waitForNormalization) {
            this.waitForNormalization = waitForNormalization;
        }

        /**
         * Used internally. For datafeeds, there is no need for the results to be searchable after the flush,
         * as the datafeed itself does not search them immediately.
         *
         * Particularly for short bucket spans these refreshes could be a significant cost.
         **/
        public void setRefreshRequired(boolean refreshRequired) {
            this.refreshRequired = refreshRequired;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, calcInterim, start, end, advanceTime, skipTime, waitForNormalization, refreshRequired);
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
                && calcInterim == other.calcInterim
                && waitForNormalization == other.waitForNormalization
                && refreshRequired == other.refreshRequired
                && Objects.equals(start, other.start)
                && Objects.equals(end, other.end)
                && Objects.equals(advanceTime, other.advanceTime)
                && Objects.equals(skipTime, other.skipTime);
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
            if (skipTime != null) {
                builder.field(SKIP_TIME.getPreferredName(), skipTime);
            }
            builder.endObject();
            return builder;
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final boolean flushed;
        private final Instant lastFinalizedBucketEnd;

        public Response(boolean flushed, @Nullable Instant lastFinalizedBucketEnd) {
            super(null, null);
            this.flushed = flushed;
            // Round to millisecond accuracy to ensure round-tripping via XContent results in an equal object
            this.lastFinalizedBucketEnd = (lastFinalizedBucketEnd != null)
                ? Instant.ofEpochMilli(lastFinalizedBucketEnd.toEpochMilli())
                : null;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            flushed = in.readBoolean();
            lastFinalizedBucketEnd = in.readOptionalInstant();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(flushed);
            out.writeOptionalInstant(lastFinalizedBucketEnd);
        }

        public boolean isFlushed() {
            return flushed;
        }

        public Instant getLastFinalizedBucketEnd() {
            return lastFinalizedBucketEnd;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("flushed", flushed);
            if (lastFinalizedBucketEnd != null) {
                builder.timeField(
                    FlushAcknowledgement.LAST_FINALIZED_BUCKET_END.getPreferredName(),
                    FlushAcknowledgement.LAST_FINALIZED_BUCKET_END.getPreferredName() + "_string",
                    lastFinalizedBucketEnd.toEpochMilli()
                );
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return flushed == response.flushed && Objects.equals(lastFinalizedBucketEnd, response.lastFinalizedBucketEnd);
        }

        @Override
        public int hashCode() {
            return Objects.hash(flushed, lastFinalizedBucketEnd);
        }
    }
}
