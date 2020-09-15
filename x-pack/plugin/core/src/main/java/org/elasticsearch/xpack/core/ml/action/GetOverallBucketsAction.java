/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.xpack.core.action.AbstractGetResourcesResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.results.OverallBucket;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * <p>
 * This action returns summarized bucket results over multiple jobs.
 * Overall buckets have the span of the largest job's bucket_span.
 * Their score is calculated by finding the max anomaly score per job
 * and then averaging the top N.
 * </p>
 * <p>
 * Overall buckets can be optionally aggregated into larger intervals
 * by setting the bucket_span parameter. When that is the case, the
 * overall_score is the max of the overall buckets that are within
 * the interval.
 * </p>
 */
public class GetOverallBucketsAction extends ActionType<GetOverallBucketsAction.Response> {

    public static final GetOverallBucketsAction INSTANCE = new GetOverallBucketsAction();
    public static final String NAME = "cluster:monitor/xpack/ml/job/results/overall_buckets/get";

    private GetOverallBucketsAction() {
        super(NAME, Response::new);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        public static final ParseField TOP_N = new ParseField("top_n");
        public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
        public static final ParseField OVERALL_SCORE = new ParseField("overall_score");
        public static final ParseField EXCLUDE_INTERIM = new ParseField("exclude_interim");
        public static final ParseField START = new ParseField("start");
        public static final ParseField END = new ParseField("end");
        @Deprecated
        public static final String ALLOW_NO_JOBS = "allow_no_jobs";
        public static final ParseField ALLOW_NO_MATCH = new ParseField("allow_no_match", ALLOW_NO_JOBS);

        private static final ObjectParser<Request, Void> PARSER = new ObjectParser<>(NAME, Request::new);

        static {
            PARSER.declareString((request, jobId) -> request.jobId = jobId, Job.ID);
            PARSER.declareInt(Request::setTopN, TOP_N);
            PARSER.declareString(Request::setBucketSpan, BUCKET_SPAN);
            PARSER.declareDouble(Request::setOverallScore, OVERALL_SCORE);
            PARSER.declareBoolean(Request::setExcludeInterim, EXCLUDE_INTERIM);
            PARSER.declareString((request, startTime) -> request.setStart(parseDateOrThrow(
                    startTime, START, System::currentTimeMillis)), START);
            PARSER.declareString((request, endTime) -> request.setEnd(parseDateOrThrow(
                    endTime, END, System::currentTimeMillis)), END);
            PARSER.declareBoolean(Request::setAllowNoMatch, ALLOW_NO_MATCH);
        }

        static long parseDateOrThrow(String date, ParseField paramName, LongSupplier now) {
            DateMathParser dateMathParser = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.toDateMathParser();

            try {
                return dateMathParser.parse(date, now).toEpochMilli();
            } catch (Exception e) {
                String msg = Messages.getMessage(Messages.REST_INVALID_DATETIME_PARAMS, paramName.getPreferredName(), date);
                throw new ElasticsearchParseException(msg, e);
            }
        }

        public static Request parseRequest(String jobId, XContentParser parser) {
            Request request = PARSER.apply(parser, null);
            if (jobId != null) {
                request.jobId = jobId;
            }
            return request;
        }

        private String jobId;
        private int topN = 1;
        private TimeValue bucketSpan;
        private double overallScore = 0.0;
        private boolean excludeInterim = false;
        private Long start;
        private Long end;
        private boolean allowNoMatch = true;

        public Request() {
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            jobId = in.readString();
            topN = in.readVInt();
            bucketSpan = in.readOptionalTimeValue();
            overallScore = in.readDouble();
            excludeInterim = in.readBoolean();
            start = in.readOptionalLong();
            end = in.readOptionalLong();
            allowNoMatch = in.readBoolean();
        }

        public Request(String jobId) {
            this.jobId = ExceptionsHelper.requireNonNull(jobId, Job.ID.getPreferredName());
        }

        public String getJobId() {
            return jobId;
        }

        public int getTopN() {
            return topN;
        }

        public void setTopN(int topN) {
            if (topN <= 0) {
                throw new IllegalArgumentException("[topN] parameter must be positive, found [" + topN + "]");
            }
            this.topN = topN;
        }

        public TimeValue getBucketSpan() {
            return bucketSpan;
        }

        public void setBucketSpan(TimeValue bucketSpan) {
            this.bucketSpan = bucketSpan;
        }

        public void setBucketSpan(String bucketSpan) {
            this.bucketSpan = TimeValue.parseTimeValue(bucketSpan, BUCKET_SPAN.getPreferredName());
        }

        public double getOverallScore() {
            return overallScore;
        }

        public void setOverallScore(double overallScore) {
            this.overallScore = overallScore;
        }

        public boolean isExcludeInterim() {
            return excludeInterim;
        }

        public void setExcludeInterim(boolean excludeInterim) {
            this.excludeInterim = excludeInterim;
        }

        public Long getStart() {
            return start;
        }

        public void setStart(Long start) {
            this.start = start;
        }

        public void setStart(String start) {
            setStart(parseDateOrThrow(start, START, System::currentTimeMillis));
        }

        public Long getEnd() {
            return end;
        }

        public void setEnd(Long end) {
            this.end = end;
        }

        public void setEnd(String end) {
            setEnd(parseDateOrThrow(end, END, System::currentTimeMillis));
        }

        public boolean allowNoMatch() {
            return allowNoMatch;
        }

        public void setAllowNoMatch(boolean allowNoMatch) {
            this.allowNoMatch = allowNoMatch;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(jobId);
            out.writeVInt(topN);
            out.writeOptionalTimeValue(bucketSpan);
            out.writeDouble(overallScore);
            out.writeBoolean(excludeInterim);
            out.writeOptionalLong(start);
            out.writeOptionalLong(end);
            out.writeBoolean(allowNoMatch);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(TOP_N.getPreferredName(), topN);
            if (bucketSpan != null) {
                builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan.getStringRep());
            }
            builder.field(OVERALL_SCORE.getPreferredName(), overallScore);
            builder.field(EXCLUDE_INTERIM.getPreferredName(), excludeInterim);
            if (start != null) {
                builder.field(START.getPreferredName(), String.valueOf(start));
            }
            if (end != null) {
                builder.field(END.getPreferredName(), String.valueOf(end));
            }
            builder.field(ALLOW_NO_MATCH.getPreferredName(), allowNoMatch);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, topN, bucketSpan, overallScore, excludeInterim, start, end, allowNoMatch);
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (getClass() != other.getClass()) {
                return false;
            }
            Request that = (Request) other;
            return Objects.equals(jobId, that.jobId) &&
                    this.topN == that.topN &&
                    Objects.equals(bucketSpan, that.bucketSpan) &&
                    this.excludeInterim == that.excludeInterim &&
                    this.overallScore == that.overallScore &&
                    Objects.equals(start, that.start) &&
                    Objects.equals(end, that.end) &&
                    this.allowNoMatch == that.allowNoMatch;
        }
    }

    public static class Response extends AbstractGetResourcesResponse<OverallBucket> implements ToXContentObject {

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(QueryPage<OverallBucket> overallBuckets) {
            super(overallBuckets);
        }

        public QueryPage<OverallBucket> getOverallBuckets() {
            return getResources();
        }

        @Override
        protected Reader<OverallBucket> getReader() {
            return OverallBucket::new;
        }
    }

}
