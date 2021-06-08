/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Overall Bucket Result POJO
 */
public class OverallBucket implements ToXContentObject {

    public static final ParseField OVERALL_SCORE = new ParseField("overall_score");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField JOBS = new ParseField("jobs");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("overall_buckets");

    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "overall_bucket";

    public static final ConstructingObjectParser<OverallBucket, Void> PARSER =
        new ConstructingObjectParser<>(RESULT_TYPE_VALUE, true,
            a -> new OverallBucket((Date) a[0], (long) a[1], (double) a[2], (boolean) a[3]));

    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p) -> TimeUtil.parseTimeField(p, Result.TIMESTAMP.getPreferredName()),
                Result.TIMESTAMP, ObjectParser.ValueType.VALUE);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), BUCKET_SPAN);
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), OVERALL_SCORE);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), Result.IS_INTERIM);
        PARSER.declareObjectArray(OverallBucket::setJobs, JobInfo.PARSER, JOBS);
    }

    private final Date timestamp;
    private final long bucketSpan;
    private final double overallScore;
    private final boolean isInterim;
    private List<JobInfo> jobs = Collections.emptyList();

    OverallBucket(Date timestamp, long bucketSpan, double overallScore, boolean isInterim) {
        this.timestamp = Objects.requireNonNull(timestamp);
        this.bucketSpan = bucketSpan;
        this.overallScore = overallScore;
        this.isInterim = isInterim;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.timeField(Result.TIMESTAMP.getPreferredName(), Result.TIMESTAMP.getPreferredName() + "_string", timestamp.getTime());
        builder.field(BUCKET_SPAN.getPreferredName(), bucketSpan);
        builder.field(OVERALL_SCORE.getPreferredName(), overallScore);
        builder.field(JOBS.getPreferredName(), jobs);
        builder.field(Result.IS_INTERIM.getPreferredName(), isInterim);
        builder.field(Result.RESULT_TYPE.getPreferredName(), RESULT_TYPE_VALUE);
        builder.endObject();
        return builder;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * Bucketspan expressed in seconds
     */
    public long getBucketSpan() {
        return bucketSpan;
    }

    public double getOverallScore() {
        return overallScore;
    }

    public List<JobInfo> getJobs() {
        return jobs;
    }

    void setJobs(List<JobInfo> jobs) {
        this.jobs = Collections.unmodifiableList(jobs);
    }

    public boolean isInterim() {
        return isInterim;
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, bucketSpan, overallScore, jobs, isInterim);
    }

    /**
     * Compare all the fields and embedded anomaly records (if any)
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        OverallBucket that = (OverallBucket) other;

        return Objects.equals(this.timestamp, that.timestamp)
                && this.bucketSpan == that.bucketSpan
                && this.overallScore == that.overallScore
                && Objects.equals(this.jobs, that.jobs)
                && this.isInterim == that.isInterim;
    }

    public static class JobInfo implements ToXContentObject, Comparable<JobInfo> {

        private static final ParseField MAX_ANOMALY_SCORE = new ParseField("max_anomaly_score");

        public static final ConstructingObjectParser<JobInfo, Void> PARSER =
            new ConstructingObjectParser<>("job_info", true, a -> new JobInfo((String) a[0], (double) a[1]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
            PARSER.declareDouble(ConstructingObjectParser.constructorArg(), MAX_ANOMALY_SCORE);
        }

        private final String jobId;
        private final double maxAnomalyScore;

        JobInfo(String jobId, double maxAnomalyScore) {
            this.jobId = Objects.requireNonNull(jobId);
            this.maxAnomalyScore = maxAnomalyScore;
        }

        public String getJobId() {
            return jobId;
        }

        public double getMaxAnomalyScore() {
            return maxAnomalyScore;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Job.ID.getPreferredName(), jobId);
            builder.field(MAX_ANOMALY_SCORE.getPreferredName(), maxAnomalyScore);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, maxAnomalyScore);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            JobInfo that = (JobInfo) other;
            return Objects.equals(this.jobId, that.jobId) && this.maxAnomalyScore == that.maxAnomalyScore;
        }

        @Override
        public int compareTo(JobInfo other) {
            int result = this.jobId.compareTo(other.jobId);
            if (result == 0) {
                result = Double.compare(this.maxAnomalyScore, other.maxAnomalyScore);
            }
            return result;
        }
    }
}
