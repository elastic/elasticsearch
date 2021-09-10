/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Overall Bucket Result POJO
 */
public class OverallBucket implements ToXContentObject, Writeable {

    public static final ParseField OVERALL_SCORE = new ParseField("overall_score");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");
    public static final ParseField JOBS = new ParseField("jobs");

    // Used for QueryPage
    public static final ParseField RESULTS_FIELD = new ParseField("overall_buckets");

    /**
     * Result type
     */
    public static final String RESULT_TYPE_VALUE = "overall_bucket";

    private final Date timestamp;
    private final long bucketSpan;
    private final double overallScore;
    private final List<JobInfo> jobs;
    private final boolean isInterim;

    public OverallBucket(Date timestamp, long bucketSpan, double overallScore, List<JobInfo> jobs, boolean isInterim) {
        this.timestamp = ExceptionsHelper.requireNonNull(timestamp, Result.TIMESTAMP.getPreferredName());
        this.bucketSpan = bucketSpan;
        this.overallScore = overallScore;
        this.jobs = jobs;
        this.isInterim = isInterim;
    }

    public OverallBucket(StreamInput in) throws IOException {
        timestamp = new Date(in.readLong());
        bucketSpan = in.readLong();
        overallScore = in.readDouble();
        jobs = in.readList(JobInfo::new);
        isInterim = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp.getTime());
        out.writeLong(bucketSpan);
        out.writeDouble(overallScore);
        out.writeList(jobs);
        out.writeBoolean(isInterim);
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

        if (other instanceof OverallBucket == false) {
            return false;
        }

        OverallBucket that = (OverallBucket) other;

        return Objects.equals(this.timestamp, that.timestamp)
                && this.bucketSpan == that.bucketSpan
                && this.overallScore == that.overallScore
                && Objects.equals(this.jobs, that.jobs)
                && this.isInterim == that.isInterim;
    }

    public static class JobInfo implements ToXContentObject, Writeable, Comparable<JobInfo> {

        private static final ParseField MAX_ANOMALY_SCORE = new ParseField("max_anomaly_score");

        private final String jobId;
        private final double maxAnomalyScore;

        public JobInfo(String jobId, double maxAnomalyScore) {
            this.jobId = Objects.requireNonNull(jobId);
            this.maxAnomalyScore = maxAnomalyScore;
        }

        public JobInfo(StreamInput in) throws IOException {
            jobId = in.readString();
            maxAnomalyScore = in.readDouble();
        }

        public String getJobId() {
            return jobId;
        }

        public double getMaxAnomalyScore() {
            return maxAnomalyScore;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(jobId);
            out.writeDouble(maxAnomalyScore);
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
            if (other instanceof JobInfo == false) {
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
