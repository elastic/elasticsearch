/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Stats that give more insight into timing of various operations performed as part of anomaly detection job.
 */
public class TimingStats implements ToXContentObject {

    public static final ParseField BUCKET_COUNT = new ParseField("bucket_count");
    public static final ParseField TOTAL_BUCKET_PROCESSING_TIME_MS = new ParseField("total_bucket_processing_time_ms");
    public static final ParseField MIN_BUCKET_PROCESSING_TIME_MS = new ParseField("minimum_bucket_processing_time_ms");
    public static final ParseField MAX_BUCKET_PROCESSING_TIME_MS = new ParseField("maximum_bucket_processing_time_ms");
    public static final ParseField AVG_BUCKET_PROCESSING_TIME_MS = new ParseField("average_bucket_processing_time_ms");
    public static final ParseField EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_MS =
        new ParseField("exponential_average_bucket_processing_time_ms");
    public static final ParseField EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_PER_HOUR_MS =
        new ParseField("exponential_average_bucket_processing_time_per_hour_ms");

    public static final ConstructingObjectParser<TimingStats, Void> PARSER =
        new ConstructingObjectParser<>(
            "timing_stats",
            true,
            args -> {
                String jobId = (String) args[0];
                Long bucketCount = (Long) args[1];
                Double totalBucketProcessingTimeMs = (Double) args[2];
                Double minBucketProcessingTimeMs = (Double) args[3];
                Double maxBucketProcessingTimeMs = (Double) args[4];
                Double avgBucketProcessingTimeMs = (Double) args[5];
                Double exponentialAvgBucketProcessingTimeMs = (Double) args[6];
                Double exponentialAvgBucketProcessingTimePerHourMs = (Double) args[7];
                return new TimingStats(
                    jobId,
                    getOrDefault(bucketCount, 0L),
                    getOrDefault(totalBucketProcessingTimeMs, 0.0),
                    minBucketProcessingTimeMs,
                    maxBucketProcessingTimeMs,
                    avgBucketProcessingTimeMs,
                    exponentialAvgBucketProcessingTimeMs,
                    exponentialAvgBucketProcessingTimePerHourMs);
            });

    static {
        PARSER.declareString(constructorArg(), Job.ID);
        PARSER.declareLong(optionalConstructorArg(), BUCKET_COUNT);
        PARSER.declareDouble(optionalConstructorArg(), TOTAL_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), MIN_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), MAX_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), AVG_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_PER_HOUR_MS);
    }

    private final String jobId;
    private long bucketCount;
    private double totalBucketProcessingTimeMs;
    private Double minBucketProcessingTimeMs;
    private Double maxBucketProcessingTimeMs;
    private Double avgBucketProcessingTimeMs;
    private Double exponentialAvgBucketProcessingTimeMs;
    private Double exponentialAvgBucketProcessingTimePerHourMs;

    public TimingStats(
            String jobId,
            long bucketCount,
            double totalBucketProcessingTimeMs,
            @Nullable Double minBucketProcessingTimeMs,
            @Nullable Double maxBucketProcessingTimeMs,
            @Nullable Double avgBucketProcessingTimeMs,
            @Nullable Double exponentialAvgBucketProcessingTimeMs,
            @Nullable Double exponentialAvgBucketProcessingTimePerHourMs) {
        this.jobId = jobId;
        this.bucketCount = bucketCount;
        this.totalBucketProcessingTimeMs = totalBucketProcessingTimeMs;
        this.minBucketProcessingTimeMs = minBucketProcessingTimeMs;
        this.maxBucketProcessingTimeMs = maxBucketProcessingTimeMs;
        this.avgBucketProcessingTimeMs = avgBucketProcessingTimeMs;
        this.exponentialAvgBucketProcessingTimeMs = exponentialAvgBucketProcessingTimeMs;
        this.exponentialAvgBucketProcessingTimePerHourMs = exponentialAvgBucketProcessingTimePerHourMs;
    }

    public String getJobId() {
        return jobId;
    }

    public long getBucketCount() {
        return bucketCount;
    }

    public double getTotalBucketProcessingTimeMs() {
        return totalBucketProcessingTimeMs;
    }

    public Double getMinBucketProcessingTimeMs() {
        return minBucketProcessingTimeMs;
    }

    public Double getMaxBucketProcessingTimeMs() {
        return maxBucketProcessingTimeMs;
    }

    public Double getAvgBucketProcessingTimeMs() {
        return avgBucketProcessingTimeMs;
    }

    public Double getExponentialAvgBucketProcessingTimeMs() {
        return exponentialAvgBucketProcessingTimeMs;
    }

    public Double getExponentialAvgBucketProcessingTimePerHourMs() {
        return exponentialAvgBucketProcessingTimePerHourMs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(BUCKET_COUNT.getPreferredName(), bucketCount);
        builder.field(TOTAL_BUCKET_PROCESSING_TIME_MS.getPreferredName(), totalBucketProcessingTimeMs);
        if (minBucketProcessingTimeMs != null) {
            builder.field(MIN_BUCKET_PROCESSING_TIME_MS.getPreferredName(), minBucketProcessingTimeMs);
        }
        if (maxBucketProcessingTimeMs != null) {
            builder.field(MAX_BUCKET_PROCESSING_TIME_MS.getPreferredName(), maxBucketProcessingTimeMs);
        }
        if (avgBucketProcessingTimeMs != null) {
            builder.field(AVG_BUCKET_PROCESSING_TIME_MS.getPreferredName(), avgBucketProcessingTimeMs);
        }
        if (exponentialAvgBucketProcessingTimeMs != null) {
            builder.field(EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_MS.getPreferredName(), exponentialAvgBucketProcessingTimeMs);
        }
        if (exponentialAvgBucketProcessingTimePerHourMs != null) {
            builder.field(
                EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_PER_HOUR_MS.getPreferredName(), exponentialAvgBucketProcessingTimePerHourMs);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimingStats that = (TimingStats) o;
        return Objects.equals(this.jobId, that.jobId)
            && this.bucketCount == that.bucketCount
            && this.totalBucketProcessingTimeMs == that.totalBucketProcessingTimeMs
            && Objects.equals(this.minBucketProcessingTimeMs, that.minBucketProcessingTimeMs)
            && Objects.equals(this.maxBucketProcessingTimeMs, that.maxBucketProcessingTimeMs)
            && Objects.equals(this.avgBucketProcessingTimeMs, that.avgBucketProcessingTimeMs)
            && Objects.equals(this.exponentialAvgBucketProcessingTimeMs, that.exponentialAvgBucketProcessingTimeMs)
            && Objects.equals(this.exponentialAvgBucketProcessingTimePerHourMs, that.exponentialAvgBucketProcessingTimePerHourMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            bucketCount,
            totalBucketProcessingTimeMs,
            minBucketProcessingTimeMs,
            maxBucketProcessingTimeMs,
            avgBucketProcessingTimeMs,
            exponentialAvgBucketProcessingTimeMs,
            exponentialAvgBucketProcessingTimePerHourMs);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    private static <T> T getOrDefault(@Nullable T value, T defaultValue) {
        return value != null ? value : defaultValue;
    }
}
