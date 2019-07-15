/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Stats that give more insight into timing of various operations performed as part of anomaly detection job.
 */
public class TimingStats implements ToXContentObject, Writeable {

    public static final ParseField BUCKET_COUNT = new ParseField("bucket_count");
    public static final ParseField MIN_BUCKET_PROCESSING_TIME_MS = new ParseField("minimum_bucket_processing_time_ms");
    public static final ParseField MAX_BUCKET_PROCESSING_TIME_MS = new ParseField("maximum_bucket_processing_time_ms");
    public static final ParseField AVG_BUCKET_PROCESSING_TIME_MS = new ParseField("average_bucket_processing_time_ms");
    public static final ParseField EXPONENTIAL_AVERAGE_BUCKET_PROCESSING_TIME_MS =
        new ParseField("exponential_average_bucket_processing_time_ms");

    public static final ParseField TYPE = new ParseField("timing_stats");

    public static final ConstructingObjectParser<TimingStats, Void> PARSER =
        new ConstructingObjectParser<>(
            TYPE.getPreferredName(),
            true,
            args ->
                new TimingStats((String) args[0], (long) args[1], (Double) args[2], (Double) args[3], (Double) args[4], (Double) args[5]));

    static {
        PARSER.declareString(constructorArg(), Job.ID);
        PARSER.declareLong(constructorArg(), BUCKET_COUNT);
        PARSER.declareDouble(optionalConstructorArg(), MIN_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), MAX_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), AVG_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVERAGE_BUCKET_PROCESSING_TIME_MS);
    }

    public static String documentId(String jobId) {
        return jobId + "_timing_stats";
    }

    private final String jobId;
    private long bucketCount;
    private Double minBucketProcessingTimeMs;
    private Double maxBucketProcessingTimeMs;
    private Double avgBucketProcessingTimeMs;
    private Double exponentialAvgBucketProcessingTimeMs;

    public TimingStats(
            String jobId,
            long bucketCount,
            @Nullable Double minBucketProcessingTimeMs,
            @Nullable Double maxBucketProcessingTimeMs,
            @Nullable Double avgBucketProcessingTimeMs,
            @Nullable Double exponentialAvgBucketProcessingTimeMs) {
        this.jobId = jobId;
        this.bucketCount = bucketCount;
        this.minBucketProcessingTimeMs = minBucketProcessingTimeMs;
        this.maxBucketProcessingTimeMs = maxBucketProcessingTimeMs;
        this.avgBucketProcessingTimeMs = avgBucketProcessingTimeMs;
        this.exponentialAvgBucketProcessingTimeMs = exponentialAvgBucketProcessingTimeMs;
    }

    public TimingStats(String jobId) {
        this(jobId, 0, null, null, null, null);
    }

    public TimingStats(TimingStats lhs) {
        this(
            lhs.jobId,
            lhs.bucketCount,
            lhs.minBucketProcessingTimeMs,
            lhs.maxBucketProcessingTimeMs,
            lhs.avgBucketProcessingTimeMs,
            lhs.exponentialAvgBucketProcessingTimeMs);
    }

    public TimingStats(StreamInput in) throws IOException {
        this.jobId = in.readString();
        this.bucketCount = in.readLong();
        this.minBucketProcessingTimeMs = in.readOptionalDouble();
        this.maxBucketProcessingTimeMs = in.readOptionalDouble();
        this.avgBucketProcessingTimeMs = in.readOptionalDouble();
        this.exponentialAvgBucketProcessingTimeMs = in.readOptionalDouble();
    }

    public String getJobId() {
        return jobId;
    }

    public long getBucketCount() {
        return bucketCount;
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

    /**
     * Updates the statistics (min, max, avg) for the given data point (bucket processing time).
     */
    public void updateStats(double bucketProcessingTimeMs) {
        if (bucketProcessingTimeMs < 0.0) {
            throw new IllegalArgumentException("bucketProcessingTimeMs must be non-negative, was: " + bucketProcessingTimeMs);
        }
        if (minBucketProcessingTimeMs == null || bucketProcessingTimeMs < minBucketProcessingTimeMs) {
            minBucketProcessingTimeMs = bucketProcessingTimeMs;
        }
        if (maxBucketProcessingTimeMs == null || bucketProcessingTimeMs > maxBucketProcessingTimeMs) {
            maxBucketProcessingTimeMs = bucketProcessingTimeMs;
        }
        if (avgBucketProcessingTimeMs == null) {
            avgBucketProcessingTimeMs = bucketProcessingTimeMs;
        } else {
            // Calculate the cumulative moving average (see https://en.wikipedia.org/wiki/Moving_average#Cumulative_moving_average) of
            // bucket processing times.
            avgBucketProcessingTimeMs = (bucketCount * avgBucketProcessingTimeMs + bucketProcessingTimeMs) / (bucketCount + 1);
        }
        if (exponentialAvgBucketProcessingTimeMs == null) {
            exponentialAvgBucketProcessingTimeMs = bucketProcessingTimeMs;
        } else {
            // Calculate the exponential moving average (see https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average) of
            // bucket processing times.
            exponentialAvgBucketProcessingTimeMs = (1 - ALPHA) * exponentialAvgBucketProcessingTimeMs + ALPHA * bucketProcessingTimeMs;
        }
        bucketCount++;
    }

    /**
     * Constant smoothing factor used for calculating exponential moving average. Represents the degree of weighting decrease.
     */
    private static double ALPHA = 0.01;

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeLong(bucketCount);
        out.writeOptionalDouble(minBucketProcessingTimeMs);
        out.writeOptionalDouble(maxBucketProcessingTimeMs);
        out.writeOptionalDouble(avgBucketProcessingTimeMs);
        out.writeOptionalDouble(exponentialAvgBucketProcessingTimeMs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(BUCKET_COUNT.getPreferredName(), bucketCount);
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
            builder.field(EXPONENTIAL_AVERAGE_BUCKET_PROCESSING_TIME_MS.getPreferredName(), exponentialAvgBucketProcessingTimeMs);
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
            && Objects.equals(this.minBucketProcessingTimeMs, that.minBucketProcessingTimeMs)
            && Objects.equals(this.maxBucketProcessingTimeMs, that.maxBucketProcessingTimeMs)
            && Objects.equals(this.avgBucketProcessingTimeMs, that.avgBucketProcessingTimeMs)
            && Objects.equals(this.exponentialAvgBucketProcessingTimeMs, that.exponentialAvgBucketProcessingTimeMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            bucketCount,
            minBucketProcessingTimeMs,
            maxBucketProcessingTimeMs,
            avgBucketProcessingTimeMs,
            exponentialAvgBucketProcessingTimeMs);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Returns true if given stats objects differ from each other by more than 10% for at least one of the statistics.
     */
    public static boolean differSignificantly(TimingStats stats1, TimingStats stats2) {
        return differSignificantly(stats1.minBucketProcessingTimeMs, stats2.minBucketProcessingTimeMs)
            || differSignificantly(stats1.maxBucketProcessingTimeMs, stats2.maxBucketProcessingTimeMs)
            || differSignificantly(stats1.avgBucketProcessingTimeMs, stats2.avgBucketProcessingTimeMs)
            || differSignificantly(stats1.exponentialAvgBucketProcessingTimeMs, stats2.exponentialAvgBucketProcessingTimeMs);
    }

    /**
     * Returns {@code true} if one of the ratios { value1 / value2, value2 / value1 } is smaller than MIN_VALID_RATIO.
     * This can be interpreted as values { value1, value2 } differing significantly from each other.
     * This method also returns:
     *   - {@code true} in case one value is {@code null} while the other is not.
     *   - {@code false} in case both values are {@code null}.
     */
    static boolean differSignificantly(Double value1, Double value2) {
        if (value1 != null && value2 != null) {
            return (value2 / value1 < MIN_VALID_RATIO) || (value1 / value2 < MIN_VALID_RATIO);
        }
        return (value1 != null) || (value2 != null);
    }

    /**
     * Minimum ratio of values that is interpreted as values being similar.
     * If the values ratio is less than MIN_VALID_RATIO, the values are interpreted as significantly different.
     */
    private static final double MIN_VALID_RATIO = 0.9;
}
