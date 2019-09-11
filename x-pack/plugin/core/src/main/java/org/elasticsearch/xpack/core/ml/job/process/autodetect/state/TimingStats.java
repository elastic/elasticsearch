/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import org.elasticsearch.Version;
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
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Stats that give more insight into timing of various operations performed as part of anomaly detection job.
 */
public class TimingStats implements ToXContentObject, Writeable {

    public static final ParseField BUCKET_COUNT = new ParseField("bucket_count");
    public static final ParseField TOTAL_BUCKET_PROCESSING_TIME_MS = new ParseField("total_bucket_processing_time_ms");
    public static final ParseField MIN_BUCKET_PROCESSING_TIME_MS = new ParseField("minimum_bucket_processing_time_ms");
    public static final ParseField MAX_BUCKET_PROCESSING_TIME_MS = new ParseField("maximum_bucket_processing_time_ms");
    public static final ParseField AVG_BUCKET_PROCESSING_TIME_MS = new ParseField("average_bucket_processing_time_ms");
    public static final ParseField EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_MS =
        new ParseField("exponential_average_bucket_processing_time_ms");
    public static final ParseField EXPONENTIAL_AVG_CALCULATION_CONTEXT = new ParseField("exponential_average_calculation_context");
    public static final ParseField EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_PER_HOUR_MS =
        new ParseField("exponential_average_bucket_processing_time_per_hour_ms");

    public static final ParseField TYPE = new ParseField("timing_stats");

    public static final ConstructingObjectParser<TimingStats, Void> PARSER =
        new ConstructingObjectParser<>(
            TYPE.getPreferredName(),
            true,
            args -> {
                String jobId = (String) args[0];
                long bucketCount = (long) args[1];
                Double minBucketProcessingTimeMs = (Double) args[2];
                Double maxBucketProcessingTimeMs = (Double) args[3];
                Double avgBucketProcessingTimeMs = (Double) args[4];
                Double exponentialAvgBucketProcessingTimeMs = (Double) args[5];
                ExponentialAverageCalculationContext exponentialAvgCalculationContext = (ExponentialAverageCalculationContext) args[6];
                return new TimingStats(
                    jobId,
                    bucketCount,
                    minBucketProcessingTimeMs,
                    maxBucketProcessingTimeMs,
                    avgBucketProcessingTimeMs,
                    exponentialAvgBucketProcessingTimeMs,
                    getOrDefault(exponentialAvgCalculationContext, new ExponentialAverageCalculationContext()));
            });

    static {
        PARSER.declareString(constructorArg(), Job.ID);
        PARSER.declareLong(constructorArg(), BUCKET_COUNT);
        PARSER.declareDouble(optionalConstructorArg(), MIN_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), MAX_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), AVG_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareObject(optionalConstructorArg(), ExponentialAverageCalculationContext.PARSER, EXPONENTIAL_AVG_CALCULATION_CONTEXT);
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
    private final ExponentialAverageCalculationContext exponentialAvgCalculationContext;

    public TimingStats(
            String jobId,
            long bucketCount,
            @Nullable Double minBucketProcessingTimeMs,
            @Nullable Double maxBucketProcessingTimeMs,
            @Nullable Double avgBucketProcessingTimeMs,
            @Nullable Double exponentialAvgBucketProcessingTimeMs,
            ExponentialAverageCalculationContext exponentialAvgCalculationContext) {
        this.jobId = Objects.requireNonNull(jobId);
        this.bucketCount = bucketCount;
        this.minBucketProcessingTimeMs = minBucketProcessingTimeMs;
        this.maxBucketProcessingTimeMs = maxBucketProcessingTimeMs;
        this.avgBucketProcessingTimeMs = avgBucketProcessingTimeMs;
        this.exponentialAvgBucketProcessingTimeMs = exponentialAvgBucketProcessingTimeMs;
        this.exponentialAvgCalculationContext = Objects.requireNonNull(exponentialAvgCalculationContext);
    }

    public TimingStats(String jobId) {
        this(jobId, 0, null, null, null, null, new ExponentialAverageCalculationContext());
    }

    public TimingStats(TimingStats lhs) {
        this(
            lhs.jobId,
            lhs.bucketCount,
            lhs.minBucketProcessingTimeMs,
            lhs.maxBucketProcessingTimeMs,
            lhs.avgBucketProcessingTimeMs,
            lhs.exponentialAvgBucketProcessingTimeMs,
            new ExponentialAverageCalculationContext(lhs.exponentialAvgCalculationContext));
    }

    public TimingStats(StreamInput in) throws IOException {
        this.jobId = in.readString();
        this.bucketCount = in.readLong();
        this.minBucketProcessingTimeMs = in.readOptionalDouble();
        this.maxBucketProcessingTimeMs = in.readOptionalDouble();
        this.avgBucketProcessingTimeMs = in.readOptionalDouble();
        this.exponentialAvgBucketProcessingTimeMs = in.readOptionalDouble();
        if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
            this.exponentialAvgCalculationContext = in.readOptionalWriteable(ExponentialAverageCalculationContext::new);
        } else {
            this.exponentialAvgCalculationContext = new ExponentialAverageCalculationContext();
        }
    }

    public String getJobId() {
        return jobId;
    }

    public long getBucketCount() {
        return bucketCount;
    }

    /** Calculates total bucket processing time as a product of the all-time average bucket processing time and the number of buckets. */
    public double getTotalBucketProcessingTimeMs() {
        return avgBucketProcessingTimeMs != null
            ? bucketCount * avgBucketProcessingTimeMs
            : 0.0;
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
        return exponentialAvgCalculationContext.getCurrentExponentialAverageMs();
    }

    // Visible for testing
    ExponentialAverageCalculationContext getExponentialAvgCalculationContext() {
        return exponentialAvgCalculationContext;
    }

    /**
     * Updates the statistics (min, max, avg, exponential avg) for the given data point (bucket processing time).
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
        exponentialAvgCalculationContext.increment(bucketProcessingTimeMs);
    }

    public void setLatestRecordTimestamp(Instant latestRecordTimestamp) {
        exponentialAvgCalculationContext.setLatestTimestamp(latestRecordTimestamp);
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
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            out.writeOptionalWriteable(exponentialAvgCalculationContext);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(Result.RESULT_TYPE.getPreferredName(), TYPE.getPreferredName());
        }
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(BUCKET_COUNT.getPreferredName(), bucketCount);
        if (params.paramAsBoolean(ToXContentParams.INCLUDE_CALCULATED_FIELDS, false)) {
            builder.field(TOTAL_BUCKET_PROCESSING_TIME_MS.getPreferredName(), getTotalBucketProcessingTimeMs());
        }
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
        if (params.paramAsBoolean(ToXContentParams.INCLUDE_CALCULATED_FIELDS, false)) {
            Double expAvgBucketProcessingTimePerHourMs = getExponentialAvgBucketProcessingTimePerHourMs();
            if (expAvgBucketProcessingTimePerHourMs != null) {
                builder.field(EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_PER_HOUR_MS.getPreferredName(), expAvgBucketProcessingTimePerHourMs);
            }
        }
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(EXPONENTIAL_AVG_CALCULATION_CONTEXT.getPreferredName(), exponentialAvgCalculationContext);
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
            && Objects.equals(this.exponentialAvgBucketProcessingTimeMs, that.exponentialAvgBucketProcessingTimeMs)
            && Objects.equals(this.exponentialAvgCalculationContext, that.exponentialAvgCalculationContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            bucketCount,
            minBucketProcessingTimeMs,
            maxBucketProcessingTimeMs,
            avgBucketProcessingTimeMs,
            exponentialAvgBucketProcessingTimeMs,
            exponentialAvgCalculationContext);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    private static <T> T getOrDefault(@Nullable T value, T defaultValue) {
        return value != null ? value : defaultValue;
    }
}
