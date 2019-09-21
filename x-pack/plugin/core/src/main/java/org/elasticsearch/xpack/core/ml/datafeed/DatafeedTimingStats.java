/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.utils.ExponentialAverageCalculationContext;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DatafeedTimingStats implements ToXContentObject, Writeable {

    public static final ParseField JOB_ID = new ParseField("job_id");
    public static final ParseField SEARCH_COUNT = new ParseField("search_count");
    public static final ParseField BUCKET_COUNT = new ParseField("bucket_count");
    public static final ParseField TOTAL_SEARCH_TIME_MS = new ParseField("total_search_time_ms");
    public static final ParseField AVG_SEARCH_TIME_PER_BUCKET_MS = new ParseField("average_search_time_per_bucket_ms");
    public static final ParseField EXPONENTIAL_AVG_CALCULATION_CONTEXT = new ParseField("exponential_average_calculation_context");
    public static final ParseField EXPONENTIAL_AVG_SEARCH_TIME_PER_HOUR_MS = new ParseField("exponential_average_search_time_per_hour_ms");

    public static final ParseField TYPE = new ParseField("datafeed_timing_stats");

    public static final ConstructingObjectParser<DatafeedTimingStats, Void> PARSER = createParser();

    private static ConstructingObjectParser<DatafeedTimingStats, Void> createParser() {
        ConstructingObjectParser<DatafeedTimingStats, Void> parser =
            new ConstructingObjectParser<>(
                TYPE.getPreferredName(),
                true,
                args -> {
                    String jobId = (String) args[0];
                    Long searchCount = (Long) args[1];
                    Long bucketCount = (Long) args[2];
                    Double totalSearchTimeMs = (Double) args[3];
                    ExponentialAverageCalculationContext exponentialAvgCalculationContext = (ExponentialAverageCalculationContext) args[4];
                    return new DatafeedTimingStats(
                        jobId,
                        getOrDefault(searchCount, 0L),
                        getOrDefault(bucketCount, 0L),
                        getOrDefault(totalSearchTimeMs, 0.0),
                        getOrDefault(exponentialAvgCalculationContext, new ExponentialAverageCalculationContext()));
                });
        parser.declareString(constructorArg(), JOB_ID);
        parser.declareLong(optionalConstructorArg(), SEARCH_COUNT);
        parser.declareLong(optionalConstructorArg(), BUCKET_COUNT);
        parser.declareDouble(optionalConstructorArg(), TOTAL_SEARCH_TIME_MS);
        parser.declareObject(optionalConstructorArg(), ExponentialAverageCalculationContext.PARSER, EXPONENTIAL_AVG_CALCULATION_CONTEXT);
        return parser;
    }

    public static String documentId(String jobId) {
        return jobId + "_datafeed_timing_stats";
    }

    private final String jobId;
    private long searchCount;
    private long bucketCount;
    private double totalSearchTimeMs;
    private final ExponentialAverageCalculationContext exponentialAvgCalculationContext;

    public DatafeedTimingStats(
            String jobId,
            long searchCount,
            long bucketCount,
            double totalSearchTimeMs,
            ExponentialAverageCalculationContext exponentialAvgCalculationContext) {
        this.jobId = Objects.requireNonNull(jobId);
        this.searchCount = searchCount;
        this.bucketCount = bucketCount;
        this.totalSearchTimeMs = totalSearchTimeMs;
        this.exponentialAvgCalculationContext = Objects.requireNonNull(exponentialAvgCalculationContext);
    }

    public DatafeedTimingStats(String jobId) {
        this(jobId, 0, 0, 0.0, new ExponentialAverageCalculationContext());
    }

    public DatafeedTimingStats(StreamInput in) throws IOException {
        this.jobId = in.readString();
        this.searchCount = in.readLong();
        this.bucketCount = in.readLong();
        this.totalSearchTimeMs = in.readDouble();
        this.exponentialAvgCalculationContext = in.readOptionalWriteable(ExponentialAverageCalculationContext::new);
    }

    public DatafeedTimingStats(DatafeedTimingStats other) {
        this(
            other.jobId,
            other.searchCount,
            other.bucketCount,
            other.totalSearchTimeMs,
            new ExponentialAverageCalculationContext(other.exponentialAvgCalculationContext));
    }

    public String getJobId() {
        return jobId;
    }

    public long getSearchCount() {
        return searchCount;
    }

    public long getBucketCount() {
        return bucketCount;
    }

    public double getTotalSearchTimeMs() {
        return totalSearchTimeMs;
    }

    public Double getAvgSearchTimePerBucketMs() {
        if (bucketCount == 0) return null;
        return totalSearchTimeMs / bucketCount;
    }

    public Double getExponentialAvgSearchTimePerHourMs() {
        return exponentialAvgCalculationContext.getCurrentExponentialAverageMs();
    }

    // Visible for testing
    ExponentialAverageCalculationContext getExponentialAvgCalculationContext() {
        return exponentialAvgCalculationContext;
    }

    public void incrementSearchTimeMs(double searchTimeMs) {
        this.searchCount++;
        this.totalSearchTimeMs += searchTimeMs;
        this.exponentialAvgCalculationContext.increment(searchTimeMs);
    }

    public void incrementBucketCount(long bucketCount) {
        this.bucketCount += bucketCount;
    }

    public void setLatestRecordTimestamp(Instant latestRecordTimestamp) {
        this.exponentialAvgCalculationContext.setLatestTimestamp(latestRecordTimestamp);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeLong(searchCount);
        out.writeLong(bucketCount);
        out.writeDouble(totalSearchTimeMs);
        out.writeOptionalWriteable(exponentialAvgCalculationContext);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(Result.RESULT_TYPE.getPreferredName(), TYPE.getPreferredName());
        }
        builder.field(JOB_ID.getPreferredName(), jobId);
        builder.field(SEARCH_COUNT.getPreferredName(), searchCount);
        builder.field(BUCKET_COUNT.getPreferredName(), bucketCount);
        builder.field(TOTAL_SEARCH_TIME_MS.getPreferredName(), totalSearchTimeMs);
        if (params.paramAsBoolean(ToXContentParams.INCLUDE_CALCULATED_FIELDS, false)) {
            Double avgSearchTimePerBucketMs = getAvgSearchTimePerBucketMs();
            if (avgSearchTimePerBucketMs != null) {
                builder.field(AVG_SEARCH_TIME_PER_BUCKET_MS.getPreferredName(), avgSearchTimePerBucketMs);
            }
            Double expAvgSearchTimePerHourMs = getExponentialAvgSearchTimePerHourMs();
            if (expAvgSearchTimePerHourMs != null) {
                builder.field(EXPONENTIAL_AVG_SEARCH_TIME_PER_HOUR_MS.getPreferredName(), expAvgSearchTimePerHourMs);
            }
        }
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(EXPONENTIAL_AVG_CALCULATION_CONTEXT.getPreferredName(), exponentialAvgCalculationContext);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        DatafeedTimingStats other = (DatafeedTimingStats) obj;
        return Objects.equals(this.jobId, other.jobId)
            && this.searchCount == other.searchCount
            && this.bucketCount == other.bucketCount
            && this.totalSearchTimeMs == other.totalSearchTimeMs
            && Objects.equals(this.exponentialAvgCalculationContext, other.exponentialAvgCalculationContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            searchCount,
            bucketCount,
            totalSearchTimeMs,
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
