/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.datafeed;

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

public class DatafeedTimingStats implements ToXContentObject {

    public static final ParseField JOB_ID = new ParseField("job_id");
    public static final ParseField SEARCH_COUNT = new ParseField("search_count");
    public static final ParseField BUCKET_COUNT = new ParseField("bucket_count");
    public static final ParseField TOTAL_SEARCH_TIME_MS = new ParseField("total_search_time_ms");
    public static final ParseField AVG_SEARCH_TIME_PER_BUCKET_MS = new ParseField("average_search_time_per_bucket_ms");
    public static final ParseField EXPONENTIAL_AVG_SEARCH_TIME_PER_HOUR_MS = new ParseField("exponential_average_search_time_per_hour_ms");

    public static final ParseField TYPE = new ParseField("datafeed_timing_stats");

    public static final ConstructingObjectParser<DatafeedTimingStats, Void> PARSER = createParser();

    private static ConstructingObjectParser<DatafeedTimingStats, Void> createParser() {
        ConstructingObjectParser<DatafeedTimingStats, Void> parser =
            new ConstructingObjectParser<>(
                "datafeed_timing_stats",
                true,
                args -> {
                    String jobId = (String) args[0];
                    Long searchCount = (Long) args[1];
                    Long bucketCount = (Long) args[2];
                    Double totalSearchTimeMs = (Double) args[3];
                    Double avgSearchTimePerBucketMs = (Double) args[4];
                    Double exponentialAvgSearchTimePerHourMs = (Double) args[5];
                    return new DatafeedTimingStats(
                        jobId,
                        getOrDefault(searchCount, 0L),
                        getOrDefault(bucketCount, 0L),
                        getOrDefault(totalSearchTimeMs, 0.0),
                        avgSearchTimePerBucketMs,
                        exponentialAvgSearchTimePerHourMs);
                });
        parser.declareString(constructorArg(), JOB_ID);
        parser.declareLong(optionalConstructorArg(), SEARCH_COUNT);
        parser.declareLong(optionalConstructorArg(), BUCKET_COUNT);
        parser.declareDouble(optionalConstructorArg(), TOTAL_SEARCH_TIME_MS);
        parser.declareDouble(optionalConstructorArg(), AVG_SEARCH_TIME_PER_BUCKET_MS);
        parser.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_SEARCH_TIME_PER_HOUR_MS);
        return parser;
    }

    private final String jobId;
    private long searchCount;
    private long bucketCount;
    private double totalSearchTimeMs;
    private Double avgSearchTimePerBucketMs;
    private Double exponentialAvgSearchTimePerHourMs;

    public DatafeedTimingStats(
            String jobId,
            long searchCount,
            long bucketCount,
            double totalSearchTimeMs,
            @Nullable Double avgSearchTimePerBucketMs,
            @Nullable Double exponentialAvgSearchTimePerHourMs) {
        this.jobId = Objects.requireNonNull(jobId);
        this.searchCount = searchCount;
        this.bucketCount = bucketCount;
        this.totalSearchTimeMs = totalSearchTimeMs;
        this.avgSearchTimePerBucketMs = avgSearchTimePerBucketMs;
        this.exponentialAvgSearchTimePerHourMs = exponentialAvgSearchTimePerHourMs;
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
        return avgSearchTimePerBucketMs;
    }

    public Double getExponentialAvgSearchTimePerHourMs() {
        return exponentialAvgSearchTimePerHourMs;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(JOB_ID.getPreferredName(), jobId);
        builder.field(SEARCH_COUNT.getPreferredName(), searchCount);
        builder.field(BUCKET_COUNT.getPreferredName(), bucketCount);
        builder.field(TOTAL_SEARCH_TIME_MS.getPreferredName(), totalSearchTimeMs);
        if (avgSearchTimePerBucketMs != null) {
            builder.field(AVG_SEARCH_TIME_PER_BUCKET_MS.getPreferredName(), avgSearchTimePerBucketMs);
        }
        if (exponentialAvgSearchTimePerHourMs != null) {
            builder.field(EXPONENTIAL_AVG_SEARCH_TIME_PER_HOUR_MS.getPreferredName(), exponentialAvgSearchTimePerHourMs);
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
            && Objects.equals(this.avgSearchTimePerBucketMs, other.avgSearchTimePerBucketMs)
            && Objects.equals(this.exponentialAvgSearchTimePerHourMs, other.exponentialAvgSearchTimePerHourMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            jobId,
            searchCount,
            bucketCount,
            totalSearchTimeMs,
            avgSearchTimePerBucketMs,
            exponentialAvgSearchTimePerHourMs);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    private static <T> T getOrDefault(@Nullable T value, T defaultValue) {
        return value != null ? value : defaultValue;
    }
}
