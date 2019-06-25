/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
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
    public static final ParseField MIN_BUCKET_PROCESSING_TIME_MS = new ParseField("minimum_bucket_processing_time_ms");
    public static final ParseField MAX_BUCKET_PROCESSING_TIME_MS = new ParseField("maximum_bucket_processing_time_ms");
    public static final ParseField AVG_BUCKET_PROCESSING_TIME_MS = new ParseField("average_bucket_processing_time_ms");
    public static final ParseField EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_MS =
        new ParseField("exponential_average_bucket_processing_time_ms");

    public static final ConstructingObjectParser<TimingStats, Void> PARSER =
        new ConstructingObjectParser<>(
            "timing_stats",
            true,
            args ->
                new TimingStats((String) args[0], (long) args[1], (Double) args[2], (Double) args[3], (Double) args[4], (Double) args[5]));

    static {
        PARSER.declareString(constructorArg(), Job.ID);
        PARSER.declareLong(constructorArg(), BUCKET_COUNT);
        PARSER.declareDouble(optionalConstructorArg(), MIN_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), MAX_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), AVG_BUCKET_PROCESSING_TIME_MS);
        PARSER.declareDouble(optionalConstructorArg(), EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_MS);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
            builder.field(EXPONENTIAL_AVG_BUCKET_PROCESSING_TIME_MS.getPreferredName(), exponentialAvgBucketProcessingTimeMs);
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
}
