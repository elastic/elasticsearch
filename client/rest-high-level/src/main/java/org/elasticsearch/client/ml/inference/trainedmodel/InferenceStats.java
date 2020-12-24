/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml.inference.trainedmodel;

import org.elasticsearch.client.common.TimeUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class InferenceStats implements ToXContentObject {

    public static final String NAME = "inference_stats";
    public static final ParseField MISSING_ALL_FIELDS_COUNT = new ParseField("missing_all_fields_count");
    public static final ParseField INFERENCE_COUNT = new ParseField("inference_count");
    public static final ParseField CACHE_MISS_COUNT = new ParseField("cache_miss_count");
    public static final ParseField FAILURE_COUNT = new ParseField("failure_count");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");

    public static final ConstructingObjectParser<InferenceStats, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new InferenceStats((Long)a[0], (Long)a[1], (Long)a[2], (Long)a[3], (Instant)a[4])
    );
    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MISSING_ALL_FIELDS_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INFERENCE_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), FAILURE_COUNT);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), CACHE_MISS_COUNT);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> TimeUtil.parseTimeFieldToInstant(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ObjectParser.ValueType.VALUE);
    }

    private final long missingAllFieldsCount;
    private final long inferenceCount;
    private final long failureCount;
    private final long cacheMissCount;
    private final Instant timeStamp;

    private InferenceStats(Long missingAllFieldsCount,
                           Long inferenceCount,
                           Long failureCount,
                           Long cacheMissCount,
                           Instant instant) {
        this(unboxOrZero(missingAllFieldsCount),
            unboxOrZero(inferenceCount),
            unboxOrZero(failureCount),
            unboxOrZero(cacheMissCount),
            instant);
    }

    public InferenceStats(long missingAllFieldsCount,
                          long inferenceCount,
                          long failureCount,
                          long cacheMissCount,
                          Instant timeStamp) {
        this.missingAllFieldsCount = missingAllFieldsCount;
        this.inferenceCount = inferenceCount;
        this.failureCount = failureCount;
        this.cacheMissCount = cacheMissCount;
        this.timeStamp = timeStamp == null ?
            Instant.ofEpochMilli(Instant.now().toEpochMilli()) :
            Instant.ofEpochMilli(timeStamp.toEpochMilli());
    }

    /**
     * How many times this model attempted to infer with all its fields missing
     */
    public long getMissingAllFieldsCount() {
        return missingAllFieldsCount;
    }

    /**
     * How many inference calls were made against this model
     */
    public long getInferenceCount() {
        return inferenceCount;
    }

    /**
     * How many inference failures occurred.
     */
    public long getFailureCount() {
        return failureCount;
    }

    /**
     * How many cache misses occurred when inferring this model
     */
    public long getCacheMissCount() {
        return cacheMissCount;
    }

    /**
     * The timestamp of these statistics.
     */
    public Instant getTimeStamp() {
        return timeStamp;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FAILURE_COUNT.getPreferredName(), failureCount);
        builder.field(INFERENCE_COUNT.getPreferredName(), inferenceCount);
        builder.field(CACHE_MISS_COUNT.getPreferredName(), cacheMissCount);
        builder.field(MISSING_ALL_FIELDS_COUNT.getPreferredName(), missingAllFieldsCount);
        builder.timeField(TIMESTAMP.getPreferredName(), TIMESTAMP.getPreferredName() + "_string", timeStamp.toEpochMilli());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceStats that = (InferenceStats) o;
        return missingAllFieldsCount == that.missingAllFieldsCount
            && inferenceCount == that.inferenceCount
            && failureCount == that.failureCount
            && cacheMissCount == that.cacheMissCount
            && Objects.equals(timeStamp, that.timeStamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(missingAllFieldsCount, inferenceCount, failureCount, cacheMissCount, timeStamp);
    }

    @Override
    public String toString() {
        return "InferenceStats{" +
            "missingAllFieldsCount=" + missingAllFieldsCount +
            ", inferenceCount=" + inferenceCount +
            ", failureCount=" + failureCount +
            ", cacheMissCount=" + cacheMissCount +
            ", timeStamp=" + timeStamp +
            '}';
    }

    private static long unboxOrZero(@Nullable Long value) {
        return value == null ? 0L : value;
    }

}
