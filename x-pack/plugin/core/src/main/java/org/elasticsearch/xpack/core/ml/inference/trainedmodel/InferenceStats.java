/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.common.time.TimeUtils;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class InferenceStats implements ToXContentObject, Writeable {

    public static final String NAME = "inference_stats";
    public static final ParseField MISSING_ALL_FIELDS_COUNT = new ParseField("missing_all_fields_count");
    public static final ParseField INFERENCE_COUNT = new ParseField("inference_count");
    public static final ParseField CACHE_MISS_COUNT = new ParseField("cache_miss_count");
    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField NODE_ID = new ParseField("node_id");
    public static final ParseField FAILURE_COUNT = new ParseField("failure_count");
    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");

    public static final ConstructingObjectParser<InferenceStats, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new InferenceStats((Long)a[0], (Long)a[1], (Long)a[2], (Long)a[3], (String)a[4], (String)a[5], (Instant)a[6])
    );
    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MISSING_ALL_FIELDS_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INFERENCE_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), FAILURE_COUNT);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), CACHE_MISS_COUNT);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MODEL_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ObjectParser.ValueType.VALUE);
    }

    public static String docId(String modelId, String nodeId) {
        return NAME + "-" + modelId + "-" + nodeId;
    }

    private final long missingAllFieldsCount;
    private final long inferenceCount;
    private final long failureCount;
    private final long cacheMissCount;
    private final String modelId;
    private final String nodeId;
    private final Instant timeStamp;

    private InferenceStats(Long missingAllFieldsCount,
                           Long inferenceCount,
                           Long failureCount,
                           Long cacheMissCount,
                           String modelId,
                           String nodeId,
                           Instant instant) {
        this(unboxOrZero(missingAllFieldsCount),
            unboxOrZero(inferenceCount),
            unboxOrZero(failureCount),
            unboxOrZero(cacheMissCount),
            modelId,
            nodeId,
            instant);
    }

    public InferenceStats(long missingAllFieldsCount,
                          long inferenceCount,
                          long failureCount,
                          long cacheMissCount,
                          String modelId,
                          String nodeId,
                          Instant timeStamp) {
        this.missingAllFieldsCount = missingAllFieldsCount;
        this.inferenceCount = inferenceCount;
        this.failureCount = failureCount;
        this.cacheMissCount = cacheMissCount;
        this.modelId = modelId;
        this.nodeId = nodeId;
        this.timeStamp = timeStamp == null ?
            Instant.ofEpochMilli(Instant.now().toEpochMilli()) :
            Instant.ofEpochMilli(timeStamp.toEpochMilli());
    }

    public InferenceStats(StreamInput in) throws IOException {
        this.missingAllFieldsCount = in.readVLong();
        this.inferenceCount = in.readVLong();
        this.failureCount = in.readVLong();
        this.cacheMissCount = in.readVLong();
        this.modelId = in.readOptionalString();
        this.nodeId = in.readOptionalString();
        this.timeStamp = in.readInstant();
    }

    public long getMissingAllFieldsCount() {
        return missingAllFieldsCount;
    }

    public long getInferenceCount() {
        return inferenceCount;
    }

    public long getFailureCount() {
        return failureCount;
    }

    public long getCacheMissCount() {
        return cacheMissCount;
    }

    public String getModelId() {
        return modelId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public Instant getTimeStamp() {
        return timeStamp;
    }

    public boolean hasStats() {
        return missingAllFieldsCount > 0 || inferenceCount > 0 || failureCount > 0 || cacheMissCount > 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            assert modelId != null : "model_id cannot be null when storing inference stats";
            assert nodeId != null : "node_id cannot be null when storing inference stats";
            builder.field(TYPE.getPreferredName(), NAME);
            builder.field(MODEL_ID.getPreferredName(), modelId);
            builder.field(NODE_ID.getPreferredName(), nodeId);
        }
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
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(timeStamp, that.timeStamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(missingAllFieldsCount, inferenceCount, failureCount, cacheMissCount, modelId, nodeId, timeStamp);
    }

    @Override
    public String toString() {
        return "InferenceStats{" +
            "missingAllFieldsCount=" + missingAllFieldsCount +
            ", inferenceCount=" + inferenceCount +
            ", failureCount=" + failureCount +
            ", cacheMissCount=" + cacheMissCount +
            ", modelId='" + modelId + '\'' +
            ", nodeId='" + nodeId + '\'' +
            ", timeStamp=" + timeStamp +
            '}';
    }

    private static long unboxOrZero(@Nullable Long value) {
        return value == null ? 0L : value;
    }

    public static Accumulator accumulator(InferenceStats stats) {
        return new Accumulator(stats);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(this.missingAllFieldsCount);
        out.writeVLong(this.inferenceCount);
        out.writeVLong(this.failureCount);
        out.writeVLong(this.cacheMissCount);
        out.writeOptionalString(this.modelId);
        out.writeOptionalString(this.nodeId);
        out.writeInstant(timeStamp);
    }

    public static class Accumulator {

        private long missingFieldsAccumulator = 0L;
        private long inferenceAccumulator = 0L;
        private long failureCountAccumulator = 0L;
        private long cacheMissAccumulator = 0L;
        private final String modelId;
        private final String nodeId;

        public Accumulator(String modelId, String nodeId, long cacheMisses) {
            this.modelId = modelId;
            this.nodeId = nodeId;
            this.cacheMissAccumulator = cacheMisses;
        }

        Accumulator(InferenceStats previousStats) {
            this.modelId = previousStats.modelId;
            this.nodeId = previousStats.nodeId;
            this.missingFieldsAccumulator += previousStats.missingAllFieldsCount;
            this.inferenceAccumulator += previousStats.inferenceCount;
            this.failureCountAccumulator += previousStats.failureCount;
            this.cacheMissAccumulator += previousStats.cacheMissCount;
        }

        /**
         * NOT Thread Safe
         *
         * @param otherStats the other stats with which to increment the current stats
         * @return Updated accumulator
         */
        public Accumulator merge(InferenceStats otherStats) {
            this.missingFieldsAccumulator += otherStats.missingAllFieldsCount;
            this.inferenceAccumulator += otherStats.inferenceCount;
            this.failureCountAccumulator += otherStats.failureCount;
            this.cacheMissAccumulator += otherStats.cacheMissCount;
            return this;
        }

        public synchronized Accumulator incMissingFields() {
            this.missingFieldsAccumulator++;
            return this;
        }

        public synchronized Accumulator incInference() {
            this.inferenceAccumulator++;
            return this;
        }

        public synchronized Accumulator incFailure() {
            this.failureCountAccumulator++;
            return this;
        }

        /**
         * Thread safe.
         *
         * Returns the current stats and resets the values of all the counters.
         * @return The current stats
         */
        public synchronized InferenceStats currentStatsAndReset() {
            InferenceStats stats = currentStats(Instant.now());
            this.missingFieldsAccumulator = 0L;
            this.inferenceAccumulator = 0L;
            this.failureCountAccumulator = 0L;
            this.cacheMissAccumulator = 0L;
            return stats;
        }

        public InferenceStats currentStats(Instant timeStamp) {
            return new InferenceStats(missingFieldsAccumulator,
                inferenceAccumulator,
                failureCountAccumulator,
                cacheMissAccumulator,
                modelId,
                nodeId,
                timeStamp);
        }
    }
}
