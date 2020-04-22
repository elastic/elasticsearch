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
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InferenceStats implements ToXContentObject, Writeable {

    public static final String NAME = "inference_stats";
    public static final ParseField MISSING_ALL_FIELDS_COUNT = new ParseField("missing_all_fields_count");
    public static final ParseField INFERENCE_COUNT = new ParseField("inference_count");
    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField NODE_ID = new ParseField("node_id");
    public static final ParseField FAILURE_COUNT = new ParseField("failure_count");
    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");

    public static final ConstructingObjectParser<InferenceStats, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new InferenceStats((Long)a[0], (Long)a[1], (Long)a[2], (String)a[3], (String)a[4], (Instant)a[5])
    );
    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MISSING_ALL_FIELDS_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INFERENCE_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), FAILURE_COUNT);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MODEL_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ObjectParser.ValueType.VALUE);
    }
    public static InferenceStats emptyStats(String modelId, String nodeId) {
        return new InferenceStats(0L, 0L, 0L, modelId, nodeId, Instant.now());
    }

    public static String docId(String modelId, String nodeId) {
        return NAME + "-" + modelId + "-" + nodeId;
    }

    private final long missingAllFieldsCount;
    private final long inferenceCount;
    private final long failureCount;
    private final String modelId;
    private final String nodeId;
    private final Instant timeStamp;

    private InferenceStats(Long missingAllFieldsCount,
                           Long inferenceCount,
                           Long failureCount,
                           String modelId,
                           String nodeId,
                           Instant instant) {
        this(unbox(missingAllFieldsCount),
            unbox(inferenceCount),
            unbox(failureCount),
            modelId,
            nodeId,
            instant);
    }

    public InferenceStats(long missingAllFieldsCount,
                          long inferenceCount,
                          long failureCount,
                          String modelId,
                          String nodeId,
                          Instant timeStamp) {
        this.missingAllFieldsCount = missingAllFieldsCount;
        this.inferenceCount = inferenceCount;
        this.failureCount = failureCount;
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
        return missingAllFieldsCount > 0 || inferenceCount > 0 || failureCount > 0;
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
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(timeStamp, that.timeStamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(missingAllFieldsCount, inferenceCount, failureCount, modelId, nodeId, timeStamp);
    }

    @Override
    public String toString() {
        return "InferenceStats{" +
            "missingAllFieldsCount=" + missingAllFieldsCount +
            ", inferenceCount=" + inferenceCount +
            ", failureCount=" + failureCount +
            ", modelId='" + modelId + '\'' +
            ", nodeId='" + nodeId + '\'' +
            ", timeStamp=" + timeStamp +
            '}';
    }

    private static long unbox(@Nullable Long value) {
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
        out.writeOptionalString(this.modelId);
        out.writeOptionalString(this.nodeId);
        out.writeInstant(timeStamp);
    }

    public static class Accumulator {

        private final LongAdder missingFieldsAccumulator = new LongAdder();
        private final LongAdder inferenceAccumulator = new LongAdder();
        private final LongAdder failureCountAccumulator = new LongAdder();
        private final String modelId;
        private final String nodeId;
        // curious reader
        // you may be wondering why the lock set to the fair.
        // When `currentStatsAndReset` is called, we want it guaranteed that it will eventually execute.
        // If a ReadWriteLock is unfair, there are no such guarantees.
        // A call for the `writelock::lock` could pause indefinitely.
        private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);

        public Accumulator(String modelId, String nodeId) {
            this.modelId = modelId;
            this.nodeId = nodeId;
        }

        public Accumulator(InferenceStats previousStats) {
            this.modelId = previousStats.modelId;
            this.nodeId = previousStats.nodeId;
            this.missingFieldsAccumulator.add(previousStats.missingAllFieldsCount);
            this.inferenceAccumulator.add(previousStats.inferenceCount);
            this.failureCountAccumulator.add(previousStats.failureCount);
        }

        public Accumulator merge(InferenceStats otherStats) {
            this.missingFieldsAccumulator.add(otherStats.missingAllFieldsCount);
            this.inferenceAccumulator.add(otherStats.inferenceCount);
            this.failureCountAccumulator.add(otherStats.failureCount);
            return this;
        }

        public Accumulator incMissingFields() {
            readWriteLock.readLock().lock();
            try {
                this.missingFieldsAccumulator.increment();
                return this;
            } finally {
                readWriteLock.readLock().unlock();
            }
        }

        public Accumulator incInference() {
            readWriteLock.readLock().lock();
            try {
                this.inferenceAccumulator.increment();
                return this;
            } finally {
                readWriteLock.readLock().unlock();
            }
        }

        public Accumulator incFailure() {
            readWriteLock.readLock().lock();
            try {
                this.failureCountAccumulator.increment();
                return this;
            } finally {
                readWriteLock.readLock().unlock();
            }
        }

        /**
         * Thread safe.
         *
         * Returns the current stats and resets the values of all the counters.
         * @return The current stats
         */
        public InferenceStats currentStatsAndReset() {
            readWriteLock.writeLock().lock();
            try {
                InferenceStats stats = currentStats(Instant.now());
                this.missingFieldsAccumulator.reset();
                this.inferenceAccumulator.reset();
                this.failureCountAccumulator.reset();
                return stats;
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }

        public InferenceStats currentStats(Instant timeStamp) {
            return new InferenceStats(missingFieldsAccumulator.longValue(),
                inferenceAccumulator.longValue(),
                failureCountAccumulator.longValue(),
                modelId,
                nodeId,
                timeStamp);
        }
    }
}
