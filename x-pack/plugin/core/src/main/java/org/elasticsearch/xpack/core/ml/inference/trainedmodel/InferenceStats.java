/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
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

public class InferenceStats implements ToXContentObject, Writeable {

    public static final String NAME = "inference_stats";
    public static final ParseField MISSING_ALL_FIELDS_COUNT = new ParseField("missing_all_fields_count");
    public static final ParseField INFERENCE_COUNT = new ParseField("inference_count");
    public static final ParseField MODEL_ID = new ParseField("model_id");
    public static final ParseField NODE_ID = new ParseField("node_id");
    public static final ParseField TOTAL_TIME_SPENT_MILLIS = new ParseField("total_time_spent_millis");
    private static final ParseField TOTAL_TIME_SPENT = new ParseField("total_time_spent");
    public static final ParseField FAILURE_COUNT = new ParseField("failure_count");
    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField TIMESTAMP = new ParseField("time_stamp");

    public static final ConstructingObjectParser<InferenceStats, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new InferenceStats((Long)a[0], (Long)a[1], (Long)a[2], (Long)a[3], (String)a[4], (String)a[5], (Instant) a[6])
    );
    static {
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MISSING_ALL_FIELDS_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), INFERENCE_COUNT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_TIME_SPENT_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), FAILURE_COUNT);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MODEL_ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            p -> TimeUtils.parseTimeFieldToInstant(p, TIMESTAMP.getPreferredName()),
            TIMESTAMP,
            ObjectParser.ValueType.VALUE);
    }
    public static InferenceStats emptyStats(String modelId, String nodeId) {
        return new InferenceStats(0L, 0L, 0L, 0L, modelId, nodeId, Instant.now());
    }

    public static String docId(String modelId, String nodeId) {
        return NAME + "-" + modelId + "-" + nodeId;
    }

    private final long missingAllFieldsCount;
    private final long inferenceCount;
    private final long totalTimeSpent;
    private final long failureCount;
    private final String modelId;
    private final String nodeId;
    private final Instant timeStamp;

    private InferenceStats(Long missingAllFieldsCount,
                           Long inferenceCount,
                           Long totalTimeSpent,
                           Long failureCount,
                           String modelId,
                           String nodeId,
                           Instant instant) {
        this(unbox(missingAllFieldsCount),
            unbox(inferenceCount),
            unbox(totalTimeSpent),
            unbox(failureCount),
            modelId,
            nodeId,
            instant == null ? Instant.now() : Instant.ofEpochMilli(instant.toEpochMilli()));
    }


    public InferenceStats(long missingAllFieldsCount,
                          long inferenceCount,
                          long totalTimeSpent,
                          long failureCount,
                          String modelId,
                          String nodeId,
                          Instant timeStamp) {
        this.missingAllFieldsCount = missingAllFieldsCount;
        this.inferenceCount = inferenceCount;
        this.totalTimeSpent = totalTimeSpent;
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
        this.totalTimeSpent = in.readVLong();
        this.failureCount = in.readVLong();
        this.modelId = in.readOptionalString();
        this.nodeId = in.readOptionalString();
        this.timeStamp = Instant.ofEpochMilli(in.readInstant().toEpochMilli());
    }

    public long getMissingAllFieldsCount() {
        return missingAllFieldsCount;
    }

    public long getInferenceCount() {
        return inferenceCount;
    }

    public long getTotalTimeSpent() {
        return totalTimeSpent;
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
        builder.timeField(TOTAL_TIME_SPENT_MILLIS.getPreferredName(), TOTAL_TIME_SPENT.getPreferredName(), totalTimeSpent);
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
            && totalTimeSpent == that.totalTimeSpent
            && failureCount == that.failureCount
            && Objects.equals(modelId, that.modelId)
            && Objects.equals(nodeId, that.nodeId)
            && Objects.equals(timeStamp, that.timeStamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(missingAllFieldsCount, inferenceCount, totalTimeSpent, failureCount, modelId, nodeId, timeStamp);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    private static long unbox(@Nullable Long value) {
        return value == null ? 0L : value;
    }

    public static Accumulator accumulator(String modelId, String nodeId) {
        return new Accumulator(modelId, nodeId);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(this.missingAllFieldsCount);
        out.writeVLong(this.inferenceCount);
        out.writeVLong(this.totalTimeSpent);
        out.writeVLong(this.failureCount);
        out.writeOptionalString(this.modelId);
        out.writeOptionalString(this.nodeId);
        out.writeInstant(timeStamp);
    }

    public static class Accumulator {

        private final LongAdder missingFieldsAccumulator = new LongAdder();
        private final LongAdder inferenceAccumulator = new LongAdder();
        private final LongAdder totalTimeSpentAccumulator = new LongAdder();
        private final LongAdder failureCountAccumulator = new LongAdder();
        private final String modelId;
        private final String nodeId;

        public Accumulator(String modelId, String nodeId) {
            this.modelId = modelId;
            this.nodeId = nodeId;
        }

        public Accumulator(InferenceStats previousStats) {
            this.modelId = previousStats.modelId;
            this.nodeId = previousStats.nodeId;
            this.missingFieldsAccumulator.add(previousStats.missingAllFieldsCount);
            this.inferenceAccumulator.add(previousStats.inferenceCount);
            this.totalTimeSpentAccumulator.add(TimeValue.timeValueMillis(previousStats.totalTimeSpent).nanos());
            this.failureCountAccumulator.add(previousStats.failureCount);
        }

        public void merge(InferenceStats otherStats) {
            this.missingFieldsAccumulator.add(otherStats.missingAllFieldsCount);
            this.inferenceAccumulator.add(otherStats.inferenceCount);
            this.totalTimeSpentAccumulator.add(TimeValue.timeValueMillis(otherStats.totalTimeSpent).nanos());
            this.failureCountAccumulator.add(otherStats.failureCount);
        }

        public void incMissingFields() {
            this.missingFieldsAccumulator.increment();
        }

        public void incInference() {
            this.inferenceAccumulator.increment();
        }

        public void incFailure() {
            this.failureCountAccumulator.increment();
        }

        public void timeSpent(long value) {
            this.totalTimeSpentAccumulator.add(value);
        }

        public InferenceStats currentStats() {
            return currentStats(Instant.now());
        }

        public InferenceStats currentStats(Instant timeStamp) {
            return new InferenceStats(missingFieldsAccumulator.longValue(),
                inferenceAccumulator.longValue(),
                TimeValue.timeValueNanos(totalTimeSpentAccumulator.longValue()).getMillis(),
                failureCountAccumulator.longValue(),
                modelId,
                nodeId,
                timeStamp);
        }
    }
}
