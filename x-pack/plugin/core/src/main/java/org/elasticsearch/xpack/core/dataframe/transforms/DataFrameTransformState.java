/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformState implements Task.Status, PersistentTaskState {
    public static final String NAME = DataFrameField.TASK_NAME;

    private final DataFrameTransformTaskState taskState;
    private final IndexerState indexerState;
    private final long generation;

    @Nullable
    private final SortedMap<String, Object> currentPosition;
    @Nullable
    private final String reason;

    private static final ParseField TASK_STATE = new ParseField("task_state");
    private static final ParseField INDEXER_STATE = new ParseField("indexer_state");
    private static final ParseField CURRENT_POSITION = new ParseField("current_position");
    private static final ParseField GENERATION = new ParseField("generation");
    private static final ParseField REASON = new ParseField("reason");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataFrameTransformState, Void> PARSER = new ConstructingObjectParser<>(NAME,
            true,
            args -> new DataFrameTransformState((DataFrameTransformTaskState) args[0],
                (IndexerState) args[1],
                (Map<String, Object>) args[2],
                (long) args[3],
                (String) args[4]));

    static {
        PARSER.declareField(constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return DataFrameTransformTaskState.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, TASK_STATE, ObjectParser.ValueType.STRING);
        PARSER.declareField(constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return IndexerState.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");

        }, INDEXER_STATE, ObjectParser.ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                return p.map();
            }
            if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, CURRENT_POSITION, ObjectParser.ValueType.VALUE_OBJECT_ARRAY);
        PARSER.declareLong(constructorArg(), GENERATION);
        PARSER.declareString(optionalConstructorArg(), REASON);
    }

    public DataFrameTransformState(DataFrameTransformTaskState taskState,
                                   IndexerState indexerState,
                                   @Nullable Map<String, Object> position,
                                   long generation,
                                   @Nullable String reason) {
        this.taskState = taskState;
        this.indexerState = indexerState;
        this.currentPosition = position == null ? null : Collections.unmodifiableSortedMap(new TreeMap<>(position));
        this.generation = generation;
        this.reason = reason;
    }

    public DataFrameTransformState(StreamInput in) throws IOException {
        taskState = DataFrameTransformTaskState.fromStream(in);
        indexerState = IndexerState.fromStream(in);
        currentPosition = in.readBoolean() ? Collections.unmodifiableSortedMap(new TreeMap<>(in.readMap())) : null;
        generation = in.readLong();
        reason = in.readOptionalString();
    }

    public DataFrameTransformTaskState getTaskState() {
        return taskState;
    }

    public IndexerState getIndexerState() {
        return indexerState;
    }

    public Map<String, Object> getPosition() {
        return currentPosition;
    }

    public long getGeneration() {
        return generation;
    }

    public String getReason() {
        return reason;
    }

    public static DataFrameTransformState fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TASK_STATE.getPreferredName(), taskState.value());
        builder.field(INDEXER_STATE.getPreferredName(), indexerState.value());
        if (currentPosition != null) {
            builder.field(CURRENT_POSITION.getPreferredName(), currentPosition);
        }
        builder.field(GENERATION.getPreferredName(), generation);
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskState.writeTo(out);
        indexerState.writeTo(out);
        out.writeBoolean(currentPosition != null);
        if (currentPosition != null) {
            out.writeMap(currentPosition);
        }
        out.writeLong(generation);
        out.writeOptionalString(reason);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformState that = (DataFrameTransformState) other;

        return Objects.equals(this.taskState, that.taskState) &&
            Objects.equals(this.indexerState, that.indexerState) &&
            Objects.equals(this.currentPosition, that.currentPosition) &&
            this.generation == that.generation &&
            Objects.equals(this.reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskState, indexerState, currentPosition, generation, reason);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
