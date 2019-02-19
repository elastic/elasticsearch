/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transform;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformTaskState implements Task.Status, PersistentTaskState {
    public static final String NAME = DataFrameField.TASK_NAME;

    private final DataFrameTransformState state;
    private final long generation;
    private final String reason;

    @Nullable
    private final SortedMap<String, Object> currentPosition;

    private static final ParseField STATE = new ParseField("transform_state");
    private static final ParseField CURRENT_POSITION = new ParseField("current_position");
    private static final ParseField GENERATION = new ParseField("generation");
    private static final ParseField REASON = new ParseField("reason");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataFrameTransformTaskState, Void> PARSER = new ConstructingObjectParser<>(NAME,
            args -> new DataFrameTransformTaskState((DataFrameTransformState) args[0],
                (HashMap<String, Object>) args[1],
                (long) args[2],
                (String) args[3]));

    static {
        PARSER.declareField(constructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return DataFrameTransformState.fromString(p.text());
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");

        }, STATE, ObjectParser.ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), p -> {
            if (p.currentToken() == XContentParser.Token.START_OBJECT) {
                return p.map();
            }
            if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        }, CURRENT_POSITION, ObjectParser.ValueType.VALUE_OBJECT_ARRAY);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), GENERATION);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REASON);
    }

    public DataFrameTransformTaskState(DataFrameTransformState state,
                                       @Nullable Map<String, Object> position,
                                       long generation,
                                       @Nullable String reason) {
        this.state = state;
        this.currentPosition = position == null ? null : Collections.unmodifiableSortedMap(new TreeMap<>(position));
        this.generation = generation;
        this.reason = reason;
    }

    public DataFrameTransformTaskState(IndexerState state, @Nullable Map<String, Object> position, long generation) {
        this.state = DataFrameTransformState.fromIndexerState(state);
        this.currentPosition = position == null ? null : Collections.unmodifiableSortedMap(new TreeMap<>(position));
        this.generation = generation;
        this.reason = null;
    }

    public DataFrameTransformTaskState(StreamInput in) throws IOException {
        state = DataFrameTransformState.fromStream(in);
        currentPosition = in.readBoolean() ? Collections.unmodifiableSortedMap(new TreeMap<>(in.readMap())) : null;
        generation = in.readLong();
        reason = in.readOptionalString();
    }

    public DataFrameTransformState getState() {
        return state;
    }

    public IndexerState getIndexerState() {
        return state.toIndexerState();
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

    public static DataFrameTransformTaskState fromXContent(XContentParser parser) {
        try {
            return PARSER.parse(parser, null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(STATE.getPreferredName(), state.value());
        if (currentPosition != null) {
            builder.field(CURRENT_POSITION.getPreferredName(), currentPosition);
        }
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        builder.field(GENERATION.getPreferredName(), generation);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        state.writeTo(out);
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

        DataFrameTransformTaskState that = (DataFrameTransformTaskState) other;

        return Objects.equals(this.state, that.state) &&
            Objects.equals(this.currentPosition, that.currentPosition) &&
            this.generation == that.generation &&
            Objects.equals(this.reason, that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, currentPosition, generation, reason);
    }
}
