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
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformState implements Task.Status, PersistentTaskState {
    public static final String NAME = DataFrameField.TASK_NAME;

    private final DataFrameTransformTaskState taskState;
    private final IndexerState indexerState;
    private final DataFrameTransformProgress progress;
    private final long checkpoint;

    @Nullable
    private final Map<String, Object> currentPosition;
    @Nullable
    private final String reason;

    private static final ParseField TASK_STATE = new ParseField("task_state");
    private static final ParseField INDEXER_STATE = new ParseField("indexer_state");
    private static final ParseField CURRENT_POSITION = new ParseField("current_position");
    private static final ParseField CHECKPOINT = new ParseField("checkpoint");
    private static final ParseField REASON = new ParseField("reason");
    private static final ParseField PROGRESS = new ParseField("progress");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataFrameTransformState, Void> PARSER = new ConstructingObjectParser<>(NAME,
            true,
            args -> new DataFrameTransformState((DataFrameTransformTaskState) args[0],
                (IndexerState) args[1],
                (Map<String, Object>) args[2],
                (long) args[3],
                (String) args[4],
                (DataFrameTransformProgress) args[5]));

    static {
        PARSER.declareField(constructorArg(), p -> DataFrameTransformTaskState.fromString(p.text()), TASK_STATE, ValueType.STRING);
        PARSER.declareField(constructorArg(), p -> IndexerState.fromString(p.text()), INDEXER_STATE, ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), XContentParser::mapOrdered, CURRENT_POSITION, ValueType.OBJECT);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), CHECKPOINT);
        PARSER.declareString(optionalConstructorArg(), REASON);
        PARSER.declareField(optionalConstructorArg(), DataFrameTransformProgress.PARSER::apply, PROGRESS, ValueType.OBJECT);
    }

    public DataFrameTransformState(DataFrameTransformTaskState taskState,
                                   IndexerState indexerState,
                                   @Nullable Map<String, Object> position,
                                   long checkpoint,
                                   @Nullable String reason,
                                   @Nullable DataFrameTransformProgress progress) {
        this.taskState = taskState;
        this.indexerState = indexerState;
        this.currentPosition = position == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(position));
        this.checkpoint = checkpoint;
        this.reason = reason;
        this.progress = progress;
    }

    public DataFrameTransformState(StreamInput in) throws IOException {
        taskState = DataFrameTransformTaskState.fromStream(in);
        indexerState = IndexerState.fromStream(in);
        Map<String, Object> position = in.readMap();
        currentPosition = position == null ? null : Collections.unmodifiableMap(position);
        checkpoint = in.readLong();
        reason = in.readOptionalString();
        progress = in.readOptionalWriteable(DataFrameTransformProgress::new);
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

    public long getCheckpoint() {
        return checkpoint;
    }

    public DataFrameTransformProgress getProgress() {
        return progress;
    }

    /**
     * Get the in-progress checkpoint
     *
     * @return checkpoint in progress or 0 if task/indexer is not active
     */
    public long getInProgressCheckpoint() {
        return indexerState.equals(IndexerState.INDEXING) ? checkpoint + 1L : 0;
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
        builder.field(CHECKPOINT.getPreferredName(), checkpoint);
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        if (progress != null) {
            builder.field(PROGRESS.getPreferredName(), progress);
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
        out.writeMap(currentPosition);
        out.writeLong(checkpoint);
        out.writeOptionalString(reason);
        out.writeOptionalWriteable(progress);
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
            this.checkpoint == that.checkpoint &&
            Objects.equals(this.reason, that.reason) &&
            Objects.equals(this.progress, that.progress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskState, indexerState, currentPosition, checkpoint, reason, progress);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
