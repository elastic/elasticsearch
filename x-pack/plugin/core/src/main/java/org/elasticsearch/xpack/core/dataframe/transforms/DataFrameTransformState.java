/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.Version;
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
    private final DataFrameIndexerPosition position;
    @Nullable
    private final String reason;
    @Nullable
    private NodeAttributes node;

    public static final ParseField TASK_STATE = new ParseField("task_state");
    public static final ParseField INDEXER_STATE = new ParseField("indexer_state");

    // 7.3 BWC: current_position only exists in 7.2.  In 7.3+ it is replaced by position.
    public static final ParseField CURRENT_POSITION = new ParseField("current_position");
    public static final ParseField POSITION = new ParseField("position");
    public static final ParseField CHECKPOINT = new ParseField("checkpoint");
    public static final ParseField REASON = new ParseField("reason");
    public static final ParseField PROGRESS = new ParseField("progress");
    public static final ParseField NODE = new ParseField("node");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DataFrameTransformState, Void> PARSER = new ConstructingObjectParser<>(NAME,
            true,
            args -> {
                DataFrameTransformTaskState taskState = (DataFrameTransformTaskState) args[0];
                IndexerState indexerState = (IndexerState) args[1];
                Map<String, Object> bwcCurrentPosition = (Map<String, Object>) args[2];
                DataFrameIndexerPosition dataFrameIndexerPosition = (DataFrameIndexerPosition) args[3];

                // BWC handling, translate current_position to position iff position isn't set
                if (bwcCurrentPosition != null && dataFrameIndexerPosition == null) {
                    dataFrameIndexerPosition = new DataFrameIndexerPosition(bwcCurrentPosition, null);
                }

                long checkpoint = (long) args[4];
                String reason = (String) args[5];
                DataFrameTransformProgress progress = (DataFrameTransformProgress) args[6];
                NodeAttributes node = (NodeAttributes) args[7];

                return new DataFrameTransformState(taskState, indexerState, dataFrameIndexerPosition, checkpoint, reason, progress, node);
            });

    static {
        PARSER.declareField(constructorArg(), p -> DataFrameTransformTaskState.fromString(p.text()), TASK_STATE, ValueType.STRING);
        PARSER.declareField(constructorArg(), p -> IndexerState.fromString(p.text()), INDEXER_STATE, ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), XContentParser::mapOrdered, CURRENT_POSITION, ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(), DataFrameIndexerPosition::fromXContent, POSITION, ValueType.OBJECT);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), CHECKPOINT);
        PARSER.declareString(optionalConstructorArg(), REASON);
        PARSER.declareField(optionalConstructorArg(), DataFrameTransformProgress.PARSER::apply, PROGRESS, ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(), NodeAttributes.PARSER::apply, NODE, ValueType.OBJECT);
    }

    public DataFrameTransformState(DataFrameTransformTaskState taskState,
                                   IndexerState indexerState,
                                   @Nullable DataFrameIndexerPosition position,
                                   long checkpoint,
                                   @Nullable String reason,
                                   @Nullable DataFrameTransformProgress progress,
                                   @Nullable NodeAttributes node) {
        this.taskState = taskState;
        this.indexerState = indexerState;
        this.position = position;
        this.checkpoint = checkpoint;
        this.reason = reason;
        this.progress = progress;
        this.node = node;
    }

    public DataFrameTransformState(DataFrameTransformTaskState taskState,
                                   IndexerState indexerState,
                                   @Nullable DataFrameIndexerPosition position,
                                   long checkpoint,
                                   @Nullable String reason,
                                   @Nullable DataFrameTransformProgress progress) {
        this(taskState, indexerState, position, checkpoint, reason, progress, null);
    }

    public DataFrameTransformState(StreamInput in) throws IOException {
        taskState = DataFrameTransformTaskState.fromStream(in);
        indexerState = IndexerState.fromStream(in);
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            position = in.readOptionalWriteable(DataFrameIndexerPosition::new);
        } else {
            Map<String, Object> pos = in.readMap();
            position = new DataFrameIndexerPosition(pos, null);
        }
        checkpoint = in.readLong();
        reason = in.readOptionalString();
        progress = in.readOptionalWriteable(DataFrameTransformProgress::new);
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            node = in.readOptionalWriteable(NodeAttributes::new);
        } else {
            node = null;
        }
    }

    public DataFrameTransformTaskState getTaskState() {
        return taskState;
    }

    public IndexerState getIndexerState() {
        return indexerState;
    }

    public DataFrameIndexerPosition getPosition() {
        return position;
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    public DataFrameTransformProgress getProgress() {
        return progress;
    }

    public String getReason() {
        return reason;
    }

    public NodeAttributes getNode() {
        return node;
    }

    public DataFrameTransformState setNode(NodeAttributes node) {
        this.node = node;
        return this;
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
        if (position != null) {
            builder.field(POSITION.getPreferredName(), position);
        }
        builder.field(CHECKPOINT.getPreferredName(), checkpoint);
        if (reason != null) {
            builder.field(REASON.getPreferredName(), reason);
        }
        if (progress != null) {
            builder.field(PROGRESS.getPreferredName(), progress);
        }
        if (node != null) {
            builder.field(NODE.getPreferredName(), node);
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
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeOptionalWriteable(position);
        } else {
            out.writeMap(position != null ? position.getIndexerPosition() : null);
        }
        out.writeLong(checkpoint);
        out.writeOptionalString(reason);
        out.writeOptionalWriteable(progress);
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeOptionalWriteable(node);
        }
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
            Objects.equals(this.position, that.position) &&
            this.checkpoint == that.checkpoint &&
            Objects.equals(this.reason, that.reason) &&
            Objects.equals(this.progress, that.progress) &&
            Objects.equals(this.node, that.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskState, indexerState, position, checkpoint, reason, progress, node);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
