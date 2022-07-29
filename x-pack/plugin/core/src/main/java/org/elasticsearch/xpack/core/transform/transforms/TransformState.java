/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformState implements Task.Status, PersistentTaskState {
    public static final String NAME = TransformField.TASK_NAME;

    private final TransformTaskState taskState;
    private final IndexerState indexerState;
    private final TransformProgress progress;
    private final long checkpoint;

    @Nullable
    private final TransformIndexerPosition position;
    @Nullable
    private final String reason;
    @Nullable
    private NodeAttributes node;

    private final boolean shouldStopAtNextCheckpoint;

    public static final ParseField TASK_STATE = new ParseField("task_state");
    public static final ParseField INDEXER_STATE = new ParseField("indexer_state");

    // 7.3 BWC: current_position only exists in 7.2. In 7.3+ it is replaced by position.
    public static final ParseField CURRENT_POSITION = new ParseField("current_position");
    public static final ParseField POSITION = new ParseField("position");
    public static final ParseField CHECKPOINT = new ParseField("checkpoint");
    public static final ParseField REASON = new ParseField("reason");
    public static final ParseField PROGRESS = new ParseField("progress");
    public static final ParseField NODE = new ParseField("node");
    public static final ParseField SHOULD_STOP_AT_NEXT_CHECKPOINT = new ParseField("should_stop_at_checkpoint");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<TransformState, Void> PARSER = new ConstructingObjectParser<>(NAME, true, args -> {
        TransformTaskState taskState = (TransformTaskState) args[0];
        IndexerState indexerState = (IndexerState) args[1];
        Map<String, Object> bwcCurrentPosition = (Map<String, Object>) args[2];
        TransformIndexerPosition transformIndexerPosition = (TransformIndexerPosition) args[3];

        // BWC handling, translate current_position to position iff position isn't set
        if (bwcCurrentPosition != null && transformIndexerPosition == null) {
            transformIndexerPosition = new TransformIndexerPosition(bwcCurrentPosition, null);
        }

        long checkpoint = (long) args[4];
        String reason = (String) args[5];
        TransformProgress progress = (TransformProgress) args[6];
        NodeAttributes node = (NodeAttributes) args[7];
        boolean shouldStopAtNextCheckpoint = args[8] == null ? false : (boolean) args[8];

        return new TransformState(
            taskState,
            indexerState,
            transformIndexerPosition,
            checkpoint,
            reason,
            progress,
            node,
            shouldStopAtNextCheckpoint
        );
    });

    static {
        PARSER.declareField(constructorArg(), p -> TransformTaskState.fromString(p.text()), TASK_STATE, ValueType.STRING);
        PARSER.declareField(constructorArg(), p -> IndexerState.fromString(p.text()), INDEXER_STATE, ValueType.STRING);
        PARSER.declareField(optionalConstructorArg(), XContentParser::mapOrdered, CURRENT_POSITION, ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(), TransformIndexerPosition::fromXContent, POSITION, ValueType.OBJECT);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), CHECKPOINT);
        PARSER.declareString(optionalConstructorArg(), REASON);
        PARSER.declareField(optionalConstructorArg(), TransformProgress.PARSER::apply, PROGRESS, ValueType.OBJECT);
        PARSER.declareField(optionalConstructorArg(), NodeAttributes.PARSER::apply, NODE, ValueType.OBJECT);
        PARSER.declareBoolean(optionalConstructorArg(), SHOULD_STOP_AT_NEXT_CHECKPOINT);
    }

    public TransformState(
        TransformTaskState taskState,
        IndexerState indexerState,
        @Nullable TransformIndexerPosition position,
        long checkpoint,
        @Nullable String reason,
        @Nullable TransformProgress progress,
        @Nullable NodeAttributes node,
        boolean shouldStopAtNextCheckpoint
    ) {
        this.taskState = taskState;
        this.indexerState = indexerState;
        this.position = position;
        this.checkpoint = checkpoint;
        this.reason = reason;
        this.progress = progress;
        this.node = node;
        this.shouldStopAtNextCheckpoint = shouldStopAtNextCheckpoint;
    }

    public TransformState(
        TransformTaskState taskState,
        IndexerState indexerState,
        @Nullable TransformIndexerPosition position,
        long checkpoint,
        @Nullable String reason,
        @Nullable TransformProgress progress,
        @Nullable NodeAttributes node
    ) {
        this(taskState, indexerState, position, checkpoint, reason, progress, node, false);
    }

    public TransformState(
        TransformTaskState taskState,
        IndexerState indexerState,
        @Nullable TransformIndexerPosition position,
        long checkpoint,
        @Nullable String reason,
        @Nullable TransformProgress progress
    ) {
        this(taskState, indexerState, position, checkpoint, reason, progress, null);
    }

    public TransformState(StreamInput in) throws IOException {
        taskState = TransformTaskState.fromStream(in);
        indexerState = IndexerState.fromStream(in);
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            position = in.readOptionalWriteable(TransformIndexerPosition::new);
        } else {
            Map<String, Object> pos = in.readMap();
            position = new TransformIndexerPosition(pos, null);
        }
        checkpoint = in.readLong();
        reason = in.readOptionalString();
        progress = in.readOptionalWriteable(TransformProgress::new);
        if (in.getVersion().onOrAfter(Version.V_7_3_0)) {
            node = in.readOptionalWriteable(NodeAttributes::new);
        } else {
            node = null;
        }
        if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
            shouldStopAtNextCheckpoint = in.readBoolean();
        } else {
            shouldStopAtNextCheckpoint = false;
        }
    }

    public TransformTaskState getTaskState() {
        return taskState;
    }

    public IndexerState getIndexerState() {
        return indexerState;
    }

    public TransformIndexerPosition getPosition() {
        return position;
    }

    public long getCheckpoint() {
        return checkpoint;
    }

    public TransformProgress getProgress() {
        return progress;
    }

    public String getReason() {
        return reason;
    }

    public NodeAttributes getNode() {
        return node;
    }

    public TransformState setNode(NodeAttributes node) {
        this.node = node;
        return this;
    }

    public boolean shouldStopAtNextCheckpoint() {
        return shouldStopAtNextCheckpoint;
    }

    public static TransformState fromXContent(XContentParser parser) {
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
        builder.field(SHOULD_STOP_AT_NEXT_CHECKPOINT.getPreferredName(), shouldStopAtNextCheckpoint);
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
            out.writeGenericMap(position != null ? position.getIndexerPosition() : null);
        }
        out.writeLong(checkpoint);
        out.writeOptionalString(reason);
        out.writeOptionalWriteable(progress);
        if (out.getVersion().onOrAfter(Version.V_7_3_0)) {
            out.writeOptionalWriteable(node);
        }
        if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
            out.writeBoolean(shouldStopAtNextCheckpoint);
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

        TransformState that = (TransformState) other;

        return Objects.equals(this.taskState, that.taskState)
            && Objects.equals(this.indexerState, that.indexerState)
            && Objects.equals(this.position, that.position)
            && this.checkpoint == that.checkpoint
            && Objects.equals(this.reason, that.reason)
            && Objects.equals(this.progress, that.progress)
            && Objects.equals(this.shouldStopAtNextCheckpoint, that.shouldStopAtNextCheckpoint)
            && Objects.equals(this.node, that.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(taskState, indexerState, position, checkpoint, reason, progress, node, shouldStopAtNextCheckpoint);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
