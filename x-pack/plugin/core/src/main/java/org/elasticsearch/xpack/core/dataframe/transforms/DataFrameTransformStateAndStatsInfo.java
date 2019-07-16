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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Used as a wrapper for the objects returned from the stats endpoint.
 * Objects of this class are expected to be ephermeral.
 * Do not persist objects of this class to cluster state or an index.
 */
public class DataFrameTransformStateAndStatsInfo implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_state_and_stats_info";
    public static final ParseField TASK_STATE_FIELD = new ParseField("task_state");
    public static final ParseField REASON_FIELD = new ParseField("reason");
    public static final ParseField NODE_FIELD = new ParseField("node");
    public static final ParseField CHECKPOINTING_INFO_FIELD = new ParseField("checkpointing");

    private final String id;
    private final DataFrameTransformTaskState taskState;
    @Nullable
    private String reason;
    @Nullable
    private NodeAttributes node;
    private final DataFrameIndexerTransformStats transformStats;
    private final DataFrameTransformCheckpointingInfo checkpointingInfo;

    public static final ConstructingObjectParser<DataFrameTransformStateAndStatsInfo, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new DataFrameTransformStateAndStatsInfo((String) a[0],
            (DataFrameTransformTaskState) a[1],
            (String) a[2],
            (NodeAttributes) a[3],
            (DataFrameIndexerTransformStats) a[4],
            (DataFrameTransformCheckpointingInfo) a[5]));

    static {
        PARSER.declareString(constructorArg(), DataFrameField.ID);
        PARSER.declareField(constructorArg(), p -> DataFrameTransformTaskState.fromString(p.text()), TASK_STATE_FIELD,
            ObjectParser.ValueType.STRING);
        PARSER.declareString(optionalConstructorArg(), REASON_FIELD);
        PARSER.declareField(optionalConstructorArg(), NodeAttributes.PARSER::apply, NODE_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareObject(constructorArg(), (p, c) -> DataFrameIndexerTransformStats.fromXContent(p),
            DataFrameField.STATS_FIELD);
        PARSER.declareObject(constructorArg(),
            (p, c) -> DataFrameTransformCheckpointingInfo.fromXContent(p), CHECKPOINTING_INFO_FIELD);
    }

    public static DataFrameTransformStateAndStatsInfo fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static DataFrameTransformStateAndStatsInfo initialStateAndStatsInfo(String id) {
        return stoppedStateAndStatsInfo(id, new DataFrameIndexerTransformStats(id));
    }

    public static DataFrameTransformStateAndStatsInfo stoppedStateAndStatsInfo(String id,
                                                                               DataFrameIndexerTransformStats indexerTransformStats) {
        return new DataFrameTransformStateAndStatsInfo(id,
            DataFrameTransformTaskState.STOPPED,
            null,
            null,
            indexerTransformStats,
            DataFrameTransformCheckpointingInfo.EMPTY);
    }


    public DataFrameTransformStateAndStatsInfo(String id, DataFrameTransformTaskState taskState, @Nullable String reason,
                                               @Nullable NodeAttributes node, DataFrameIndexerTransformStats stats,
                                               DataFrameTransformCheckpointingInfo checkpointingInfo) {
        this.id = Objects.requireNonNull(id);
        this.taskState = Objects.requireNonNull(taskState);
        this.reason = reason;
        this.node = node;
        this.transformStats = Objects.requireNonNull(stats);
        this.checkpointingInfo = Objects.requireNonNull(checkpointingInfo);
    }

    public DataFrameTransformStateAndStatsInfo(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) { // TODO change to V_7_4_0 after backport
            this.id = in.readString();
            this.taskState = in.readEnum(DataFrameTransformTaskState.class);
            if (in.readBoolean()) {
                this.node = new NodeAttributes(in);
            } else {
                this.node = null;
            }
            this.transformStats = new DataFrameIndexerTransformStats(in);
            this.checkpointingInfo = new DataFrameTransformCheckpointingInfo(in);

        } else {
            // Prior to version 7.4 DataFrameTransformStateAndStatsInfo didn't exist, and we have
            // to do the best we can of reading from a DataFrameTransformStateAndStats object
            this.id = in.readString();
            this.taskState = new DataFrameTransformState(in).getTaskState();
            this.node = null;
            this.transformStats = new DataFrameIndexerTransformStats(in);
            this.checkpointingInfo = new DataFrameTransformCheckpointingInfo(in);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), id);
        builder.field(TASK_STATE_FIELD.getPreferredName(), taskState);
        if (node != null) {
            builder.field(NODE_FIELD.getPreferredName(), node);
        }
        builder.field(DataFrameField.STATS_FIELD.getPreferredName(), transformStats, params);
        builder.field(CHECKPOINTING_INFO_FIELD.getPreferredName(), checkpointingInfo, params);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) { // TODO change to V_7_4_0 after backport
            out.writeString(id);
            out.writeEnum(taskState);
            if (node != null) {
                out.writeBoolean(true);
                node.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            transformStats.writeTo(out);
            checkpointingInfo.writeTo(out);
        } else {
            // Prior to version 7.4 DataFrameTransformStateAndStatsInfo didn't exist, and we have
            // to do the best we can of writing to a DataFrameTransformStateAndStats object
            out.writeString(id);
            new DataFrameTransformState(taskState,
                checkpointingInfo.getNext().getIndexerState(),
                checkpointingInfo.getNext().getPosition(),
                checkpointingInfo.getLast().getCheckpoint(),
                reason,
                checkpointingInfo.getNext().getCheckpointProgress()).writeTo(out);
            out.writeBoolean(false);
            transformStats.writeTo(out);
            checkpointingInfo.writeTo(out);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, taskState, node, transformStats, checkpointingInfo);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformStateAndStatsInfo that = (DataFrameTransformStateAndStatsInfo) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.taskState, that.taskState)
            && Objects.equals(this.node, that.node)
            && Objects.equals(this.transformStats, that.transformStats)
            && Objects.equals(this.checkpointingInfo, that.checkpointingInfo);
    }

    public String getId() {
        return id;
    }

    public DataFrameTransformTaskState getTaskState() {
        return taskState;
    }

    @Nullable
    public String getReason() {
        return reason;
    }

    @Nullable
    public NodeAttributes getNode() {
        return node;
    }

    public void setNode(NodeAttributes node) {
        this.node = node;
    }

    public DataFrameIndexerTransformStats getTransformStats() {
        return transformStats;
    }

    public DataFrameTransformCheckpointingInfo getCheckpointingInfo() {
        return checkpointingInfo;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
