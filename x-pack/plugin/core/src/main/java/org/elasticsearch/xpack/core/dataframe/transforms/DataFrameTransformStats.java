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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Used as a wrapper for the objects returned from the stats endpoint.
 * Objects of this class are expected to be ephemeral.
 * Do not persist objects of this class to cluster state or an index.
 */
public class DataFrameTransformStats implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_stats";
    public static final ParseField STATE_FIELD = new ParseField("state");
    public static final ParseField REASON_FIELD = new ParseField("reason");
    public static final ParseField NODE_FIELD = new ParseField("node");
    public static final ParseField CHECKPOINTING_INFO_FIELD = new ParseField("checkpointing");

    private final String id;
    private final State state;
    @Nullable
    private final String reason;
    @Nullable
    private NodeAttributes node;
    private final DataFrameIndexerTransformStats indexerStats;
    private final DataFrameTransformCheckpointingInfo checkpointingInfo;

    public static final ConstructingObjectParser<DataFrameTransformStats, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new DataFrameTransformStats((String) a[0],
            (State) a[1],
            (String) a[2],
            (NodeAttributes) a[3],
            (DataFrameIndexerTransformStats) a[4],
            (DataFrameTransformCheckpointingInfo) a[5]));

    static {
        PARSER.declareString(constructorArg(), DataFrameField.ID);
        PARSER.declareField(constructorArg(), p -> DataFrameTransformStats.State.fromString(p.text()), STATE_FIELD,
            ObjectParser.ValueType.STRING);
        PARSER.declareString(optionalConstructorArg(), REASON_FIELD);
        PARSER.declareField(optionalConstructorArg(), NodeAttributes.PARSER::apply, NODE_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareObject(constructorArg(), (p, c) -> DataFrameIndexerTransformStats.fromXContent(p),
            DataFrameField.STATS_FIELD);
        PARSER.declareObject(constructorArg(),
            (p, c) -> DataFrameTransformCheckpointingInfo.fromXContent(p), CHECKPOINTING_INFO_FIELD);
    }

    public static DataFrameTransformStats fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static DataFrameTransformStats initialStats(String id) {
        return stoppedStats(id, new DataFrameIndexerTransformStats());
    }

    public static DataFrameTransformStats stoppedStats(String id, DataFrameIndexerTransformStats indexerTransformStats) {
        return new DataFrameTransformStats(id,
            State.STOPPED,
            null,
            null,
            indexerTransformStats,
            DataFrameTransformCheckpointingInfo.EMPTY);
    }


    public DataFrameTransformStats(String id, State state, @Nullable String reason,
                                   @Nullable NodeAttributes node, DataFrameIndexerTransformStats stats,
                                   DataFrameTransformCheckpointingInfo checkpointingInfo) {
        this.id = Objects.requireNonNull(id);
        this.state = Objects.requireNonNull(state);
        this.reason = reason;
        this.node = node;
        this.indexerStats = Objects.requireNonNull(stats);
        this.checkpointingInfo = Objects.requireNonNull(checkpointingInfo);
    }

    public DataFrameTransformStats(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
            this.id = in.readString();
            this.state = in.readEnum(State.class);
            this.reason = in.readOptionalString();
            if (in.readBoolean()) {
                this.node = new NodeAttributes(in);
            } else {
                this.node = null;
            }
            this.indexerStats = new DataFrameIndexerTransformStats(in);
            this.checkpointingInfo = new DataFrameTransformCheckpointingInfo(in);

        } else {
            // Prior to version 7.4 DataFrameTransformStats didn't exist, and we have
            // to do the best we can of reading from a DataFrameTransformStoredDoc object
            // (which is called DataFrameTransformStateAndStats in 7.2/7.3)
            this.id = in.readString();
            DataFrameTransformState transformState = new DataFrameTransformState(in);
            this.state = State.fromComponents(transformState.getTaskState(), transformState.getIndexerState());
            this.reason = transformState.getReason();
            this.node = null;
            this.indexerStats = new DataFrameIndexerTransformStats(in);
            this.checkpointingInfo = new DataFrameTransformCheckpointingInfo(in);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DataFrameField.ID.getPreferredName(), id);
        builder.field(STATE_FIELD.getPreferredName(), state.value());
        if (reason != null) {
            builder.field(REASON_FIELD.getPreferredName(), reason);
        }
        if (node != null) {
            builder.field(NODE_FIELD.getPreferredName(), node);
        }
        builder.field(DataFrameField.STATS_FIELD.getPreferredName(), indexerStats, params);
        builder.field(CHECKPOINTING_INFO_FIELD.getPreferredName(), checkpointingInfo, params);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            out.writeString(id);
            out.writeEnum(state);
            out.writeOptionalString(reason);
            if (node != null) {
                out.writeBoolean(true);
                node.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
            indexerStats.writeTo(out);
            checkpointingInfo.writeTo(out);
        } else {
            // Prior to version 7.4 DataFrameTransformStats didn't exist, and we have
            // to do the best we can of writing to a DataFrameTransformStoredDoc object
            // (which is called DataFrameTransformStateAndStats in 7.2/7.3)
            out.writeString(id);
            Tuple<DataFrameTransformTaskState, IndexerState> stateComponents = state.toComponents();
            new DataFrameTransformState(stateComponents.v1(),
                stateComponents.v2(),
                checkpointingInfo.getNext().getPosition(),
                checkpointingInfo.getLast().getCheckpoint(),
                reason,
                checkpointingInfo.getNext().getCheckpointProgress()).writeTo(out);
            out.writeBoolean(false);
            indexerStats.writeTo(out);
            checkpointingInfo.writeTo(out);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, state, reason, node, indexerStats, checkpointingInfo);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        DataFrameTransformStats that = (DataFrameTransformStats) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.state, that.state)
            && Objects.equals(this.reason, that.reason)
            && Objects.equals(this.node, that.node)
            && Objects.equals(this.indexerStats, that.indexerStats)
            && Objects.equals(this.checkpointingInfo, that.checkpointingInfo);
    }

    public String getId() {
        return id;
    }

    public State getState() {
        return state;
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

    public DataFrameIndexerTransformStats getIndexerStats() {
        return indexerStats;
    }

    public DataFrameTransformCheckpointingInfo getCheckpointingInfo() {
        return checkpointingInfo;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public enum State implements Writeable {

        STARTED, INDEXING, ABORTING, STOPPING, STOPPED, FAILED;

        public static State fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }

        public static State fromStream(StreamInput in) throws IOException {
            return in.readEnum(State.class);
        }

        public static State fromComponents(DataFrameTransformTaskState taskState, IndexerState indexerState) {

            if (taskState == null || taskState == DataFrameTransformTaskState.STOPPED) {
                return STOPPED;
            } else if (taskState == DataFrameTransformTaskState.FAILED) {
                return FAILED;
            } else {

                // If we get here then the task state must be started, and that means we should have an indexer state
                assert(taskState == DataFrameTransformTaskState.STARTED);
                assert(indexerState != null);

                switch (indexerState) {
                    case STARTED:
                        return STARTED;
                    case INDEXING:
                        return INDEXING;
                    case STOPPING:
                        return STOPPING;
                    case STOPPED:
                        return STOPPED;
                    case ABORTING:
                        return ABORTING;
                    default:
                        throw new IllegalStateException("Unexpected indexer state enum value: " + indexerState);
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }

        public Tuple<DataFrameTransformTaskState, IndexerState> toComponents() {

            switch (this) {
                case STARTED:
                    return new Tuple<>(DataFrameTransformTaskState.STARTED, IndexerState.STARTED);
                case INDEXING:
                    return new Tuple<>(DataFrameTransformTaskState.STARTED, IndexerState.INDEXING);
                case ABORTING:
                    return new Tuple<>(DataFrameTransformTaskState.STARTED, IndexerState.ABORTING);
                case STOPPING:
                    return new Tuple<>(DataFrameTransformTaskState.STARTED, IndexerState.STOPPING);
                case STOPPED:
                    // This one is not deterministic, because an overall state of STOPPED could arise
                    // from either (STOPPED, null) or (STARTED, STOPPED).  However, (STARTED, STOPPED)
                    // is a very short-lived state so it's reasonable to assume the other, especially
                    // as this method is only for mixed version cluster compatibility.
                    return new Tuple<>(DataFrameTransformTaskState.STOPPED, null);
                case FAILED:
                    return new Tuple<>(DataFrameTransformTaskState.FAILED, null);
                default:
                    throw new IllegalStateException("Unexpected state enum value: " + this);
            }
        }
    }
}
