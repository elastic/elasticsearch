/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Used as a wrapper for the objects returned from the stats endpoint.
 * Objects of this class are expected to be ephemeral.
 * Do not persist objects of this class to cluster state or an index.
 */
public class TransformStats implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_stats";
    public static final ParseField HEALTH_FIELD = new ParseField("health");
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
    private final TransformIndexerStats indexerStats;
    private final TransformCheckpointingInfo checkpointingInfo;
    private final TransformHealth health;

    public static TransformStats initialStats(String id) {
        return new TransformStats(
            id,
            State.STOPPED,
            null,
            null,
            new TransformIndexerStats(),
            TransformCheckpointingInfo.EMPTY,
            TransformHealth.GREEN
        );
    }

    public TransformStats(
        String id,
        State state,
        @Nullable String reason,
        @Nullable NodeAttributes node,
        TransformIndexerStats stats,
        TransformCheckpointingInfo checkpointingInfo,
        TransformHealth health
    ) {
        this.id = Objects.requireNonNull(id);
        this.state = Objects.requireNonNull(state);
        this.reason = reason;
        this.node = node;
        this.indexerStats = Objects.requireNonNull(stats);
        this.checkpointingInfo = Objects.requireNonNull(checkpointingInfo);
        this.health = Objects.requireNonNull(health);
    }

    public TransformStats(StreamInput in) throws IOException {
        this.id = in.readString();
        this.state = in.readEnum(State.class);
        this.reason = in.readOptionalString();
        if (in.readBoolean()) {
            this.node = new NodeAttributes(in);
        } else {
            this.node = null;
        }
        this.indexerStats = new TransformIndexerStats(in);
        this.checkpointingInfo = new TransformCheckpointingInfo(in);

        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            if (in.readBoolean()) {
                this.health = new TransformHealth(in);
            } else {
                this.health = null;
            }
        } else {
            this.health = null;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TransformField.ID.getPreferredName(), id);
        builder.field(STATE_FIELD.getPreferredName(), state.value());
        if (reason != null) {
            builder.field(REASON_FIELD.getPreferredName(), reason);
        }
        if (node != null) {
            builder.field(NODE_FIELD.getPreferredName(), node);
        }
        builder.field(TransformField.STATS_FIELD.getPreferredName(), indexerStats, params);
        builder.field(CHECKPOINTING_INFO_FIELD.getPreferredName(), checkpointingInfo, params);
        if (health != null) {
            builder.field(HEALTH_FIELD.getPreferredName(), health);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            if (health != null) {
                out.writeBoolean(true);
                health.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, state, reason, node, indexerStats, checkpointingInfo, health);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TransformStats that = (TransformStats) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.state, that.state)
            && Objects.equals(this.reason, that.reason)
            && Objects.equals(this.node, that.node)
            && Objects.equals(this.indexerStats, that.indexerStats)
            && Objects.equals(this.checkpointingInfo, that.checkpointingInfo)
            && Objects.equals(this.health, that.health);
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

    public TransformIndexerStats getIndexerStats() {
        return indexerStats;
    }

    public TransformCheckpointingInfo getCheckpointingInfo() {
        return checkpointingInfo;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public enum State implements Writeable {

        STARTED,
        INDEXING,
        ABORTING,
        STOPPING,
        STOPPED,
        FAILED,
        WAITING;

        public static State fromString(String name) {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        }

        public static State fromStream(StreamInput in) throws IOException {
            return in.readEnum(State.class);
        }

        public static State fromComponents(TransformTaskState taskState, IndexerState indexerState) {

            if (taskState == null || taskState == TransformTaskState.STOPPED) {
                return STOPPED;
            } else if (taskState == TransformTaskState.FAILED) {
                return FAILED;
            } else {
                // If we get here then the task state must be started, and that means we should have an indexer state
                assert (taskState == TransformTaskState.STARTED);
                assert (indexerState != null);

                return switch (indexerState) {
                    case STARTED -> STARTED;
                    case INDEXING -> INDEXING;
                    case STOPPING -> STOPPING;
                    case STOPPED -> STOPPING;
                    case ABORTING -> ABORTING;
                };
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }

        // only used when speaking to nodes < 7.4 (can be removed for 8.0)
        public Tuple<TransformTaskState, IndexerState> toComponents() {

            return switch (this) {
                case STARTED -> new Tuple<>(TransformTaskState.STARTED, IndexerState.STARTED);
                case INDEXING -> new Tuple<>(TransformTaskState.STARTED, IndexerState.INDEXING);
                case ABORTING -> new Tuple<>(TransformTaskState.STARTED, IndexerState.ABORTING);
                case STOPPING ->
                    // This one is not deterministic, because an overall state of STOPPING could arise
                    // from either (STARTED, STOPPED) or (STARTED, STOPPING). However, (STARTED, STOPPED)
                    // is a very short-lived state so it's reasonable to assume the other, especially
                    // as this method is only for mixed version cluster compatibility.
                    new Tuple<>(TransformTaskState.STARTED, IndexerState.STOPPING);
                case STOPPED -> new Tuple<>(TransformTaskState.STOPPED, null);
                case FAILED -> new Tuple<>(TransformTaskState.FAILED, null);
                default -> throw new IllegalStateException("Unexpected state enum value: " + this);
            };
        }
    }
}
