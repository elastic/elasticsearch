/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TransformStats {

    public static final ParseField ID = new ParseField("id");
    public static final ParseField STATE_FIELD = new ParseField("state");
    public static final ParseField REASON_FIELD = new ParseField("reason");
    public static final ParseField NODE_FIELD = new ParseField("node");
    public static final ParseField STATS_FIELD = new ParseField("stats");
    public static final ParseField CHECKPOINTING_INFO_FIELD = new ParseField("checkpointing");

    public static final ConstructingObjectParser<TransformStats, Void> PARSER = new ConstructingObjectParser<>(
        "data_frame_transform_state_and_stats_info",
        true,
        a -> new TransformStats(
            (String) a[0],
            (State) a[1],
            (String) a[2],
            (NodeAttributes) a[3],
            (TransformIndexerStats) a[4],
            (TransformCheckpointingInfo) a[5]
        )
    );

    static {
        PARSER.declareString(constructorArg(), ID);
        PARSER.declareField(optionalConstructorArg(), p -> State.fromString(p.text()), STATE_FIELD, ObjectParser.ValueType.STRING);
        PARSER.declareString(optionalConstructorArg(), REASON_FIELD);
        PARSER.declareField(optionalConstructorArg(), NodeAttributes.PARSER::apply, NODE_FIELD, ObjectParser.ValueType.OBJECT);
        PARSER.declareObject(constructorArg(), (p, c) -> TransformIndexerStats.fromXContent(p), STATS_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> TransformCheckpointingInfo.fromXContent(p), CHECKPOINTING_INFO_FIELD);
    }

    public static TransformStats fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String id;
    private final String reason;
    private final State state;
    private final NodeAttributes node;
    private final TransformIndexerStats indexerStats;
    private final TransformCheckpointingInfo checkpointingInfo;

    public TransformStats(
        String id,
        State state,
        String reason,
        NodeAttributes node,
        TransformIndexerStats stats,
        TransformCheckpointingInfo checkpointingInfo
    ) {
        this.id = id;
        this.state = state;
        this.reason = reason;
        this.node = node;
        this.indexerStats = stats;
        this.checkpointingInfo = checkpointingInfo;
    }

    public String getId() {
        return id;
    }

    public State getState() {
        return state;
    }

    public String getReason() {
        return reason;
    }

    public NodeAttributes getNode() {
        return node;
    }

    public TransformIndexerStats getIndexerStats() {
        return indexerStats;
    }

    public TransformCheckpointingInfo getCheckpointingInfo() {
        return checkpointingInfo;
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

        TransformStats that = (TransformStats) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.state, that.state)
            && Objects.equals(this.reason, that.reason)
            && Objects.equals(this.node, that.node)
            && Objects.equals(this.indexerStats, that.indexerStats)
            && Objects.equals(this.checkpointingInfo, that.checkpointingInfo);
    }

    public enum State {

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

        public String value() {
            return name().toLowerCase(Locale.ROOT);
        }
    }
}
