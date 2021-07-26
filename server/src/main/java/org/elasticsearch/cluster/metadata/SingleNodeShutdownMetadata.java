/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Contains data about a single node's shutdown readiness.
 */
public class SingleNodeShutdownMetadata extends AbstractDiffable<SingleNodeShutdownMetadata>
    implements
        ToXContentObject,
        Diffable<SingleNodeShutdownMetadata> {

    public static final ParseField NODE_ID_FIELD = new ParseField("node_id");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField REASON_FIELD = new ParseField("reason");
    public static final String STARTED_AT_READABLE_FIELD = "shutdown_started";
    public static final ParseField STARTED_AT_MILLIS_FIELD = new ParseField(STARTED_AT_READABLE_FIELD + "millis");
    public static final ParseField SHARD_REALLOCATION_DELAY = new ParseField("shard_reallocation_delay");

    public static final ConstructingObjectParser<SingleNodeShutdownMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "node_shutdown_info",
        a -> new SingleNodeShutdownMetadata(
            (String) a[0],
            Type.valueOf((String) a[1]),
            (String) a[2],
            (long) a[3],
            (TimeValue) a[4]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REASON_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), STARTED_AT_MILLIS_FIELD);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), SHARD_REALLOCATION_DELAY.getPreferredName()), SHARD_REALLOCATION_DELAY,
            ObjectParser.ValueType.STRING_OR_NULL
        );
    }

    public static SingleNodeShutdownMetadata parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static final TimeValue DEFAULT_RESTART_SHARD_ALLOCATION_DELAY = TimeValue.timeValueMinutes(10);

    private final String nodeId;
    private final Type type;
    private final String reason;
    private final long startedAtMillis;
    @Nullable private final TimeValue shardReallocationDelay;

    /**
     * @param nodeId The node ID that this shutdown metadata refers to.
     * @param type The type of shutdown. See {@link Type}.
     * @param reason The reason for the shutdown, per the original shutdown request.
     * @param startedAtMillis The timestamp at which this shutdown was requested.
     */
    private SingleNodeShutdownMetadata(
        String nodeId,
        Type type,
        String reason,
        long startedAtMillis,
        @Nullable TimeValue shardReallocationDelay
    ) {
        this.nodeId = Objects.requireNonNull(nodeId, "node ID must not be null");
        this.type = Objects.requireNonNull(type, "shutdown type must not be null");
        this.reason = Objects.requireNonNull(reason, "shutdown reason must not be null");
        this.startedAtMillis = startedAtMillis;
        if (shardReallocationDelay != null && Type.RESTART.equals(type) == false) {
            throw new IllegalArgumentException("shard allocation delay is only valid for RESTART-type shutdowns");
        }
        this.shardReallocationDelay = shardReallocationDelay;
    }

    public SingleNodeShutdownMetadata(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.type = in.readEnum(Type.class);
        this.reason = in.readString();
        this.startedAtMillis = in.readVLong();
        this.shardReallocationDelay = in.readOptionalTimeValue();
    }

    /**
     * @return The ID of the node this {@link SingleNodeShutdownMetadata} concerns.
     */
    public String getNodeId() {
        return nodeId;
    }

    /**
     * @return The type of shutdown this is (shutdown vs. permanent).
     */
    public Type getType() {
        return type;
    }

    /**
     * @return The user-supplied reason this node is shutting down.
     */
    public String getReason() {
        return reason;
    }

    /**
     * @return The timestamp that this shutdown procedure was started.
     */
    public long getStartedAtMillis() {
        return startedAtMillis;
    }

    /**
     * @return The amount of time shard reallocation should be delayed for shards on this node, so that they will not be automatically
     * reassigned while the node is restarting. Will be {@code null} for non-restart shutdowns.
     */
    @Nullable
    public TimeValue getShardReallocationDelay() {
        if (shardReallocationDelay != null) {
            return shardReallocationDelay;
        } else if (Type.RESTART.equals(type)) {
            return DEFAULT_RESTART_SHARD_ALLOCATION_DELAY;
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeEnum(type);
        out.writeString(reason);
        out.writeVLong(startedAtMillis);
        out.writeOptionalTimeValue(shardReallocationDelay);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(NODE_ID_FIELD.getPreferredName(), nodeId);
            builder.field(TYPE_FIELD.getPreferredName(), type);
            builder.field(REASON_FIELD.getPreferredName(), reason);
            builder.timeField(STARTED_AT_MILLIS_FIELD.getPreferredName(), STARTED_AT_READABLE_FIELD, startedAtMillis);
            if (shardReallocationDelay != null) {
                builder.field(SHARD_REALLOCATION_DELAY.getPreferredName(), shardReallocationDelay.getStringRep());
            }
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof SingleNodeShutdownMetadata) == false) return false;
        SingleNodeShutdownMetadata that = (SingleNodeShutdownMetadata) o;
        return getStartedAtMillis() == that.getStartedAtMillis()
            && getNodeId().equals(that.getNodeId())
            && getType() == that.getType()
            && getReason().equals(that.getReason())
            && Objects.equals(getShardReallocationDelay(), that.getShardReallocationDelay());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            getNodeId(),
            getType(),
            getReason(),
            getStartedAtMillis(),
            getShardReallocationDelay()
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(SingleNodeShutdownMetadata original) {
        if (original == null) {
            return builder();
        }
        return new Builder()
            .setNodeId(original.getNodeId())
            .setType(original.getType())
            .setReason(original.getReason())
            .setStartedAtMillis(original.getStartedAtMillis());
    }

    public static class Builder {
        private String nodeId;
        private Type type;
        private String reason;
        private long startedAtMillis = -1;
        private TimeValue shardReallocationDelay;

        private Builder() {}

        /**
         * @param nodeId The node ID this metadata refers to.
         * @return This builder.
         */
        public Builder setNodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        /**
         * @param type The type of shutdown.
         * @return This builder.
         */
        public Builder setType(Type type) {
            this.type = type;
            return this;
        }

        /**
         * @param reason The reason for the shutdown. An arbitrary string provided by the user.
         * @return This builder.
         */
        public Builder setReason(String reason) {
            this.reason = reason;
            return this;
        }

        /**
         * @param startedAtMillis The timestamp at which this shutdown was requested.
         * @return This builder.
         */
        public Builder setStartedAtMillis(long startedAtMillis) {
            this.startedAtMillis = startedAtMillis;
            return this;
        }

        public Builder setShardReallocationDelay(TimeValue shardReallocationDelay) {
            this.shardReallocationDelay = shardReallocationDelay;
            return this;
        }

        public SingleNodeShutdownMetadata build() {
            if (startedAtMillis == -1) {
                throw new IllegalArgumentException("start timestamp must be set");
            }

            return new SingleNodeShutdownMetadata(
                nodeId,
                type,
                reason,
                startedAtMillis,
                shardReallocationDelay
            );
        }
    }

    /**
     * Describes the type of node shutdown - permanent (REMOVE) or temporary (RESTART).
     */
    public enum Type {
        REMOVE,
        RESTART;

        public static Type parse(String type) {
            if ("remove".equals(type.toLowerCase(Locale.ROOT))) {
                return REMOVE;
            } else if ("restart".equals(type.toLowerCase(Locale.ROOT))) {
                return RESTART;
            } else {
                throw new IllegalArgumentException("unknown shutdown type: " + type);
            }
        }
    }

    /**
     * Describes the status of a component of shutdown.
     */
    public enum Status {
        // These are ordered (see #combine(...))
        NOT_STARTED,
        IN_PROGRESS,
        STALLED,
        COMPLETE;

        /**
         * Merges multiple statuses into a single, final, status
         *
         * For example, if called with NOT_STARTED, IN_PROGRESS, and STALLED, the returned state is STALLED.
         * Called with IN_PROGRESS, IN_PROGRESS, NOT_STARTED, the returned state is IN_PROGRESS.
         * Called with IN_PROGRESS, NOT_STARTED, COMPLETE, the returned state is IN_PROGRESS
         * Called with COMPLETE, COMPLETE, COMPLETE, the returned state is COMPLETE
         * Called with an empty array, the returned state is COMPLETE
         */
        public static Status combine(Status... statuses) {
            int statusOrd = -1;
            for (Status status : statuses) {
                // Max the status up to, but not including, "complete"
                if (status != COMPLETE) {
                    statusOrd = Math.max(status.ordinal(), statusOrd);
                }
            }
            if (statusOrd == -1) {
                // Either all the statuses were complete, or there were no statuses given
                return COMPLETE;
            } else {
                return Status.values()[statusOrd];
            }
        }
    }
}
