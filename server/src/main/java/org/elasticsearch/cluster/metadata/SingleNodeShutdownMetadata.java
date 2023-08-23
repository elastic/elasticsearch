/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

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

    public static final Version REPLACE_SHUTDOWN_TYPE_ADDED_VERSION = Version.V_7_16_0;

    public static final ParseField NODE_ID_FIELD = new ParseField("node_id");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField REASON_FIELD = new ParseField("reason");
    public static final String STARTED_AT_READABLE_FIELD = "shutdown_started";
    public static final ParseField STARTED_AT_MILLIS_FIELD = new ParseField(STARTED_AT_READABLE_FIELD + "millis");
    public static final ParseField ALLOCATION_DELAY_FIELD = new ParseField("allocation_delay");
    public static final ParseField NODE_SEEN_FIELD = new ParseField("node_seen");
    public static final ParseField TARGET_NODE_NAME_FIELD = new ParseField("target_node_name");

    public static final ConstructingObjectParser<SingleNodeShutdownMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "node_shutdown_info",
        a -> new SingleNodeShutdownMetadata(
            (String) a[0],
            Type.valueOf((String) a[1]),
            (String) a[2],
            (long) a[3],
            (boolean) a[4],
            (TimeValue) a[5],
            (String) a[6]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REASON_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), STARTED_AT_MILLIS_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), NODE_SEEN_FIELD);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), ALLOCATION_DELAY_FIELD.getPreferredName()),
            ALLOCATION_DELAY_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TARGET_NODE_NAME_FIELD);
    }

    public static SingleNodeShutdownMetadata parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static final TimeValue DEFAULT_RESTART_SHARD_ALLOCATION_DELAY = TimeValue.timeValueMinutes(5);

    private final String nodeId;
    private final Type type;
    private final String reason;
    private final long startedAtMillis;
    private final boolean nodeSeen;
    @Nullable
    private final TimeValue allocationDelay;
    @Nullable
    private final String targetNodeName;

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
        boolean nodeSeen,
        @Nullable TimeValue allocationDelay,
        @Nullable String targetNodeName
    ) {
        this.nodeId = Objects.requireNonNull(nodeId, "node ID must not be null");
        this.type = Objects.requireNonNull(type, "shutdown type must not be null");
        this.reason = Objects.requireNonNull(reason, "shutdown reason must not be null");
        this.startedAtMillis = startedAtMillis;
        this.nodeSeen = nodeSeen;
        if (allocationDelay != null && Type.RESTART.equals(type) == false) {
            throw new IllegalArgumentException("shard allocation delay is only valid for RESTART-type shutdowns");
        }
        this.allocationDelay = allocationDelay;
        if (targetNodeName != null && type != Type.REPLACE) {
            throw new IllegalArgumentException(
                new ParameterizedMessage(
                    "target node name is only valid for REPLACE type shutdowns, " + "but was given type [{}] and target node name [{}]",
                    type,
                    targetNodeName
                ).getFormattedMessage()
            );
        } else if (Strings.hasText(targetNodeName) == false && type == Type.REPLACE) {
            throw new IllegalArgumentException("target node name is required for REPLACE type shutdowns");
        }
        this.targetNodeName = targetNodeName;
    }

    public SingleNodeShutdownMetadata(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.type = in.readEnum(Type.class);
        this.reason = in.readString();
        this.startedAtMillis = in.readVLong();
        this.nodeSeen = in.readBoolean();
        this.allocationDelay = in.readOptionalTimeValue();
        if (in.getVersion().onOrAfter(REPLACE_SHUTDOWN_TYPE_ADDED_VERSION)) {
            this.targetNodeName = in.readOptionalString();
        } else {
            this.targetNodeName = null;
        }
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
     * @return A boolean indicated whether this node has been seen in the cluster since the shutdown was registered.
     */
    public boolean getNodeSeen() {
        return nodeSeen;
    }

    /**
     * @return The name of the node to be used as a replacement for this node, or null.
     */
    public String getTargetNodeName() {
        return targetNodeName;
    }

    /**
     * @return The amount of time shard reallocation should be delayed for shards on this node, so that they will not be automatically
     * reassigned while the node is restarting. Will be {@code null} for non-restart shutdowns.
     */
    @Nullable
    public TimeValue getAllocationDelay() {
        if (allocationDelay != null) {
            return allocationDelay;
        } else if (Type.RESTART.equals(type)) {
            return DEFAULT_RESTART_SHARD_ALLOCATION_DELAY;
        }
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        if (out.getVersion().before(REPLACE_SHUTDOWN_TYPE_ADDED_VERSION) && this.type == SingleNodeShutdownMetadata.Type.REPLACE) {
            out.writeEnum(SingleNodeShutdownMetadata.Type.REMOVE);
        } else {
            out.writeEnum(type);
        }
        out.writeString(reason);
        out.writeVLong(startedAtMillis);
        out.writeBoolean(nodeSeen);
        out.writeOptionalTimeValue(allocationDelay);
        if (out.getVersion().onOrAfter(REPLACE_SHUTDOWN_TYPE_ADDED_VERSION)) {
            out.writeOptionalString(targetNodeName);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(NODE_ID_FIELD.getPreferredName(), nodeId);
            builder.field(TYPE_FIELD.getPreferredName(), type);
            builder.field(REASON_FIELD.getPreferredName(), reason);
            builder.timeField(STARTED_AT_MILLIS_FIELD.getPreferredName(), STARTED_AT_READABLE_FIELD, startedAtMillis);
            builder.field(NODE_SEEN_FIELD.getPreferredName(), nodeSeen);
            if (allocationDelay != null) {
                builder.field(ALLOCATION_DELAY_FIELD.getPreferredName(), allocationDelay.getStringRep());
            }
            if (targetNodeName != null) {
                builder.field(TARGET_NODE_NAME_FIELD.getPreferredName(), targetNodeName);
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
            && getNodeSeen() == that.getNodeSeen()
            && Objects.equals(allocationDelay, that.allocationDelay)
            && Objects.equals(targetNodeName, that.targetNodeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeId(), getType(), getReason(), getStartedAtMillis(), getNodeSeen(), allocationDelay, targetNodeName);
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{")
            .append("nodeId=[")
            .append(nodeId)
            .append(']')
            .append(", type=[")
            .append(type)
            .append("], reason=[")
            .append(reason)
            .append(']');
        if (allocationDelay != null) {
            stringBuilder.append(", allocationDelay=[").append(allocationDelay).append("]");
        }
        if (targetNodeName != null) {
            stringBuilder.append(", targetNodeName=[").append(targetNodeName).append("]");
        }
        stringBuilder.append("}");
        return stringBuilder.toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(SingleNodeShutdownMetadata original) {
        if (original == null) {
            return builder();
        }
        return new Builder().setNodeId(original.getNodeId())
            .setType(original.getType())
            .setReason(original.getReason())
            .setStartedAtMillis(original.getStartedAtMillis())
            .setNodeSeen(original.getNodeSeen())
            .setTargetNodeName(original.getTargetNodeName());
    }

    public static class Builder {
        private String nodeId;
        private Type type;
        private String reason;
        private long startedAtMillis = -1;
        private boolean nodeSeen = false;
        private TimeValue allocationDelay;
        private String targetNodeName;

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

        /**
         * @param nodeSeen Whether or not the node has been seen since the shutdown was registered.
         * @return This builder.
         */
        public Builder setNodeSeen(boolean nodeSeen) {
            this.nodeSeen = nodeSeen;
            return this;
        }

        /**
         * @param allocationDelay The amount of time shard reallocation should be delayed while this node is offline.
         * @return This builder.
         */
        public Builder setAllocationDelay(TimeValue allocationDelay) {
            this.allocationDelay = allocationDelay;
            return this;
        }

        /**
         * @param targetNodeName The name of the node which should be used to replcae this one. Only valid if the shutdown type is REPLACE.
         * @return This builder.
         */
        public Builder setTargetNodeName(String targetNodeName) {
            this.targetNodeName = targetNodeName;
            return this;
        }

        public SingleNodeShutdownMetadata build() {
            if (startedAtMillis == -1) {
                throw new IllegalArgumentException("start timestamp must be set");
            }

            return new SingleNodeShutdownMetadata(nodeId, type, reason, startedAtMillis, nodeSeen, allocationDelay, targetNodeName);
        }
    }

    /**
     * Describes the type of node shutdown - permanent (REMOVE) or temporary (RESTART).
     */
    public enum Type {
        REMOVE,
        RESTART,
        REPLACE;

        public static Type parse(String type) {
            if ("remove".equals(type.toLowerCase(Locale.ROOT))) {
                return REMOVE;
            } else if ("restart".equals(type.toLowerCase(Locale.ROOT))) {
                return RESTART;
            } else if ("replace".equals(type.toLowerCase(Locale.ROOT))) {
                return REPLACE;
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
