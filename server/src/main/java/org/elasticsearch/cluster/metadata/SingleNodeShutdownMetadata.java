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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Contains data about a single node's shutdown readiness.
 */
public class SingleNodeShutdownMetadata extends AbstractDiffable<SingleNodeShutdownMetadata>
    implements
        ToXContentObject,
        Diffable<SingleNodeShutdownMetadata> {

    private static final ParseField NODE_ID_FIELD = new ParseField("node_id");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField REASON_FIELD = new ParseField("reason");
    private static final ParseField STATUS_FIELD = new ParseField("status");
    private static final String STARTED_AT_READABLE_FIELD = "shutdown_started";
    private static final ParseField STARTED_AT_MILLIS_FIELD = new ParseField(STARTED_AT_READABLE_FIELD + "millis");
    private static final ParseField SHARD_MIGRATION_FIELD = new ParseField("shard_migration");

    public static final ConstructingObjectParser<SingleNodeShutdownMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "node_shutdown_info",
        a -> new SingleNodeShutdownMetadata(
            (String) a[0],
            Type.valueOf((String) a[1]),
            (String) a[2],
            Status.valueOf((String) a[3]),
            (long) a[4],
            (ComponentShutdownStatus) a[5]
        )
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REASON_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), STATUS_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), STARTED_AT_MILLIS_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (parser, context) -> ComponentShutdownStatus.parse(parser),
            SHARD_MIGRATION_FIELD
        );
    }

    public static SingleNodeShutdownMetadata parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String nodeId;
    private final Type type;
    private final String reason;
    private final Status status;
    private final long startedAtMillis;
    private final ComponentShutdownStatus shardMigrationStatus;


    public SingleNodeShutdownMetadata(
        String nodeId, Type type, String reason, Status status, long startedAtMillis, ComponentShutdownStatus shardMigrationStatus) {
        this.nodeId = Objects.requireNonNull(nodeId, "node ID must not be null");
        this.type = Objects.requireNonNull(type, "shutdown type must not be null");
        this.reason = Objects.requireNonNull(reason, "shutdown reason must not be null");
        this.status = status;
        this.startedAtMillis = startedAtMillis;
        this.shardMigrationStatus = Objects.requireNonNull(shardMigrationStatus, "shard migration status must not be null");
    }

    public SingleNodeShutdownMetadata(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.type = in.readEnum(Type.class);
        this.reason = in.readString();
        this.status = in.readEnum(Status.class);
        this.startedAtMillis = in.readVLong();
        this.shardMigrationStatus = new ComponentShutdownStatus(in);
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
     * @return The status of this node's shutdown.
     */
    public Status isStatus() {
        return status;
    }

    /**
     * @return The timestamp that this shutdown procedure was started.
     */
    public long getStartedAtMillis() {
        return startedAtMillis;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeEnum(type);
        out.writeString(reason);
        out.writeEnum(status);
        out.writeVLong(startedAtMillis);
        shardMigrationStatus.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(NODE_ID_FIELD.getPreferredName(), nodeId);
            builder.field(TYPE_FIELD.getPreferredName(), type);
            builder.field(REASON_FIELD.getPreferredName(), reason);
            builder.field(STATUS_FIELD.getPreferredName(), status);
            builder.timeField(STARTED_AT_MILLIS_FIELD.getPreferredName(), STARTED_AT_READABLE_FIELD, startedAtMillis);
            builder.field(SHARD_MIGRATION_FIELD.getPreferredName(), shardMigrationStatus);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof SingleNodeShutdownMetadata) == false) return false;
        SingleNodeShutdownMetadata that = (SingleNodeShutdownMetadata) o;
        return isStatus() == that.isStatus()
            && getStartedAtMillis() == that.getStartedAtMillis()
            && getNodeId().equals(that.getNodeId())
            && getType().equals(that.getType())
            && getReason().equals(that.getReason());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeId(), getType(), getReason(), isStatus(), getStartedAtMillis());
    }

    public enum Type {
        REMOVE,
        RESTART
    }

    public enum Status {
        NOT_STARTED,
        IN_PROGRESS,
        STALLED,
        COMPLETE
    }

    public static class ComponentShutdownStatus extends AbstractDiffable<ComponentShutdownStatus> implements ToXContentFragment {
        private final Status status;
        @Nullable private final Long startedAtMillis;
        @Nullable private final String errorMessage;

        private static final ParseField STATUS_FIELD = new ParseField("status");

        private static final ParseField TIME_STARTED_FIELD = new ParseField("time_started_millis");
        private static final ParseField ERROR_FIELD = new ParseField("error");
        private static final ConstructingObjectParser<ComponentShutdownStatus, Void> PARSER = new ConstructingObjectParser<>(
            "node_shutdown_component",
            a -> new ComponentShutdownStatus(Status.valueOf((String) a[0]), (Long) a[1], (String) a[2])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), STATUS_FIELD);
            PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), TIME_STARTED_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ERROR_FIELD);
        }

        public static ComponentShutdownStatus parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        public ComponentShutdownStatus(Status status, Long startedAtMillis, String errorMessage) {
            this.status = status;
            this.startedAtMillis = startedAtMillis;
            this.errorMessage = errorMessage;
        }

        public ComponentShutdownStatus(StreamInput in) throws IOException {
            this.status = in.readEnum(Status.class);
            this.startedAtMillis = in.readOptionalVLong();
            this.errorMessage = in.readOptionalString();
        }

        public Status getStatus() {
            return status;
        }

        public Long getStartedAtMillis() {
            return startedAtMillis;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field(STATUS_FIELD.getPreferredName(), status);
                if (startedAtMillis != null) {
                    builder.timeField(TIME_STARTED_FIELD.getPreferredName(), "time_started", startedAtMillis);
                }
                if (errorMessage != null) {
                    builder.field(ERROR_FIELD.getPreferredName(), errorMessage);
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(status);
            out.writeOptionalVLong(startedAtMillis);
            out.writeOptionalString(errorMessage);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof ComponentShutdownStatus) == false) return false;
            ComponentShutdownStatus that = (ComponentShutdownStatus) o;
            return getStatus() == that.getStatus()
                && Objects.equals(getStartedAtMillis(), that.getStartedAtMillis())
                && Objects.equals(getErrorMessage(), that.getErrorMessage());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getStatus(), getStartedAtMillis(), getErrorMessage());
        }
    }
}
