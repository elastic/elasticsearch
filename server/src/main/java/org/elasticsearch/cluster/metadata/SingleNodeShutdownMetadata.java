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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
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
    private static final ParseField STARTED_AT_FIELD = new ParseField("shutdown_started");
    private static final ParseField STARTED_AT_MILLIS_FIELD = new ParseField("shutdown_started_millis");

    public static final ConstructingObjectParser<SingleNodeShutdownMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "node_shutdown_info",
        a -> new SingleNodeShutdownMetadata((String) a[0], (String) a[1], (String) a[2], (boolean) a[3], (long) a[4])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NODE_ID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REASON_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), STATUS_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), STARTED_AT_MILLIS_FIELD);
    }

    public static SingleNodeShutdownMetadata parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String nodeId;
    private final String type;
    private final String reason;
    private final boolean status; // GWB> Replace with an actual status object
    private final long startedAtDate;

    public SingleNodeShutdownMetadata(String nodeId, String type, String reason, boolean status, long startedAtDate) {
        this.nodeId = Objects.requireNonNull(nodeId, "node ID must not be null");
        this.type = Objects.requireNonNull(type, "shutdown type must not be null");
        this.reason = Objects.requireNonNull(reason, "shutdown reason must not be null");
        this.status = status;
        this.startedAtDate = startedAtDate;
    }

    public SingleNodeShutdownMetadata(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.type = in.readString();
        this.reason = in.readString();
        this.status = in.readBoolean();
        this.startedAtDate = in.readVLong();
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
    public String getType() {
        return type;
    }

    /**
     * @return The user-supplied reason this node is shutting down.
     */
    public String getReason() {
        return reason;
    }

    /**
     * @return True if this node is ready to shut down, false otherwise.
     */
    public boolean isStatus() {
        return status;
    }

    /**
     * @return The timestamp that this shutdown procedure was started.
     */
    public long getStartedAtDate() {
        return startedAtDate;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeString(type);
        out.writeString(reason);
        out.writeBoolean(status);
        out.writeVLong(startedAtDate);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(NODE_ID_FIELD.getPreferredName(), nodeId);
            builder.field(TYPE_FIELD.getPreferredName(), type);
            builder.field(REASON_FIELD.getPreferredName(), reason);
            builder.field(STATUS_FIELD.getPreferredName(), status);
            builder.timeField(STARTED_AT_MILLIS_FIELD.getPreferredName(), STARTED_AT_FIELD.getPreferredName(), startedAtDate);
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
            && getStartedAtDate() == that.getStartedAtDate()
            && getNodeId().equals(that.getNodeId())
            && getType().equals(that.getType())
            && getReason().equals(that.getReason());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodeId(), getType(), getReason(), isStatus(), getStartedAtDate());
    }
}
