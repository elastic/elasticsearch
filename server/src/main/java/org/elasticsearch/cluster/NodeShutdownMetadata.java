/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Contains the data about nodes which are currently configured to shut down, either permanently or temporarily.
 *
 * Stored in the cluster state as custom metadata.
 */
public class NodeShutdownMetadata implements Metadata.Custom {
    public static final String TYPE = "node_shutdown";
    public static final Version NODE_SHUTDOWN_VERSION = Version.V_8_0_0;

    private static final ParseField NODES_FIELD = new ParseField("nodes");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<NodeShutdownMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, a -> {
        final Map<String, SingleNodeShutdownMetadata> nodes = ((List<SingleNodeShutdownMetadata>) a[0]).stream()
            .collect(Collectors.toMap(SingleNodeShutdownMetadata::getNodeId, Function.identity()));
        return new NodeShutdownMetadata(nodes);
    });

    static {
        PARSER.declareNamedObjects(
            ConstructingObjectParser.constructorArg(),
            (p, c, n) -> SingleNodeShutdownMetadata.parse(p),
            v -> { throw new IllegalArgumentException("ordered " + NODES_FIELD.getPreferredName() + " are not supported"); },
            NODES_FIELD
        );
    }

    public static NodeShutdownMetadata parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final Map<String, SingleNodeShutdownMetadata> nodes;

    public NodeShutdownMetadata(Map<String, SingleNodeShutdownMetadata> nodes) {
        this.nodes = nodes;
    }

    public NodeShutdownMetadata(StreamInput in) throws IOException {
        this.nodes = in.readMap(StreamInput::readString, SingleNodeShutdownMetadata::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(nodes, StreamOutput::writeString, (outStream, v) -> v.writeTo(outStream));
    }

    /**
     * Retrieve the data about nodes which are currently in the process of shutting down.
     * @return A map of node IDs to information about the node's shutdown status.
     */
    public Map<String, SingleNodeShutdownMetadata> getPerNodeInfo() {
        return Collections.unmodifiableMap(nodes);
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new NodeShutdownMetadataDiff((NodeShutdownMetadata) previousState, this);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return NODE_SHUTDOWN_VERSION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof NodeShutdownMetadata) == false) return false;
        NodeShutdownMetadata that = (NodeShutdownMetadata) o;
        return getPerNodeInfo().equals(that.getPerNodeInfo());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPerNodeInfo());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(NODES_FIELD.getPreferredName(), nodes);
        return builder;
    }

    /**
     * Handles diffing and appling diffs for {@link NodeShutdownMetadata} as necessary for the cluster state infrastructure.
     */
    public static class NodeShutdownMetadataDiff implements NamedDiff<Metadata.Custom> {

        private final Diff<Map<String, SingleNodeShutdownMetadata>> nodesDiff;

        NodeShutdownMetadataDiff(NodeShutdownMetadata before, NodeShutdownMetadata after) {
            this.nodesDiff = DiffableUtils.diff(before.nodes, after.nodes, DiffableUtils.getStringKeySerializer());
        }

        public NodeShutdownMetadataDiff(StreamInput in) throws IOException {
            this.nodesDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                SingleNodeShutdownMetadata::new,
                NodeShutdownMetadataDiff::readNodesDiffFrom
            );
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            TreeMap<String, SingleNodeShutdownMetadata> newNodes = new TreeMap<>(nodesDiff.apply(((NodeShutdownMetadata) part).getPerNodeInfo()));
            return new NodeShutdownMetadata(newNodes);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            nodesDiff.writeTo(out);
        }

        static Diff<SingleNodeShutdownMetadata> readNodesDiffFrom(StreamInput in) throws IOException {
            return AbstractDiffable.readDiffFrom(SingleNodeShutdownMetadata::new, in);
        }
    }

    /**
     * Contains data about a single node's shutdown readiness.
     */
    public static class SingleNodeShutdownMetadata extends AbstractDiffable<SingleNodeShutdownMetadata>
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
}
