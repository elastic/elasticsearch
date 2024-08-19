/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Contains the data about nodes which are currently configured to shut down, either permanently or temporarily.
 *
 * Stored in the cluster state as custom metadata.
 */
public class NodesShutdownMetadata implements Metadata.Custom {
    public static final String TYPE = "node_shutdown";
    public static final TransportVersion NODE_SHUTDOWN_VERSION = TransportVersions.V_7_13_0;
    public static final NodesShutdownMetadata EMPTY = new NodesShutdownMetadata(Map.of());

    private static final ParseField NODES_FIELD = new ParseField("nodes");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<NodesShutdownMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, a -> {
        final Map<String, SingleNodeShutdownMetadata> nodes = ((List<SingleNodeShutdownMetadata>) a[0]).stream()
            .collect(Collectors.toMap(SingleNodeShutdownMetadata::getNodeId, Function.identity()));
        return new NodesShutdownMetadata(nodes);
    });

    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> SingleNodeShutdownMetadata.parse(p), v -> {
            throw new IllegalArgumentException("ordered " + NODES_FIELD.getPreferredName() + " are not supported");
        }, NODES_FIELD);
    }

    public static NodesShutdownMetadata fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new NodeShutdownMetadataDiff(in);
    }

    private final Map<String, SingleNodeShutdownMetadata> nodes;

    public NodesShutdownMetadata(Map<String, SingleNodeShutdownMetadata> nodes) {
        this.nodes = Map.copyOf(nodes);
    }

    public NodesShutdownMetadata(StreamInput in) throws IOException {
        this(in.readImmutableMap(SingleNodeShutdownMetadata::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(nodes, StreamOutput::writeWriteable);
    }

    /**
     * @return A map of NodeID to shutdown metadata.
     */
    public Map<String, SingleNodeShutdownMetadata> getAll() {
        return nodes;
    }

    /**
     * @return a set of all node ids that might be restarting or shutting down
     */
    public Set<String> getAllNodeIds() {
        return nodes.keySet();
    }

    /**
     * @return a shutdown entry for the node if exists with any shutdown type
     */
    @Nullable
    public SingleNodeShutdownMetadata get(String nodeId) {
        return nodes.get(nodeId);
    }

    /**
     * Returns true if the given node is marked as shutting down with any shutdown type.
     */
    public boolean contains(String nodeId) {
        return get(nodeId) != null;
    }

    /**
     * @return a shutdown entry for the node if exists and matches the supplied type
     */
    @Nullable
    public SingleNodeShutdownMetadata get(String nodeId, SingleNodeShutdownMetadata.Type type) {
        var shutdown = get(nodeId);
        return shutdown != null && shutdown.getType() == type ? shutdown : null;
    }

    public boolean contains(String nodeId, SingleNodeShutdownMetadata.Type type) {
        return get(nodeId, type) != null;
    }

    /**
     * Checks if the provided node is scheduled for being permanently removed from the cluster.
     */
    public boolean isNodeMarkedForRemoval(String nodeId) {
        var singleNodeShutdownMetadata = get(nodeId);
        return singleNodeShutdownMetadata != null && singleNodeShutdownMetadata.getType().isRemovalType();
    }

    /**
     * Add or update the shutdown metadata for a single node.
     * @param nodeShutdownMetadata The single node shutdown metadata to add or update.
     * @return A new {@link NodesShutdownMetadata} that reflects the updated value.
     */
    public NodesShutdownMetadata putSingleNodeMetadata(SingleNodeShutdownMetadata nodeShutdownMetadata) {
        HashMap<String, SingleNodeShutdownMetadata> newNodes = new HashMap<>(nodes);
        newNodes.put(nodeShutdownMetadata.getNodeId(), nodeShutdownMetadata);
        return new NodesShutdownMetadata(newNodes);
    }

    /**
     * Removes all shutdown metadata for a particular node ID.
     * @param nodeId The node ID to remove shutdown metadata for.
     * @return A new {@link NodesShutdownMetadata} that does not contain shutdown metadata for the given node.
     */
    public NodesShutdownMetadata removeSingleNodeMetadata(String nodeId) {
        HashMap<String, SingleNodeShutdownMetadata> newNodes = new HashMap<>(nodes);
        newNodes.remove(nodeId);
        return new NodesShutdownMetadata(newNodes);
    }

    @Override
    public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
        return new NodeShutdownMetadataDiff((NodesShutdownMetadata) previousState, this);
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
    public TransportVersion getMinimalSupportedVersion() {
        return NODE_SHUTDOWN_VERSION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof NodesShutdownMetadata) == false) return false;
        NodesShutdownMetadata that = (NodesShutdownMetadata) o;
        return nodes.equals(that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodes);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params ignored) {
        return ChunkedToXContentHelper.xContentValuesMap(NODES_FIELD.getPreferredName(), nodes);
    }

    /**
     * Handles diffing and appling diffs for {@link NodesShutdownMetadata} as necessary for the cluster state infrastructure.
     */
    public static class NodeShutdownMetadataDiff implements NamedDiff<Metadata.Custom> {

        private final Diff<Map<String, SingleNodeShutdownMetadata>> nodesDiff;

        NodeShutdownMetadataDiff(NodesShutdownMetadata before, NodesShutdownMetadata after) {
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
            TreeMap<String, SingleNodeShutdownMetadata> newNodes = new TreeMap<>(nodesDiff.apply(((NodesShutdownMetadata) part).nodes));
            return new NodesShutdownMetadata(newNodes);
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
            return SimpleDiffable.readDiffFrom(SingleNodeShutdownMetadata::new, in);
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return NODE_SHUTDOWN_VERSION;
        }

    }
}
