/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;

public class DesiredNodes implements Writeable, ToXContentObject, Iterable<DesiredNodeWithStatus> {
    public static final String CONTEXT_MODE_PARAM = "desired_nodes_x_content_context";
    public static final String CONTEXT_MODE_API = SerializationContext.GET_DESIRED_NODES_API.toString();
    public static final String CONTEXT_MODE_CLUSTER_STATE = SerializationContext.CLUSTER_STATE.toString();

    public enum SerializationContext {
        GET_DESIRED_NODES_API,
        CLUSTER_STATE
    }

    private static final ParseField HISTORY_ID_FIELD = new ParseField("history_id");
    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField NODES_FIELD = new ParseField("nodes");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DesiredNodes, Void> PARSER = new ConstructingObjectParser<>(
        "desired_nodes",
        false,
        (args, unused) -> create((String) args[0], (long) args[1], (List<DesiredNodeWithStatus>) args[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HISTORY_ID_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> DesiredNodeWithStatus.fromXContent(p), NODES_FIELD);
    }

    private final String historyID;
    private final long version;
    private final Map<String, DesiredNodeWithStatus> nodes;
    private final Set<DesiredNode> actualized;
    private final Set<DesiredNode> pending;

    private DesiredNodes(String historyID, long version, Map<String, DesiredNodeWithStatus> nodes) {
        assert historyID != null && historyID.isBlank() == false;
        assert version != Long.MIN_VALUE;

        this.historyID = historyID;
        this.version = version;
        this.nodes = Collections.unmodifiableMap(nodes);
        this.actualized = nodes.values()
            .stream()
            .filter(DesiredNodeWithStatus::actualized)
            .map(DesiredNodeWithStatus::desiredNode)
            .collect(Collectors.toUnmodifiableSet());
        this.pending = nodes.values()
            .stream()
            .filter(DesiredNodeWithStatus::pending)
            .map(DesiredNodeWithStatus::desiredNode)
            .collect(Collectors.toUnmodifiableSet());
    }

    public static DesiredNodes readFrom(StreamInput in) throws IOException {
        final var historyId = in.readString();
        final var version = in.readLong();
        final var nodesWithStatus = in.readList(DesiredNodeWithStatus::readFrom);
        return create(historyId, version, nodesWithStatus);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(historyID);
        out.writeLong(version);
        out.writeCollection(nodes.values());
    }

    static DesiredNodes fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(HISTORY_ID_FIELD.getPreferredName(), historyID);
        builder.field(VERSION_FIELD.getPreferredName(), version);
        builder.xContentList(NODES_FIELD.getPreferredName(), nodes.values(), params);
        builder.endObject();
        return builder;
    }

    public static DesiredNodes createIncludingStatusFromPreviousVersion(
        String historyId,
        long version,
        List<DesiredNode> nodes,
        @Nullable DesiredNodes previousDesiredNodes
    ) {
        if (previousDesiredNodes == null || previousDesiredNodes.historyID.equals(historyId) == false) {
            return create(
                historyId,
                version,
                nodes.stream().map(desiredNode -> new DesiredNodeWithStatus(desiredNode, DesiredNodeWithStatus.Status.PENDING)).toList()
            );
        }

        return create(historyId, version, previousDesiredNodes.transferStatusInformation(nodes));
    }

    public static DesiredNodes create(String historyID, long version, List<DesiredNodeWithStatus> nodes) {
        checkForDuplicatedExternalIDs(nodes);

        return new DesiredNodes(historyID, version, toMap(nodes));
    }

    @Nullable
    public static DesiredNodes latestFromClusterState(ClusterState clusterState) {
        return DesiredNodesMetadata.fromClusterState(clusterState).getLatestDesiredNodes();
    }

    public boolean isSupersededBy(DesiredNodes otherDesiredNodes) {
        return historyID.equals(otherDesiredNodes.historyID) == false || version < otherDesiredNodes.version;
    }

    public boolean hasSameVersion(DesiredNodes other) {
        return historyID.equals(other.historyID) && version == other.version;
    }

    public boolean hasSameHistoryId(DesiredNodes other) {
        return other != null && historyID.equals(other.historyID);
    }

    private static void checkForDuplicatedExternalIDs(List<DesiredNodeWithStatus> nodes) {
        Set<String> nodeIDs = new HashSet<>(nodes.size());
        Set<String> duplicatedIDs = new HashSet<>();
        for (DesiredNodeWithStatus node : nodes) {
            String externalID = node.desiredNode().externalId();
            assert externalID != null;
            if (nodeIDs.add(externalID) == false) {
                duplicatedIDs.add(externalID);
            }
        }
        if (duplicatedIDs.isEmpty() == false) {
            throw new IllegalArgumentException(
                format(
                    Locale.ROOT,
                    "Some nodes contain the same setting value %s for [%s]",
                    duplicatedIDs,
                    NODE_EXTERNAL_ID_SETTING.getKey()
                )
            );
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DesiredNodes that = (DesiredNodes) o;
        return version == that.version && Objects.equals(historyID, that.historyID) && Objects.equals(nodes, that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(historyID, version, nodes);
    }

    @Override
    public String toString() {
        return "DesiredNodes{" + "historyID='" + historyID + '\'' + ", version=" + version + ", nodes=" + nodes + '}';
    }

    public String historyID() {
        return historyID;
    }

    public long version() {
        return version;
    }

    public Set<DesiredNodeWithStatus> nodes() {
        return Set.copyOf(nodes.values());
    }

    public Set<DesiredNode> actualized() {
        return actualized;
    }

    public Set<DesiredNode> pending() {
        return pending;
    }

    @Override
    public Iterator<DesiredNodeWithStatus> iterator() {
        return nodes.values().iterator();
    }

    @Nullable
    public DesiredNodeWithStatus find(String externalId) {
        return nodes.get(externalId);
    }

    private static Map<String, DesiredNodeWithStatus> toMap(final List<DesiredNodeWithStatus> desiredNodes) {
        return Collections.unmodifiableMap(
            desiredNodes.stream().collect(Collectors.toMap(DesiredNodeWithStatus::externalId, Function.identity(), (left, right) -> {
                assert left.desiredNode().externalId().equals(right.externalId()) == false;
                throw new IllegalStateException("duplicate desired node external id [" + left.externalId() + "]");
            }, TreeMap::new))
        );
    }

    private List<DesiredNodeWithStatus> transferStatusInformation(List<DesiredNode> proposedDesiredNodes) {
        List<DesiredNodeWithStatus> desiredNodesWithStatus = new ArrayList<>(proposedDesiredNodes.size());
        for (final var desiredNode : proposedDesiredNodes) {
            final var desiredNodeWithStatus = nodes.get(desiredNode.externalId());
            if (desiredNodeWithStatus != null) {
                desiredNodesWithStatus.add(new DesiredNodeWithStatus(desiredNode, desiredNodeWithStatus.status()));
            } else {
                desiredNodesWithStatus.add(new DesiredNodeWithStatus(desiredNode, DesiredNodeWithStatus.Status.PENDING));
            }
        }
        return Collections.unmodifiableList(desiredNodesWithStatus);
    }

    public static ClusterState updateDesiredNodesStatusIfNeeded(ClusterState clusterState) {
        final var desiredNodes = latestFromClusterState(clusterState);

        final var updatedDesiredNodes = updateDesiredNodesStatusIfNeeded(clusterState.nodes(), desiredNodes);
        return desiredNodes == updatedDesiredNodes
            ? clusterState
            : clusterState.copyAndUpdateMetadata(
                metadata -> metadata.putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(updatedDesiredNodes))
            );
    }

    public static DesiredNodes updateDesiredNodesStatusIfNeeded(DiscoveryNodes discoveryNodes, DesiredNodes desiredNodes) {
        if (desiredNodes == null) {
            return null;
        }

        Map<String, DesiredNodeWithStatus> desiredNodesWithUpdatedStatus = null;
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            final var desiredNode = desiredNodes.find(discoveryNode.getExternalId());
            if (desiredNode != null && desiredNode.pending()) {
                if (desiredNodesWithUpdatedStatus == null) {
                    desiredNodesWithUpdatedStatus = new HashMap<>(desiredNodes.nodes);
                }
                desiredNodesWithUpdatedStatus.put(
                    desiredNode.externalId(),
                    new DesiredNodeWithStatus(desiredNode.desiredNode(), DesiredNodeWithStatus.Status.ACTUALIZED)
                );
            }
        }

        return desiredNodesWithUpdatedStatus == null
            ? desiredNodes
            : new DesiredNodes(desiredNodes.historyID(), desiredNodes.version(), desiredNodesWithUpdatedStatus);
    }
}
