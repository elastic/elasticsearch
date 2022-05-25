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

public class DesiredNodes implements Writeable, ToXContentObject, Iterable<DesiredNode> {

    private static final ParseField HISTORY_ID_FIELD = new ParseField("history_id");
    private static final ParseField VERSION_FIELD = new ParseField("version");
    private static final ParseField NODES_FIELD = new ParseField("nodes");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DesiredNodes, Void> PARSER = new ConstructingObjectParser<>(
        "desired_nodes",
        false,
        (args, unused) -> new DesiredNodes((String) args[0], (long) args[1], (List<DesiredNode>) args[2])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), HISTORY_ID_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION_FIELD);
        PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> DesiredNode.fromXContent(p, DesiredNode.ParsingContext.CLUSTER_STATE),
            NODES_FIELD
        );
    }

    private final String historyID;
    private final long version;
    private final Map<String, DesiredNode> nodes;
    private final List<DesiredNode> members;
    private final List<DesiredNode> notMembers;

    public DesiredNodes(String historyID, long version, List<DesiredNode> nodes) {
        assert historyID != null && historyID.isBlank() == false;
        assert version != Long.MIN_VALUE;
        checkForDuplicatedExternalIDs(nodes);

        this.historyID = historyID;
        this.version = version;
        this.nodes = toMap(nodes);
        this.members = nodes.stream().filter(DesiredNode::isMember).toList();
        this.notMembers = nodes.stream().filter(dn -> dn.isMember() == false).toList();
    }

    private DesiredNodes(String historyID, long version, Map<String, DesiredNode> nodes) {
        this.historyID = historyID;
        this.version = version;
        this.nodes = Collections.unmodifiableMap(nodes);
        this.members = nodes.values().stream().filter(DesiredNode::isMember).toList();
        this.notMembers = nodes.values().stream().filter(dn -> dn.isMember() == false).toList();
    }

    public DesiredNodes(StreamInput in) throws IOException {
        this(in.readString(), in.readLong(), in.readList(DesiredNode::readFrom));
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
        builder.xContentList(NODES_FIELD.getPreferredName(), nodes.values());
        builder.endObject();
        return builder;
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

    private static void checkForDuplicatedExternalIDs(List<DesiredNode> nodes) {
        Set<String> nodeIDs = new HashSet<>(nodes.size());
        Set<String> duplicatedIDs = new HashSet<>();
        for (DesiredNode node : nodes) {
            String externalID = node.externalId();
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

    public List<DesiredNode> nodes() {
        return List.copyOf(nodes.values());
    }

    public List<DesiredNode> members() {
        assert members.stream().allMatch(DesiredNode::isMember);
        return members;
    }

    public List<DesiredNode> notMembers() {
        assert notMembers.stream().allMatch(desiredNode -> desiredNode.isMember() == false);

        return notMembers;
    }

    @Override
    public Iterator<DesiredNode> iterator() {
        return nodes.values().iterator();
    }

    @Nullable
    public DesiredNode find(String externalId) {
        return nodes.get(externalId);
    }

    private static Map<String, DesiredNode> toMap(final List<DesiredNode> desiredNodes) {
        // use a linked hash map to preserve order
        return Collections.unmodifiableMap(
            desiredNodes.stream().collect(Collectors.toMap(DesiredNode::externalId, Function.identity(), (left, right) -> {
                assert left.externalId().equals(right.externalId()) == false;
                throw new IllegalStateException("duplicate desired node external id [" + left.externalId() + "]");
            }, TreeMap::new))
        );
    }

    public static ClusterState withDesiredNodesMembershipUpdated(ClusterState clusterState) {
        final var desiredNodes = latestFromClusterState(clusterState);
        if (desiredNodes == null) {
            return clusterState;
        }

        return withMembershipInformationUpgraded(clusterState, desiredNodes);
    }

    public static ClusterState withMembershipInformationUpgraded(ClusterState clusterState, DesiredNodes desiredNodes) {
        if (desiredNodes == null) {
            return clusterState;
        }

        final Map<String, DesiredNode> updatedStateDesiredNodes = new HashMap<>(desiredNodes.nodes);

        boolean membershipInformationModified = false;
        for (DiscoveryNode discoveryNode : clusterState.nodes()) {
            final var desiredNode = desiredNodes.find(discoveryNode.getExternalId());
            if (desiredNode != null && desiredNode.isMember() == false) {
                updatedStateDesiredNodes.put(desiredNode.externalId(), desiredNode.asMember());
                membershipInformationModified = true;
            }
        }

        if (membershipInformationModified || desiredNodes != latestFromClusterState(clusterState)) {
            final var updatedClusterState = clusterState.copyAndUpdateMetadata(
                metadata -> metadata.putCustom(
                    DesiredNodesMetadata.TYPE,
                    new DesiredNodesMetadata(new DesiredNodes(desiredNodes.historyID(), desiredNodes.version(), updatedStateDesiredNodes))
                )
            );
            assert knownDesiredNodesAreCorrect(updatedClusterState);
            return updatedClusterState;
        } else {
            assert knownDesiredNodesAreCorrect(clusterState);
            return clusterState;
        }
    }

    public DesiredNodes withMembershipInfoFrom(DesiredNodes previousDesiredNodes) {
        if (previousDesiredNodes == this) {
            return this;
        }
        final Map<String, DesiredNode> updatedStateDesiredNodes = new HashMap<>(nodes);

        boolean modified = false;
        if (hasSameHistoryId(previousDesiredNodes)) {
            for (DesiredNode desiredNode : nodes.values()) {
                final var previousDesiredNode = previousDesiredNodes.find(desiredNode.externalId());
                if (previousDesiredNode != null && previousDesiredNode.membershipStatus() != desiredNode.membershipStatus()) {
                    updatedStateDesiredNodes.put(
                        desiredNode.externalId(),
                        desiredNode.withMembershipStatus(previousDesiredNode.membershipStatus())
                    );
                    modified = true;
                }
            }
        }

        if (modified) {
            return new DesiredNodes(historyID, version, updatedStateDesiredNodes);
        } else {
            return this;
        }
    }

    public static boolean knownDesiredNodesAreCorrect(ClusterState clusterState) {
        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterState);
        return desiredNodes == null
            || clusterState.nodes()
                .stream()
                .map(node -> desiredNodes.find(node.getExternalId()))
                .filter(Objects::nonNull)
                .allMatch(DesiredNode::isMember);
    }

    public static class MembershipInformation {
        public static final MembershipInformation EMPTY = new MembershipInformation(null, Set.of());

        private final DesiredNodes desiredNodes;
        private final Set<DesiredNode> members;
        private final Set<DesiredNode> notMembers;

        public MembershipInformation(DesiredNodes desiredNodes, Set<DesiredNode> members) {
            Objects.requireNonNull(members);

            this.desiredNodes = desiredNodes;
            this.members = members;
            if (desiredNodes == null) {
                this.notMembers = Collections.emptySet();
            } else {
                final Set<DesiredNode> pendingMembers = new HashSet<>();
                for (DesiredNode desiredNode : desiredNodes) {
                    if (members.contains(desiredNode) == false) {
                        pendingMembers.add(desiredNode);
                    }
                }
                this.notMembers = Collections.unmodifiableSet(pendingMembers);
            }
        }

        public boolean isMember(DesiredNode desiredNode) {
            return members.contains(desiredNode);
        }

        public int memberCount() {
            return members.size();
        }

        public List<DesiredNode> allDesiredNodes() {
            return desiredNodes == null ? Collections.emptyList() : desiredNodes.nodes();
        }

        public Set<DesiredNode> notMembers() {
            return notMembers;
        }

        public Set<DesiredNode> members() {
            return members;
        }
    }
}
