/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class DesiredNodesMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {
    private static final Version MIN_SUPPORTED_VERSION = Version.V_8_1_0;
    private static final Version MEMBER_TRACKING_VERSION = Version.V_8_2_0;

    public static final String TYPE = "desired_nodes";

    public static final DesiredNodesMetadata EMPTY = new DesiredNodesMetadata(null);

    private static final ParseField LATEST_FIELD = new ParseField("latest");
    private static final ParseField MEMBER_IDS_FIELD = new ParseField("member_external_ids");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DesiredNodesMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        (args, unused) -> DesiredNodesMetadata.createFromMemberIds((DesiredNodes) args[0], (List<String>) args[1])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DesiredNodes.fromXContent(p), LATEST_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), MEMBER_IDS_FIELD);
    }

    private static DesiredNodesMetadata createFromMemberIds(DesiredNodes latestDesiredNodes, Collection<String> clusterMemberExternalIds) {
        assert latestDesiredNodes != null;

        final Set<DesiredNode> clusterMembers = clusterMemberExternalIds.stream().map(latestDesiredNodes::find).collect(Collectors.toSet());
        return create(latestDesiredNodes, clusterMembers);
    }

    public static DesiredNodesMetadata create(DesiredNodes latestDesiredNodes, Set<DesiredNode> clusterMembers) {
        assert latestDesiredNodes != null;

        final Set<DesiredNode> unknownNodes = new HashSet<>(latestDesiredNodes.nodes());
        unknownNodes.removeAll(clusterMembers);
        return new DesiredNodesMetadata(latestDesiredNodes, clusterMembers, unknownNodes);
    }

    private final DesiredNodes latestDesiredNodes;
    private final Set<DesiredNode> clusterMembers;
    private final Set<DesiredNode> notClusterMembers;

    public DesiredNodesMetadata(DesiredNodes latestDesiredNodes) {
        this(latestDesiredNodes, Collections.emptySet(), Collections.emptySet());
    }

    private DesiredNodesMetadata(DesiredNodes latestDesiredNodes, Set<DesiredNode> clusterMembers, Set<DesiredNode> notClusterMembers) {
        assert clusterMembers.stream().noneMatch(Objects::isNull);
        assert clusterMembers.stream().noneMatch(notClusterMembers::contains);
        this.latestDesiredNodes = latestDesiredNodes;
        this.clusterMembers = Collections.unmodifiableSortedSet(new TreeSet<>(clusterMembers));
        this.notClusterMembers = Collections.unmodifiableSortedSet(new TreeSet<>(notClusterMembers));
    }

    public static DesiredNodesMetadata readFrom(StreamInput in) throws IOException {
        final DesiredNodes desiredNodes = new DesiredNodes(in);
        if (in.getVersion().onOrAfter(MEMBER_TRACKING_VERSION)) {
            List<String> externalIds = in.readStringList();
            return createFromMemberIds(desiredNodes, externalIds);
        } else {
            return new DesiredNodesMetadata(desiredNodes);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        latestDesiredNodes.writeTo(out);
        if (out.getVersion().onOrAfter(MEMBER_TRACKING_VERSION)) {
            final List<String> memberExternalIds = clusterMembers.stream().map(DesiredNode::externalId).toList();
            out.writeStringCollection(memberExternalIds);
        }
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    public static DesiredNodesMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(LATEST_FIELD.getPreferredName(), latestDesiredNodes);
        builder.stringListField(MEMBER_IDS_FIELD.getPreferredName(), clusterMembers.stream().map(DesiredNode::externalId).toList());
        return builder;
    }

    public Set<DesiredNode> getClusterMembers() {
        return clusterMembers;
    }

    public Set<DesiredNode> getNotClusterMembers() {
        return notClusterMembers;
    }

    public static DesiredNodesMetadata fromClusterState(ClusterState clusterState) {
        return clusterState.metadata().custom(TYPE, EMPTY);
    }

    @Nullable
    public DesiredNodes getLatestDesiredNodes() {
        return latestDesiredNodes;
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
        return MIN_SUPPORTED_VERSION;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DesiredNodesMetadata that = (DesiredNodesMetadata) o;
        return Objects.equals(latestDesiredNodes, that.latestDesiredNodes)
            && Objects.equals(clusterMembers, that.clusterMembers)
            && Objects.equals(notClusterMembers, that.notClusterMembers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latestDesiredNodes, clusterMembers, notClusterMembers);
    }

    @Override
    public String toString() {
        return "DesiredNodesMetadata{"
            + "latestDesiredNodes="
            + latestDesiredNodes
            + ", clusterMembers="
            + clusterMembers
            + ", notClusterMembers="
            + notClusterMembers
            + '}';
    }

    public static class Builder {
        private final Logger logger = LogManager.getLogger(Builder.class);

        private DesiredNodes desiredNodes;
        private final Set<DesiredNode> clusterMembers;

        public Builder(DesiredNodesMetadata desiredNodesMetadata) {
            this.desiredNodes = desiredNodesMetadata.latestDesiredNodes;
            this.clusterMembers = new HashSet<>(desiredNodesMetadata.clusterMembers);
        }

        public Builder withDesiredNodes(DesiredNodes desiredNodes) {
            // It's possible that a node left the cluster temporarily,
            // and we don't want to remove its cluster membership status
            // in the desired nodes state.
            clusterMembers.removeIf(clusterMember -> desiredNodes.find(clusterMember.externalId()) == null);
            this.desiredNodes = desiredNodes;
            return this;
        }

        /**
         * Returns true if the desired nodes membership has changed
         */
        public boolean markDesiredNodeAsMemberIfPresent(DiscoveryNode discoveryNode) {
            if (desiredNodes == null) {
                return false;
            }

            DesiredNode desiredNode = desiredNodes.find(discoveryNode.getExternalId());

            if (desiredNode == null) {
                logger.warn("{} is missing in desired nodes {}", discoveryNode, desiredNodes);
            }

            if (desiredNode != null && desiredNode.getRoles().equals(discoveryNode.getRoles()) == false) {
                logger.warn(
                    "Node {} has different roles {} than its desired node {}",
                    discoveryNode,
                    desiredNode.getRoles(),
                    discoveryNode.getRoles()
                );
            }

            return desiredNode != null && clusterMembers.add(desiredNode);
        }

        public DesiredNodesMetadata build() {
            return DesiredNodesMetadata.create(desiredNodes, clusterMembers);
        }
    }
}
