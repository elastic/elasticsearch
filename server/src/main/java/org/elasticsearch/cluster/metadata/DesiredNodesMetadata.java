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
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class DesiredNodesMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {
    private static final Version MIN_SUPPORTED_VERSION = Version.V_8_1_0;
    private static final Version MEMBER_TRACKING_VERSION = Version.V_8_2_0;

    public static final String TYPE = "desired_nodes";

    public static final DesiredNodesMetadata EMPTY = new DesiredNodesMetadata((DesiredNodes) null);

    private static final ParseField LATEST_FIELD = new ParseField("latest");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DesiredNodesMetadata, Void> PARSER = new ConstructingObjectParser<>(
        TYPE,
        false,
        (args, unused) -> new DesiredNodesMetadata((DesiredNodes) args[0])
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> DesiredNodes.fromXContent(p), LATEST_FIELD);
    }

    private final DesiredNodes latestDesiredNodes;
    private final Set<DesiredNode> memberDesiredNodes;
    private final Set<DesiredNode> unknownNodes;

    public DesiredNodesMetadata(DesiredNodes latestDesiredNodes) {
        this(latestDesiredNodes, Collections.emptySet());
    }

    private DesiredNodesMetadata(DesiredNodes latestDesiredNodes, Set<DesiredNode> memberDesiredNodes) {
        this.latestDesiredNodes = latestDesiredNodes;
        this.memberDesiredNodes = Collections.unmodifiableSet(memberDesiredNodes);
        if (latestDesiredNodes != null) {
            final Set<DesiredNode> unknownNodes = new HashSet<>(latestDesiredNodes.nodes());
            unknownNodes.removeAll(memberDesiredNodes);
            this.unknownNodes = Collections.unmodifiableSet(unknownNodes);
        } else {
            this.unknownNodes = Collections.emptySet();
        }
    }

    public static DesiredNodesMetadata readFrom(StreamInput in) throws IOException {
        final DesiredNodes desiredNodes = DesiredNodes.readFrom(in);
        if (in.getVersion().onOrAfter(MEMBER_TRACKING_VERSION)) {
            List<String> externalIds = in.readStringList();
            Set<DesiredNode> memberNodes = new HashSet<>(externalIds.size());
            for (String externalId : externalIds) {
                DesiredNode desiredNode = desiredNodes.find(externalId);
                assert desiredNode != null;
                memberNodes.add(desiredNode);
            }
            return new DesiredNodesMetadata(desiredNodes, memberNodes);
        } else {
            return new DesiredNodesMetadata(desiredNodes);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        latestDesiredNodes.writeTo(out);
        if (out.getVersion().onOrAfter(MEMBER_TRACKING_VERSION)) {
            final List<String> memberExternalIds = memberDesiredNodes.stream().map(DesiredNode::externalId).toList();
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
        return builder;
    }

    public Set<DesiredNode> getClusterMembers() {
        return memberDesiredNodes;
    }

    public Set<DesiredNode> getUnknownNodes() {
        return unknownNodes;
    }

    public static DesiredNodes latestFromClusterState(ClusterState clusterState) {
        return clusterState.metadata().custom(TYPE, EMPTY).getLatestDesiredNodes();
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
        return Objects.equals(latestDesiredNodes, that.latestDesiredNodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(latestDesiredNodes);
    }

    public static class Builder {
        private final Logger logger = LogManager.getLogger(Builder.class);

        private final DesiredNodes desiredNodes;
        private final Set<DesiredNode> members;

        public Builder(DesiredNodesMetadata desiredNodesMetadata) {
            this.desiredNodes = desiredNodesMetadata.latestDesiredNodes;
            this.members = new HashSet<>(desiredNodesMetadata.memberDesiredNodes);
        }

        public Builder(DesiredNodes desiredNodes) {
            this.desiredNodes = desiredNodes;
            this.members = new HashSet<>();
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

            return desiredNode != null && members.add(desiredNode);
        }

        public DesiredNodesMetadata build() {
            return new DesiredNodesMetadata(desiredNodes, members);
        }
    }
}
