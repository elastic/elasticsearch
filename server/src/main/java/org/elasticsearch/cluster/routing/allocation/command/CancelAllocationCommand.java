/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * A command that cancels relocation, or recovery of a given shard on a node.
 */
public class CancelAllocationCommand implements AllocationCommand {

    private static final Logger logger = LogManager.getLogger(CancelAllocationCommand.class);

    public static final String NAME = "cancel";
    public static final ParseField COMMAND_NAME_FIELD = new ParseField(NAME);

    private final String index;
    private final int shardId;
    private final String node;
    private final boolean allowPrimary;
    private final ProjectId projectId;

    /**
     * Creates a new {@link CancelAllocationCommand}
     *
     * @param index index of the shard which allocation should be canceled
     * @param shardId id of the shard which allocation should be canceled
     * @param node id of the node that manages the shard which allocation should be canceled
     * @param projectId      the project-id that this index belongs to
     */
    public CancelAllocationCommand(String index, int shardId, String node, boolean allowPrimary, ProjectId projectId) {
        this.index = index;
        this.shardId = shardId;
        this.node = node;
        this.allowPrimary = allowPrimary;
        this.projectId = projectId;
    }

    @FixForMultiProject(description = "Should be removed since a ProjectId must always be available")
    @Deprecated(forRemoval = true)
    public CancelAllocationCommand(String index, int shardId, String node, boolean allowPrimary) {
        this(index, shardId, node, allowPrimary, Metadata.DEFAULT_PROJECT_ID);
    }

    /**
     * Read from a stream.
     */
    public CancelAllocationCommand(StreamInput in) throws IOException {
        index = in.readString();
        shardId = in.readVInt();
        node = in.readString();
        allowPrimary = in.readBoolean();
        if (in.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
            projectId = ProjectId.readFrom(in);
        } else {
            projectId = Metadata.DEFAULT_PROJECT_ID;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeString(node);
        out.writeBoolean(allowPrimary);
        if (out.getTransportVersion().onOrAfter(TransportVersions.MULTI_PROJECT)) {
            projectId.writeTo(out);
        } else {
            assert Metadata.DEFAULT_PROJECT_ID.equals(projectId) : projectId;
            if (Metadata.DEFAULT_PROJECT_ID.equals(projectId) == false) {
                throw new IllegalArgumentException("expected default project, but got " + projectId);
            }
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ProjectId projectId() {
        return projectId;
    }

    /**
     * Get the index of the shard which allocation should be canceled
     * @return index of the shard which allocation should be canceled
     */
    public String index() {
        return this.index;
    }

    /**

     * Get the id of the shard which allocation should be canceled
     * @return id of the shard which allocation should be canceled
     */
    public int shardId() {
        return this.shardId;
    }

    /**
     * Get the id of the node that manages the shard which allocation should be canceled
     * @return id of the node that manages the shard which allocation should be canceled
     */
    public String node() {
        return this.node;
    }

    public boolean allowPrimary() {
        return this.allowPrimary;
    }

    @Override
    public RerouteExplanation execute(RoutingAllocation allocation, boolean explain) {
        DiscoveryNode discoNode = allocation.nodes().resolveNode(node);
        ShardRouting shardRouting = null;
        RoutingNodes routingNodes = allocation.routingNodes();
        RoutingNode routingNode = routingNodes.node(discoNode.getId());
        IndexMetadata indexMetadata = null;
        if (routingNode != null) {
            indexMetadata = allocation.metadata().getProject(projectId).index(index());
            if (indexMetadata == null) {
                throw new IndexNotFoundException(index());
            }
            ShardId shardId = new ShardId(indexMetadata.getIndex(), shardId());
            shardRouting = routingNode.getByShardId(shardId);
        }
        if (shardRouting == null) {
            if (explain) {
                return new RerouteExplanation(
                    this,
                    allocation.decision(
                        Decision.NO,
                        "cancel_allocation_command",
                        "can't cancel [" + index + "][" + shardId + "], failed to find it on node " + discoNode
                    )
                );
            }
            throw new IllegalArgumentException(
                "[cancel_allocation] can't cancel [" + index + "][" + shardId + "], failed to find it on node " + discoNode
            );
        }
        if (shardRouting.primary() && allowPrimary == false) {
            if ((shardRouting.initializing() && shardRouting.relocatingNodeId() != null) == false) {
                // only allow cancelling initializing shard of primary relocation without allowPrimary flag
                if (explain) {
                    return new RerouteExplanation(
                        this,
                        allocation.decision(
                            Decision.NO,
                            "cancel_allocation_command",
                            "can't cancel ["
                                + index
                                + "]["
                                + shardId
                                + "] on node "
                                + discoNode
                                + ", shard is primary and "
                                + shardRouting.state().name().toLowerCase(Locale.ROOT)
                        )
                    );
                }
                throw new IllegalArgumentException(
                    "[cancel_allocation] can't cancel ["
                        + index
                        + "]["
                        + shardId
                        + "] on node "
                        + discoNode
                        + ", shard is primary and "
                        + shardRouting.state().name().toLowerCase(Locale.ROOT)
                );
            }
        }
        routingNodes.failShard(shardRouting, new UnassignedInfo(UnassignedInfo.Reason.REROUTE_CANCELLED, null), allocation.changes());
        // TODO: We don't have to remove a cancelled shard from in-sync set once we have a strict resync implementation.
        allocation.removeAllocationId(shardRouting);
        return new RerouteExplanation(
            this,
            allocation.decision(
                Decision.YES,
                "cancel_allocation_command",
                "shard [" + index + "][" + shardId + "] on node " + discoNode + " can be cancelled"
            )
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index());
        builder.field("shard", shardId());
        builder.field("node", node());
        builder.field("allow_primary", allowPrimary());
        return builder.endObject();
    }

    @FixForMultiProject(description = "projectId should not be null once multi-project is fully in place")
    public static CancelAllocationCommand fromXContent(XContentParser parser, Object projectId) throws IOException {
        assert projectId == null || projectId instanceof ProjectId : projectId;

        String index = null;
        int shardId = -1;
        String nodeId = null;
        boolean allowPrimary = false;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("index".equals(currentFieldName)) {
                    index = parser.text();
                } else if ("shard".equals(currentFieldName)) {
                    shardId = parser.intValue();
                } else if ("node".equals(currentFieldName)) {
                    nodeId = parser.text();
                } else if ("allow_primary".equals(currentFieldName) || "allowPrimary".equals(currentFieldName)) {
                    allowPrimary = parser.booleanValue();
                } else {
                    throw new ElasticsearchParseException("[{}] command does not support field [{}]", NAME, currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("[{}] command does not support complex json tokens [{}]", NAME, token);
            }
        }
        if (index == null) {
            throw new ElasticsearchParseException("[{}] command missing the index parameter", NAME);
        }
        if (shardId == -1) {
            throw new ElasticsearchParseException("[{}] command missing the shard parameter", NAME);
        }
        if (nodeId == null) {
            throw new ElasticsearchParseException("[{}] command missing the node parameter", NAME);
        }

        return new CancelAllocationCommand(
            index,
            shardId,
            nodeId,
            allowPrimary,
            projectId == null ? Metadata.DEFAULT_PROJECT_ID : (ProjectId) projectId
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        CancelAllocationCommand other = (CancelAllocationCommand) obj;
        // Override equals and hashCode for testing
        return Objects.equals(index, other.index)
            && Objects.equals(shardId, other.shardId)
            && Objects.equals(node, other.node)
            && Objects.equals(allowPrimary, other.allowPrimary)
            && Objects.equals(projectId, other.projectId);
    }

    @Override
    public int hashCode() {
        // Override equals and hashCode for testing
        return Objects.hash(index, shardId, node, allowPrimary, projectId);
    }
}
