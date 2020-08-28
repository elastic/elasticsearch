/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.command;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RerouteExplanation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Abstract base class for allocating an unassigned shard to a node
 */
public abstract class AbstractAllocateAllocationCommand implements AllocationCommand {

    private static final String INDEX_FIELD = "index";
    private static final String SHARD_FIELD = "shard";
    private static final String NODE_FIELD = "node";

    protected static <T extends Builder<?>> ObjectParser<T, Void> createAllocateParser(String command) {
        ObjectParser<T, Void> parser = new ObjectParser<>(command);
        parser.declareString(Builder::setIndex, new ParseField(INDEX_FIELD));
        parser.declareInt(Builder::setShard, new ParseField(SHARD_FIELD));
        parser.declareString(Builder::setNode, new ParseField(NODE_FIELD));
        return parser;
    }

    /**
     * Works around ObjectParser not supporting constructor arguments.
     */
    protected abstract static class Builder<T extends AbstractAllocateAllocationCommand> {
        protected String index;
        protected int shard = -1;
        protected String node;

        public void setIndex(String index) {
            this.index = index;
        }

        public void setShard(int shard) {
            this.shard = shard;
        }

        public void setNode(String node) {
            this.node = node;
        }

        public abstract Builder<T> parse(XContentParser parser) throws IOException;

        public abstract T build();

        protected void validate() {
            if (index == null) {
                throw new IllegalArgumentException("Argument [index] must be defined");
            }
            if (shard < 0) {
                throw new IllegalArgumentException("Argument [shard] must be defined and non-negative");
            }
            if (node == null) {
                throw new IllegalArgumentException("Argument [node] must be defined");
            }
        }
    }

    protected final String index;
    protected final int shardId;
    protected final String node;

    protected AbstractAllocateAllocationCommand(String index, int shardId, String node) {
        this.index = index;
        this.shardId = shardId;
        this.node = node;
    }

    /**
     * Read from a stream.
     */
    protected AbstractAllocateAllocationCommand(StreamInput in) throws IOException {
        index = in.readString();
        shardId = in.readVInt();
        node = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        out.writeString(node);
    }

    /**
     * Get the index name
     *
     * @return name of the index
     */
    public String index() {
        return this.index;
    }

    /**
     * Get the shard id
     *
     * @return id of the shard
     */
    public int shardId() {
        return this.shardId;
    }

    /**
     * Get the id of the node
     *
     * @return id of the node
     */
    public String node() {
        return this.node;
    }

    /**
     * Handle case where a disco node cannot be found in the routing table. Usually means that it's not a data node.
     */
    protected RerouteExplanation explainOrThrowMissingRoutingNode(RoutingAllocation allocation, boolean explain, DiscoveryNode discoNode) {
        if (!discoNode.isDataNode()) {
            return explainOrThrowRejectedCommand(explain, allocation, "allocation can only be done on data nodes, not [" + node + "]");
        } else {
            return explainOrThrowRejectedCommand(explain, allocation, "could not find [" + node + "] among the routing nodes");
        }
    }

    /**
     * Utility method for rejecting the current allocation command based on provided reason
     */
    protected RerouteExplanation explainOrThrowRejectedCommand(boolean explain, RoutingAllocation allocation, String reason) {
        if (explain) {
            return new RerouteExplanation(this, allocation.decision(Decision.NO, name() + " (allocation command)", reason));
        }
        throw new IllegalArgumentException("[" + name() + "] " + reason);
    }

    /**
     * Utility method for rejecting the current allocation command based on provided exception
     */
    protected RerouteExplanation explainOrThrowRejectedCommand(boolean explain, RoutingAllocation allocation, RuntimeException rte) {
        if (explain) {
            return new RerouteExplanation(this, allocation.decision(Decision.NO, name() + " (allocation command)", rte.getMessage()));
        }
        throw rte;
    }

    /**
     * Initializes an unassigned shard on a node and removes it from the unassigned
     *
     * @param allocation the allocation
     * @param routingNodes the routing nodes
     * @param routingNode the node to initialize it to
     * @param shardRouting the shard routing that is to be matched in unassigned shards
     */
    protected void initializeUnassignedShard(RoutingAllocation allocation, RoutingNodes routingNodes,
                                             RoutingNode routingNode, ShardRouting shardRouting) {
        initializeUnassignedShard(allocation, routingNodes, routingNode, shardRouting, null, null);
    }

    /**
     * Initializes an unassigned shard on a node and removes it from the unassigned
     *
     * @param allocation the allocation
     * @param routingNodes the routing nodes
     * @param routingNode the node to initialize it to
     * @param shardRouting the shard routing that is to be matched in unassigned shards
     * @param unassignedInfo unassigned info to override
     * @param recoverySource recovery source to override
     */
    protected void initializeUnassignedShard(RoutingAllocation allocation, RoutingNodes routingNodes, RoutingNode routingNode,
                                             ShardRouting shardRouting, @Nullable UnassignedInfo unassignedInfo,
                                             @Nullable RecoverySource recoverySource) {
        for (RoutingNodes.UnassignedShards.UnassignedIterator it = routingNodes.unassigned().iterator(); it.hasNext(); ) {
            ShardRouting unassigned = it.next();
            if (!unassigned.equalsIgnoringMetadata(shardRouting)) {
                continue;
            }
            if (unassignedInfo != null || recoverySource != null) {
                unassigned = it.updateUnassigned(unassignedInfo != null ? unassignedInfo : unassigned.unassignedInfo(),
                    recoverySource != null ? recoverySource : unassigned.recoverySource(), allocation.changes());
            }
            it.initialize(routingNode.nodeId(), null,
                allocation.clusterInfo().getShardSize(unassigned, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE), allocation.changes());
            return;
        }
        assert false : "shard to initialize not found in list of unassigned shards";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD, index());
        builder.field(SHARD_FIELD, shardId());
        builder.field(NODE_FIELD, node());
        extraXContent(builder);
        return builder.endObject();
    }

    protected void extraXContent(XContentBuilder builder) throws IOException {
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AbstractAllocateAllocationCommand other = (AbstractAllocateAllocationCommand) obj;
        // Override equals and hashCode for testing
        return Objects.equals(index, other.index) &&
                Objects.equals(shardId, other.shardId) &&
                Objects.equals(node, other.node);
    }

    @Override
    public int hashCode() {
        // Override equals and hashCode for testing
        return Objects.hash(index, shardId, node);
    }
}
