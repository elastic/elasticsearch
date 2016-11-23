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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A node-level explanation for the decision to rebalance a shard.
 */
public final class NodeRebalanceResult extends NodeAllocationResult {
    private final Decision.Type nodeDecisionType;
    private final boolean betterWeightThanCurrent;
    private final boolean deltaAboveThreshold;
    private final boolean betterWeightWithShardAdded;

    public NodeRebalanceResult(DiscoveryNode node, Decision.Type nodeDecisionType, Decision canAllocate, int weightRanking,
                               boolean betterWeightThanCurrent, boolean deltaAboveThreshold, boolean betterWeightWithShardAdded) {
        super(node, canAllocate, weightRanking);
        this.nodeDecisionType = Objects.requireNonNull(nodeDecisionType);
        this.betterWeightThanCurrent = betterWeightThanCurrent;
        this.deltaAboveThreshold = deltaAboveThreshold;
        this.betterWeightWithShardAdded = betterWeightWithShardAdded;
    }

    public NodeRebalanceResult(StreamInput in) throws IOException {
        super(in);
        nodeDecisionType = Decision.Type.readFrom(in);
        betterWeightThanCurrent = in.readBoolean();
        deltaAboveThreshold = in.readBoolean();
        betterWeightWithShardAdded = in.readBoolean();
    }

    @Override
    public Decision.Type getNodeDecisionType() {
        return nodeDecisionType;
    }

    /**
     * Returns whether the weight of the node is better than the weight of the node where the shard currently resides.
     */
    public boolean isBetterWeightThanCurrent() {
        return betterWeightThanCurrent;
    }

    /**
     * Returns if the weight delta by assigning to this node was above the threshold to warrant a rebalance.
     */
    public boolean isDeltaAboveThreshold() {
        return deltaAboveThreshold;
    }

    /**
     * Returns if the simulated weight of the shard after assigning the node to it would be better than the current node's weight.
     * If {@code true}, then the shard should be rebalanced to the node, assuming allocation is allowed to the node.
     */
    public boolean isBetterWeightWithShardAdded() {
        return betterWeightWithShardAdded;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("better_weight_than_current", betterWeightThanCurrent);
        if (betterWeightThanCurrent) {
            builder.field("delta_above_threshold", deltaAboveThreshold);
            builder.field("better_weight_with_shard_added", betterWeightWithShardAdded);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        nodeDecisionType.writeTo(out);
        out.writeBoolean(betterWeightThanCurrent);
        out.writeBoolean(deltaAboveThreshold);
        out.writeBoolean(betterWeightWithShardAdded);
    }
}
