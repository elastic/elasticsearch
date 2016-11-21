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
    private final float delta;
    private final boolean deltaAboveThreshold;
    private final float weightWithShardAdded;
    private final float deltaWithShardAdded;

    public NodeRebalanceResult(DiscoveryNode node, Decision.Type nodeDecisionType, Decision canAllocate, boolean betterWeightThanCurrent,
                               float delta, boolean deltaAboveThreshold, float currentWeight, float weightWithShardAdded,
                               float deltaWithShardAdded) {
        super(node, canAllocate, currentWeight);
        this.nodeDecisionType = Objects.requireNonNull(nodeDecisionType);
        this.betterWeightThanCurrent = betterWeightThanCurrent;
        this.delta = delta;
        this.deltaAboveThreshold = deltaAboveThreshold;
        this.weightWithShardAdded = weightWithShardAdded;
        this.deltaWithShardAdded = deltaWithShardAdded;
    }

    public NodeRebalanceResult(StreamInput in) throws IOException {
        super(in);
        nodeDecisionType = Decision.Type.readFrom(in);
        betterWeightThanCurrent = in.readBoolean();
        delta = in.readFloat();
        deltaAboveThreshold = in.readBoolean();
        weightWithShardAdded = in.readFloat();
        deltaWithShardAdded = in.readFloat();
    }

    @Override
    public Decision.Type getFinalDecisionType() {
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
     * Returns the weight of the node if the shard is added to the node.
     */
    public float getWeightWithShardAdded() {
        return weightWithShardAdded;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("better_weight_than_current", betterWeightThanCurrent);
        if (delta != Float.POSITIVE_INFINITY) {
            builder.field("delta", delta);
            builder.field("delta_above_threshold", deltaAboveThreshold);
        }
        if (weightWithShardAdded != Float.POSITIVE_INFINITY) {
            builder.field("weight_with_shard_added", weightWithShardAdded);
        }
        if (deltaWithShardAdded != Float.POSITIVE_INFINITY) {
            builder.field("delta_with_shard_added", deltaWithShardAdded);
        }
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        Decision.Type.writeTo(nodeDecisionType, out);
        out.writeBoolean(betterWeightThanCurrent);
        out.writeFloat(delta);
        out.writeBoolean(deltaAboveThreshold);
        out.writeFloat(weightWithShardAdded);
        out.writeFloat(deltaWithShardAdded);
    }
}
