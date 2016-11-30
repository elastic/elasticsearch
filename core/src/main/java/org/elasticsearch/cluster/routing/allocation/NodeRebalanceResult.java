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
    private final boolean deltaAboveThreshold;
    private final boolean betterBalance;

    public NodeRebalanceResult(DiscoveryNode node, Decision.Type nodeDecisionType, Decision canAllocate, int weightRanking,
                               boolean deltaAboveThreshold, boolean betterBalance) {
        super(node, canAllocate, weightRanking);
        this.nodeDecisionType = Objects.requireNonNull(nodeDecisionType);
        this.deltaAboveThreshold = deltaAboveThreshold;
        this.betterBalance = betterBalance;
    }

    public NodeRebalanceResult(StreamInput in) throws IOException {
        super(in);
        nodeDecisionType = Decision.Type.readFrom(in);
        deltaAboveThreshold = in.readBoolean();
        betterBalance = in.readBoolean();
    }

    @Override
    public Decision.Type getNodeDecisionType() {
        return nodeDecisionType;
    }

    /**
     * Returns if the weight delta by assigning to this node was above the threshold to warrant a rebalance.
     */
    public boolean isDeltaAboveThreshold() {
        return deltaAboveThreshold;
    }

    /**
     * Returns {@code true} if moving the shard to this node will provide a better balance to the cluster,
     * as long as the balance differential is above the threshold to warrant moving the shard.  Returns
     * {@code false} if {@link #isDeltaAboveThreshold()} returns {@code false} or if moving the shard to
     * the node does not result in a more balanced cluster.
     */
    public boolean isBetterBalance() {
        return betterBalance;
    }

    @Override
    protected void innerToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("delta_above_threshold", deltaAboveThreshold);
        if (deltaAboveThreshold) {
            builder.field("better_balance", betterBalance);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        nodeDecisionType.writeTo(out);
        out.writeBoolean(deltaAboveThreshold);
        out.writeBoolean(betterBalance);
    }
}
