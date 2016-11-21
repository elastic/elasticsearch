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

import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import java.util.Objects;

/**
 * A node-level explanation for the decision to rebalance a shard.
 */
public final class NodeRebalanceResult {
    private final Decision.Type nodeDecisionType;
    private final Decision canAllocate;
    private final boolean betterWeightThanCurrent;
    private final boolean deltaAboveThreshold;
    private final float currentWeight;
    private final float weightWithShardAdded;

    public NodeRebalanceResult(Decision.Type nodeDecisionType, Decision canAllocate, boolean betterWeightThanCurrent,
                               boolean deltaAboveThreshold, float currentWeight, float weightWithShardAdded) {
        this.nodeDecisionType = Objects.requireNonNull(nodeDecisionType);
        this.canAllocate = Objects.requireNonNull(canAllocate);
        this.betterWeightThanCurrent = betterWeightThanCurrent;
        this.deltaAboveThreshold = deltaAboveThreshold;
        this.currentWeight = currentWeight;
        this.weightWithShardAdded = weightWithShardAdded;
    }

    /**
     * Returns the decision to rebalance to the node.
     */
    public Decision.Type getNodeDecisionType() {
        return nodeDecisionType;
    }

    /**
     * Returns whether the shard is allowed to be allocated to the node.
     */
    public Decision getCanAllocateDecision() {
        return canAllocate;
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
     * Returns the current weight of the node if the shard is not added to the node.
     */
    public float getCurrentWeight() {
        return currentWeight;
    }

    /**
     * Returns the weight of the node if the shard is added to the node.
     */
    public float getWeightWithShardAdded() {
        return weightWithShardAdded;
    }
}
