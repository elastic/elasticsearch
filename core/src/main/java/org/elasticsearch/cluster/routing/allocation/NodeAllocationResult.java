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
 * This class represents the shard allocation decision for a single node,
 * including the {@link Decision} whether to allocate to the node and other
 * information related to obtaining the decision for the node.
 */
public final class NodeAllocationResult {

    private final Decision decision;
    private final float weight;

    public NodeAllocationResult(Decision decision) {
        this.decision = Objects.requireNonNull(decision);
        this.weight = Float.POSITIVE_INFINITY;
    }

    public NodeAllocationResult(Decision decision, float weight) {
        this.decision = Objects.requireNonNull(decision);
        this.weight = Objects.requireNonNull(weight);
    }

    /**
     * The decision for allocating to the node.
     */
    public Decision getDecision() {
        return decision;
    }

    /**
     * The calculated weight for allocating a shard to the node.  A value of {@link Float#POSITIVE_INFINITY}
     * means the weight was not calculated or factored into the decision.
     */
    public float getWeight() {
        return weight;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        NodeAllocationResult that = (NodeAllocationResult) other;
        return decision.equals(that.decision) && Float.compare(weight, that.weight) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(decision, weight);
    }
}
