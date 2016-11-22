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
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.Nullable;

import java.util.Collections;
import java.util.Map;

/**
 * Represents a decision to move a started shard to form a more optimally balanced cluster.
 */
public final class RebalanceDecision extends RelocationDecision {
    /** a constant representing no decision taken */
    public static final RebalanceDecision NOT_TAKEN = new RebalanceDecision(null, null, null, null, null, Float.POSITIVE_INFINITY);

    @Nullable
    private final Decision canRebalanceDecision;
    @Nullable
    private final Map<String, NodeRebalanceResult> nodeDecisions;
    private float currentWeight;

    public RebalanceDecision(Decision canRebalanceDecision, Type finalDecision, String finalExplanation) {
        this(canRebalanceDecision, finalDecision, finalExplanation, null, null, Float.POSITIVE_INFINITY);
    }

    public RebalanceDecision(Decision canRebalanceDecision, Type finalDecision, String finalExplanation,
                             String assignedNodeId, Map<String, NodeRebalanceResult> nodeDecisions, float currentWeight) {
        super(finalDecision, finalExplanation, assignedNodeId);
        this.canRebalanceDecision = canRebalanceDecision;
        this.nodeDecisions = nodeDecisions != null ? Collections.unmodifiableMap(nodeDecisions) : null;
        this.currentWeight = currentWeight;
    }

    /**
     * Creates a new {@link RebalanceDecision}, computing the explanation based on the decision parameters.
     */
    public static RebalanceDecision decision(Decision canRebalanceDecision, Type finalDecision, String assignedNodeId,
                                             Map<String, NodeRebalanceResult> nodeDecisions, float currentWeight, float threshold) {
        final String explanation = produceFinalExplanation(finalDecision, assignedNodeId, threshold);
        return new RebalanceDecision(canRebalanceDecision, finalDecision, explanation, assignedNodeId, nodeDecisions, currentWeight);
    }

    /**
     * Returns the decision for being allowed to rebalance the shard.
     */
    @Nullable
    public Decision getCanRebalanceDecision() {
        return canRebalanceDecision;
    }

    /**
     * Gets the individual node-level decisions that went into making the final decision as represented by
     * {@link #getFinalDecisionType()}.  The map that is returned has the node id as the key and a {@link NodeRebalanceResult}.
     */
    @Nullable
    public Map<String, NodeRebalanceResult> getNodeDecisions() {
        return nodeDecisions;
    }

    private static String produceFinalExplanation(final Type finalDecisionType, final String assignedNodeId, final float threshold) {
        final String finalExplanation;
        if (assignedNodeId != null) {
            if (finalDecisionType == Type.THROTTLE) {
                finalExplanation = "throttle moving shard to node [" + assignedNodeId + "], as it is " +
                                       "currently busy with other shard relocations";
            } else {
                finalExplanation = "moving shard to node [" + assignedNodeId + "] to form a more balanced cluster";
            }
        } else {
            finalExplanation = "cannot rebalance shard, no other node exists that would form a more balanced " +
                                   "cluster within the defined threshold [" + threshold + "]";
        }
        return finalExplanation;
    }
}
