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
import org.elasticsearch.common.Nullable;

/**
 * Represents a decision to relocate a started shard from its current node.
 */
public abstract class RelocationDecision {
    @Nullable
    private final Decision.Type finalDecision;
    @Nullable
    private final String finalExplanation;
    @Nullable
    private final String assignedNodeId;

    protected RelocationDecision(Decision.Type finalDecision, String finalExplanation, String assignedNodeId) {
        this.finalDecision = finalDecision;
        this.finalExplanation = finalExplanation;
        this.assignedNodeId = assignedNodeId;
    }

    /**
     * Returns {@code true} if a decision was taken by the allocator, {@code false} otherwise.
     * If no decision was taken, then the rest of the fields in this object are meaningless and return {@code null}.
     */
    public boolean isDecisionTaken() {
        return finalDecision != null;
    }

    /**
     * Returns the final decision made by the allocator on whether to assign the shard, and
     * {@code null} if no decision was taken.
     */
    public Decision.Type getFinalDecisionType() {
        return finalDecision;
    }

    /**
     * Returns the free-text explanation for the reason behind the decision taken in {@link #getFinalDecisionType()}.
     */
    @Nullable
    public String getFinalExplanation() {
        return finalExplanation;
    }

    /**
     * Get the node id that the allocator will assign the shard to, unless {@link #getFinalDecisionType()} returns
     * a value other than {@link Decision.Type#YES}, in which case this returns {@code null}.
     */
    @Nullable
    public String getAssignedNodeId() {
        return assignedNodeId;
    }
}
