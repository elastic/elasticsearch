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
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Represents a decision to move a started shard to form a more optimally balanced cluster.
 */
public final class RebalanceDecision extends RelocationDecision {
    /** a constant representing no decision taken */
    public static final RebalanceDecision NOT_TAKEN = new RebalanceDecision(null, null, null, null, Float.POSITIVE_INFINITY, false);

    @Nullable
    private final Decision canRebalanceDecision;
    @Nullable
    private final Map<String, NodeRebalanceResult> nodeDecisions;
    private final float currentWeight;
    private final boolean fetchPending;

    private RebalanceDecision(Decision canRebalanceDecision, Type finalDecision, DiscoveryNode assignedNode,
                              Map<String, NodeRebalanceResult> nodeDecisions, float currentWeight, boolean fetchPending) {
        super(finalDecision, assignedNode);
        this.canRebalanceDecision = canRebalanceDecision;
        this.nodeDecisions = nodeDecisions != null ? Collections.unmodifiableMap(nodeDecisions) : null;
        this.currentWeight = currentWeight;
        this.fetchPending = fetchPending;
    }

    public RebalanceDecision(StreamInput in) throws IOException {
        super(in);
        canRebalanceDecision = in.readOptionalWriteable(Decision::readFrom);
        Map<String, NodeRebalanceResult> nodeDecisionsMap = null;
        if (in.readBoolean()) {
            nodeDecisionsMap = in.readMap(StreamInput::readString, NodeRebalanceResult::new);
        }
        nodeDecisions = nodeDecisionsMap == null ? null : Collections.unmodifiableMap(nodeDecisionsMap);
        currentWeight = in.readFloat();
        fetchPending = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(canRebalanceDecision);
        if (nodeDecisions != null) {
            out.writeBoolean(true);
            out.writeVInt(nodeDecisions.size());
            for (Map.Entry<String, NodeRebalanceResult> entry : nodeDecisions.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        } else {
            out.writeBoolean(false);
        }
        out.writeFloat(currentWeight);
        out.writeBoolean(fetchPending);
    }

    /**
     * Creates a new NO {@link RebalanceDecision}.
     */
    public static RebalanceDecision no(Decision canRebalanceDecision, Map<String, NodeRebalanceResult> nodeDecisions,
                                       float currentWeight, boolean fetchPending) {
        return new RebalanceDecision(canRebalanceDecision, Type.NO, null, nodeDecisions, currentWeight, fetchPending);
    }

    /**
     * Creates a new {@link RebalanceDecision}.
     */
    public static RebalanceDecision decision(Decision canRebalanceDecision, Type finalDecision, DiscoveryNode assignedNode,
                                             Map<String, NodeRebalanceResult> nodeDecisions, float currentWeight) {
        return new RebalanceDecision(canRebalanceDecision, finalDecision, assignedNode, nodeDecisions, currentWeight, false);
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

    @Override
    public String getExplanation() {
        String explanation;
        if (fetchPending) {
            explanation = "cannot rebalance as information about existing copies of this shard in the cluster is still being gathered";
        } else if (canRebalanceDecision.type() != Type.YES) {
            explanation = "rebalancing is not allowed";
        } else {
            if (getAssignedNode() != null) {
                if (getFinalDecisionType() == Type.THROTTLE) {
                    explanation = "shard rebalancing throttled";
                } else {
                    explanation = "can rebalance shard";
                }
            } else {
                explanation = "cannot rebalance as no target node node exists that can both allocate this shard " +
                              "and improve the cluster balance";
            }
        }
        return explanation;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        super.toXContent(builder, params);
        builder.field("current_weight", currentWeight);
        builder.startObject("can_rebalance_decision");
        {
            builder.field("decision", canRebalanceDecision.type().toString());
            canRebalanceDecision.toXContent(builder, params);
        }
        builder.endObject();
        if (nodeDecisions != null) {
            builder.startObject("node_decisions");
            {
                List<String> nodeIds = new ArrayList<>(nodeDecisions.keySet());
                Collections.sort(nodeIds);
                for (String nodeId : nodeIds) {
                    NodeRebalanceResult result = nodeDecisions.get(nodeId);
                    result.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        return builder;
    }
}
