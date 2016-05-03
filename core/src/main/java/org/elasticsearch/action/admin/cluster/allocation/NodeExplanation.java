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

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
/** The cluster allocation explanation for a single node */
public class NodeExplanation implements Writeable, ToXContent {
    private final DiscoveryNode node;
    private final Decision nodeDecision;
    private final Float nodeWeight;
    private final IndicesShardStoresResponse.StoreStatus storeStatus;
    private final ClusterAllocationExplanation.FinalDecision finalDecision;
    private final ClusterAllocationExplanation.StoreCopy storeCopy;
    private final String finalExplanation;

    public NodeExplanation(final DiscoveryNode node, final Decision nodeDecision, final Float nodeWeight,
                           final @Nullable IndicesShardStoresResponse.StoreStatus storeStatus,
                           final ClusterAllocationExplanation.FinalDecision finalDecision,
                           final String finalExplanation,
                           final ClusterAllocationExplanation.StoreCopy storeCopy) {
        this.node = node;
        this.nodeDecision = nodeDecision;
        this.nodeWeight = nodeWeight;
        this.storeStatus = storeStatus;
        this.finalDecision = finalDecision;
        this.finalExplanation = finalExplanation;
        this.storeCopy = storeCopy;
    }

    public NodeExplanation(StreamInput in) throws IOException {
        this.node = new DiscoveryNode(in);
        this.nodeDecision = Decision.readFrom(in);
        this.nodeWeight = in.readFloat();
        if (in.readBoolean()) {
            this.storeStatus = IndicesShardStoresResponse.StoreStatus.readStoreStatus(in);
        } else {
            this.storeStatus = null;
        }
        this.finalDecision = ClusterAllocationExplanation.FinalDecision.readFrom(in);
        this.finalExplanation = in.readString();
        this.storeCopy = ClusterAllocationExplanation.StoreCopy.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        Decision.writeTo(nodeDecision, out);
        out.writeFloat(nodeWeight);
        if (storeStatus == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            storeStatus.writeTo(out);
        }
        finalDecision.writeTo(out);
        out.writeString(finalExplanation);
        storeCopy.writeTo(out);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(node.getId()); {
            builder.field("node_name", node.getName());
            builder.startObject("node_attributes"); {
                for (Map.Entry<String, String> attrEntry : node.getAttributes().entrySet()) {
                    builder.field(attrEntry.getKey(), attrEntry.getValue());
                }
            }
            builder.endObject(); // end attributes
            builder.startObject("store"); {
                builder.field("shard_copy", storeCopy.toString());
                if (storeStatus != null) {
                    final Throwable storeErr = storeStatus.getStoreException();
                    if (storeErr != null) {
                        builder.field("store_exception", ExceptionsHelper.detailedMessage(storeErr));
                    }
                }
            }
            builder.endObject(); // end store
            builder.field("final_decision", finalDecision.toString());
            builder.field("final_explanation", finalExplanation.toString());
            builder.field("weight", nodeWeight);
            nodeDecision.toXContent(builder, params);
        }
        builder.endObject(); // end node <uuid>
        return builder;
    }

    public DiscoveryNode getNode() {
        return this.node;
    }

    public Decision getDecision() {
        return this.nodeDecision;
    }

    public Float getWeight() {
        return this.nodeWeight;
    }

    @Nullable
    public IndicesShardStoresResponse.StoreStatus getStoreStatus() {
        return this.storeStatus;
    }

    public ClusterAllocationExplanation.FinalDecision getFinalDecision() {
        return this.finalDecision;
    }

    public String getFinalExplanation() {
        return this.finalExplanation;
    }

    public ClusterAllocationExplanation.StoreCopy getStoreCopy() {
        return this.storeCopy;
    }
}
