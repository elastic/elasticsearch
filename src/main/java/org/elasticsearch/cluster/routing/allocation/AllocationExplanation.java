/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class AllocationExplanation implements Streamable {

    public static final AllocationExplanation EMPTY = new AllocationExplanation();

    public static class NodeExplanation {
        private final DiscoveryNode node;

        private final String description;

        public NodeExplanation(DiscoveryNode node, String description) {
            this.node = node;
            this.description = description;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String description() {
            return description;
        }
    }

    private final Map<ShardId, List<NodeExplanation>> explanations = Maps.newHashMap();

    public AllocationExplanation add(ShardId shardId, NodeExplanation nodeExplanation) {
        List<NodeExplanation> list = explanations.get(shardId);
        if (list == null) {
            list = Lists.newArrayList();
            explanations.put(shardId, list);
        }
        list.add(nodeExplanation);
        return this;
    }

    public Map<ShardId, List<NodeExplanation>> explanations() {
        return this.explanations;
    }

    public static AllocationExplanation readAllocationExplanation(StreamInput in) throws IOException {
        AllocationExplanation e = new AllocationExplanation();
        e.readFrom(in);
        return e;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            ShardId shardId = ShardId.readShardId(in);
            int size2 = in.readVInt();
            List<NodeExplanation> ne = Lists.newArrayListWithCapacity(size2);
            for (int j = 0; j < size2; j++) {
                DiscoveryNode node = null;
                if (in.readBoolean()) {
                    node = DiscoveryNode.readNode(in);
                }
                ne.add(new NodeExplanation(node, in.readUTF()));
            }
            explanations.put(shardId, ne);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(explanations.size());
        for (Map.Entry<ShardId, List<NodeExplanation>> entry : explanations.entrySet()) {
            entry.getKey().writeTo(out);
            out.writeVInt(entry.getValue().size());
            for (NodeExplanation nodeExplanation : entry.getValue()) {
                if (nodeExplanation.node() == null) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    nodeExplanation.node().writeTo(out);
                }
                out.writeUTF(nodeExplanation.description());
            }
        }
    }
}
