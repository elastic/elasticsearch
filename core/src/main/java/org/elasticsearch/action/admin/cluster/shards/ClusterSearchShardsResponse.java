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

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 */
public class ClusterSearchShardsResponse extends ActionResponse implements ToXContent {

    private ClusterSearchShardsGroup[] groups;
    private DiscoveryNode[] nodes;

    ClusterSearchShardsResponse() {

    }

    public ClusterSearchShardsGroup[] getGroups() {
        return groups;
    }

    public DiscoveryNode[] getNodes() {
        return nodes;
    }

    public ClusterSearchShardsResponse(ClusterSearchShardsGroup[] groups, DiscoveryNode[] nodes) {
        this.groups = groups;
        this.nodes = nodes;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        groups = new ClusterSearchShardsGroup[in.readVInt()];
        for (int i = 0; i < groups.length; i++) {
            groups[i] = ClusterSearchShardsGroup.readSearchShardsGroupResponse(in);
        }
        nodes = new DiscoveryNode[in.readVInt()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = DiscoveryNode.readNode(in);
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(groups.length);
        for (ClusterSearchShardsGroup response : groups) {
            response.writeTo(out);
        }
        out.writeVInt(nodes.length);
        for (DiscoveryNode node : nodes) {
            node.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (DiscoveryNode node : nodes) {
            node.toXContent(builder, params);
        }
        builder.endObject();
        builder.startArray("shards");
        for (ClusterSearchShardsGroup group : groups) {
            group.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
