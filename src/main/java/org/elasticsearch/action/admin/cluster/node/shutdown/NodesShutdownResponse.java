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

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public class NodesShutdownResponse extends ActionResponse {

    private ClusterName clusterName;
    private DiscoveryNode[] nodes;

    NodesShutdownResponse() {
    }

    public NodesShutdownResponse(ClusterName clusterName, DiscoveryNode[] nodes) {
        this.clusterName = clusterName;
        this.nodes = nodes;
    }

    public ClusterName getClusterName() {
        return this.clusterName;
    }

    public DiscoveryNode[] getNodes() {
        return this.nodes;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        clusterName = ClusterName.readClusterName(in);
        nodes = new DiscoveryNode[in.readVInt()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = DiscoveryNode.readNode(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        clusterName.writeTo(out);
        out.writeVInt(nodes.length);
        for (DiscoveryNode node : nodes) {
            node.writeTo(out);
        }
    }
}
