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

package org.elasticsearch.action.admin.cluster.node.restart;

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 *
 */
public class NodesRestartResponse extends NodesOperationResponse<NodesRestartResponse.NodeRestartResponse> {

    NodesRestartResponse() {
    }

    public NodesRestartResponse(ClusterName clusterName, NodeRestartResponse[] nodes) {
        super(clusterName, nodes);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodes = new NodeRestartResponse[in.readVInt()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeRestartResponse.readNodeRestartResponse(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(nodes.length);
        for (NodeRestartResponse node : nodes) {
            node.writeTo(out);
        }
    }

    public static class NodeRestartResponse extends NodeOperationResponse {

        NodeRestartResponse() {
        }

        public NodeRestartResponse(DiscoveryNode node) {
            super(node);
        }

        public static NodeRestartResponse readNodeRestartResponse(StreamInput in) throws IOException {
            NodeRestartResponse res = new NodeRestartResponse();
            res.readFrom(in);
            return res;
        }
    }
}