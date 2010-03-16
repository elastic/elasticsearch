/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

import org.elasticsearch.action.support.nodes.NodeOperationResponse;
import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.Node;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
public class NodesShutdownResponse extends NodesOperationResponse<NodesShutdownResponse.NodeShutdownResponse> {

    NodesShutdownResponse() {
    }

    public NodesShutdownResponse(ClusterName clusterName, NodeShutdownResponse[] nodes) {
        super(clusterName, nodes);
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        nodes = new NodeShutdownResponse[in.readInt()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeShutdownResponse.readNodeShutdownResponse(in);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(nodes.length);
        for (NodeShutdownResponse node : nodes) {
            node.writeTo(out);
        }
    }

    public static class NodeShutdownResponse extends NodeOperationResponse {

        NodeShutdownResponse() {
        }

        public NodeShutdownResponse(Node node) {
            super(node);
        }

        public static NodeShutdownResponse readNodeShutdownResponse(DataInput in) throws ClassNotFoundException, IOException {
            NodeShutdownResponse res = new NodeShutdownResponse();
            res.readFrom(in);
            return res;
        }
    }
}