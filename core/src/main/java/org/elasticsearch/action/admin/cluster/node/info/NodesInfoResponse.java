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

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class NodesInfoResponse extends BaseNodesResponse<NodeInfo> implements ToXContent {

    public NodesInfoResponse() {
    }

    public NodesInfoResponse(ClusterName clusterName, List<NodeInfo> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeInfo> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeInfo::readNodeInfo);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeInfo> nodes) throws IOException {
        out.writeStreamableList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        for (NodeInfo nodeInfo : getNodes()) {
            builder.startObject(nodeInfo.getNode().getId());

            builder.field("name", nodeInfo.getNode().getName());
            builder.field("transport_address", nodeInfo.getNode().getAddress().toString());
            builder.field("host", nodeInfo.getNode().getHostName());
            builder.field("ip", nodeInfo.getNode().getHostAddress());

            builder.field("version", nodeInfo.getVersion());
            builder.field("build_hash", nodeInfo.getBuild().shortHash());

            if (nodeInfo.getServiceAttributes() != null) {
                for (Map.Entry<String, String> nodeAttribute : nodeInfo.getServiceAttributes().entrySet()) {
                    builder.field(nodeAttribute.getKey(), nodeAttribute.getValue());
                }
            }

            builder.startArray("roles");
            for (DiscoveryNode.Role role : nodeInfo.getNode().getRoles()) {
                builder.value(role.getRoleName());
            }
            builder.endArray();

            if (!nodeInfo.getNode().getAttributes().isEmpty()) {
                builder.startObject("attributes");
                for (Map.Entry<String, String> entry : nodeInfo.getNode().getAttributes().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }

            if (nodeInfo.getSettings() != null) {
                builder.startObject("settings");
                Settings settings = nodeInfo.getSettings();
                settings.toXContent(builder, params);
                builder.endObject();
            }

            if (nodeInfo.getOs() != null) {
                nodeInfo.getOs().toXContent(builder, params);
            }
            if (nodeInfo.getProcess() != null) {
                nodeInfo.getProcess().toXContent(builder, params);
            }
            if (nodeInfo.getJvm() != null) {
                nodeInfo.getJvm().toXContent(builder, params);
            }
            if (nodeInfo.getThreadPool() != null) {
                nodeInfo.getThreadPool().toXContent(builder, params);
            }
            if (nodeInfo.getTransport() != null) {
                nodeInfo.getTransport().toXContent(builder, params);
            }
            if (nodeInfo.getHttp() != null) {
                nodeInfo.getHttp().toXContent(builder, params);
            }
            if (nodeInfo.getPlugins() != null) {
                nodeInfo.getPlugins().toXContent(builder, params);
            }
            if (nodeInfo.getIngest() != null) {
                nodeInfo.getIngest().toXContent(builder, params);
            }

            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
