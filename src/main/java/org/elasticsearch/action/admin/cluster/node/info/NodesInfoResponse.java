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

import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class NodesInfoResponse extends NodesOperationResponse<NodeInfo> implements ToXContent {

    public NodesInfoResponse() {
    }

    public NodesInfoResponse(ClusterName clusterName, NodeInfo[] nodes) {
        super(clusterName, nodes);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodes = new NodeInfo[in.readVInt()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeInfo.readNodeInfo(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(nodes.length);
        for (NodeInfo node : nodes) {
            node.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("cluster_name", getClusterName().value(), XContentBuilder.FieldCaseConversion.NONE);

        builder.startObject("nodes");
        for (NodeInfo nodeInfo : this) {
            builder.startObject(nodeInfo.getNode().id(), XContentBuilder.FieldCaseConversion.NONE);

            builder.field("name", nodeInfo.getNode().name(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field("transport_address", nodeInfo.getNode().address().toString());
            builder.field("host", nodeInfo.getNode().getHostName(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field("ip", nodeInfo.getNode().getHostAddress(), XContentBuilder.FieldCaseConversion.NONE);

            builder.field("version", nodeInfo.getVersion());
            builder.field("build", nodeInfo.getBuild().hashShort());

            if (nodeInfo.getServiceAttributes() != null) {
                for (Map.Entry<String, String> nodeAttribute : nodeInfo.getServiceAttributes().entrySet()) {
                    builder.field(nodeAttribute.getKey(), nodeAttribute.getValue(), XContentBuilder.FieldCaseConversion.NONE);
                }
            }

            if (!nodeInfo.getNode().attributes().isEmpty()) {
                builder.startObject("attributes");
                for (Map.Entry<String, String> attr : nodeInfo.getNode().attributes().entrySet()) {
                    builder.field(attr.getKey(), attr.getValue(), XContentBuilder.FieldCaseConversion.NONE);
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
            if (nodeInfo.getNetwork() != null) {
                nodeInfo.getNetwork().toXContent(builder, params);
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
