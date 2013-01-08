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

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class NodesInfoResponse extends NodesOperationResponse<NodeInfo> implements ToXContent {

    private SettingsFilter settingsFilter;

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

    public NodesInfoResponse settingsFilter(SettingsFilter settingsFilter) {
        this.settingsFilter = settingsFilter;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("cluster_name", clusterName().value());

        builder.startObject("nodes");
        for (NodeInfo nodeInfo : this) {
            builder.startObject(nodeInfo.node().id(), XContentBuilder.FieldCaseConversion.NONE);

            builder.field("name", nodeInfo.node().name(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field("transport_address", nodeInfo.node().address().toString());

            if (nodeInfo.hostname() != null) {
                builder.field("hostname", nodeInfo.hostname(), XContentBuilder.FieldCaseConversion.NONE);
            }

            if (nodeInfo.version() != null) {
                builder.field("version", nodeInfo.version());
            }

            if (nodeInfo.serviceAttributes() != null) {
                for (Map.Entry<String, String> nodeAttribute : nodeInfo.serviceAttributes().entrySet()) {
                    builder.field(nodeAttribute.getKey(), nodeAttribute.getValue());
                }
            }

            if (!nodeInfo.node().attributes().isEmpty()) {
                builder.startObject("attributes");
                for (Map.Entry<String, String> attr : nodeInfo.node().attributes().entrySet()) {
                    builder.field(attr.getKey(), attr.getValue());
                }
                builder.endObject();
            }


            if (nodeInfo.settings() != null) {
                builder.startObject("settings");
                Settings settings = settingsFilter.filterSettings(nodeInfo.settings());
                for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }

            if (nodeInfo.os() != null) {
                nodeInfo.os().toXContent(builder, params);
            }
            if (nodeInfo.process() != null) {
                nodeInfo.process().toXContent(builder, params);
            }
            if (nodeInfo.jvm() != null) {
                nodeInfo.jvm().toXContent(builder, params);
            }
            if (nodeInfo.threadPool() != null) {
                nodeInfo.threadPool().toXContent(builder, params);
            }
            if (nodeInfo.network() != null) {
                nodeInfo.network().toXContent(builder, params);
            }
            if (nodeInfo.transport() != null) {
                nodeInfo.transport().toXContent(builder, params);
            }
            if (nodeInfo.http() != null) {
                nodeInfo.http().toXContent(builder, params);
            }

            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
