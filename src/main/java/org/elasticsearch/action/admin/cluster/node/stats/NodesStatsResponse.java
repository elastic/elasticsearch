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

package org.elasticsearch.action.admin.cluster.node.stats;

import org.elasticsearch.action.support.nodes.NodesOperationResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class NodesStatsResponse extends NodesOperationResponse<NodeStats> implements ToXContent {

    NodesStatsResponse() {
    }

    public NodesStatsResponse(ClusterName clusterName, NodeStats[] nodes) {
        super(clusterName, nodes);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        nodes = new NodeStats[in.readVInt()];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = NodeStats.readNodeStats(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(nodes.length);
        for (NodeStats node : nodes) {
            node.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("cluster_name", getClusterName().value());

        builder.startObject("nodes");
        for (NodeStats nodeStats : this) {
            builder.startObject(nodeStats.getNode().id(), XContentBuilder.FieldCaseConversion.NONE);

            builder.field("timestamp", nodeStats.getTimestamp());
            builder.field("name", nodeStats.getNode().name(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field("transport_address", nodeStats.getNode().address().toString());

            if (nodeStats.getHostname() != null) {
                builder.field("hostname", nodeStats.getHostname(), XContentBuilder.FieldCaseConversion.NONE);
            }

            if (!nodeStats.getNode().attributes().isEmpty()) {
                builder.startObject("attributes");
                for (Map.Entry<String, String> attr : nodeStats.getNode().attributes().entrySet()) {
                    builder.field(attr.getKey(), attr.getValue());
                }
                builder.endObject();
            }

            if (nodeStats.getIndices() != null) {
                nodeStats.getIndices().toXContent(builder, params);
            }

            if (nodeStats.getOs() != null) {
                nodeStats.getOs().toXContent(builder, params);
            }
            if (nodeStats.getProcess() != null) {
                nodeStats.getProcess().toXContent(builder, params);
            }
            if (nodeStats.getJvm() != null) {
                nodeStats.getJvm().toXContent(builder, params);
            }
            if (nodeStats.getThreadPool() != null) {
                nodeStats.getThreadPool().toXContent(builder, params);
            }
            if (nodeStats.getNetwork() != null) {
                nodeStats.getNetwork().toXContent(builder, params);
            }
            if (nodeStats.getFs() != null) {
                nodeStats.getFs().toXContent(builder, params);
            }
            if (nodeStats.getTransport() != null) {
                nodeStats.getTransport().toXContent(builder, params);
            }
            if (nodeStats.getHttp() != null) {
                nodeStats.getHttp().toXContent(builder, params);
            }

            builder.endObject();
        }
        builder.endObject();

        return builder;
    }
}