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
        builder.field("cluster_name", clusterName().value());

        builder.startObject("nodes");
        for (NodeStats nodeStats : this) {
            builder.startObject(nodeStats.node().id(), XContentBuilder.FieldCaseConversion.NONE);

            builder.field("timestamp", nodeStats.timestamp());
            builder.field("name", nodeStats.node().name(), XContentBuilder.FieldCaseConversion.NONE);
            builder.field("transport_address", nodeStats.node().address().toString());

            if (nodeStats.hostname() != null) {
                builder.field("hostname", nodeStats.hostname(), XContentBuilder.FieldCaseConversion.NONE);
            }

            if (!nodeStats.node().attributes().isEmpty()) {
                builder.startObject("attributes");
                for (Map.Entry<String, String> attr : nodeStats.node().attributes().entrySet()) {
                    builder.field(attr.getKey(), attr.getValue());
                }
                builder.endObject();
            }

            if (nodeStats.indices() != null) {
                nodeStats.indices().toXContent(builder, params);
            }

            if (nodeStats.os() != null) {
                nodeStats.os().toXContent(builder, params);
            }
            if (nodeStats.process() != null) {
                nodeStats.process().toXContent(builder, params);
            }
            if (nodeStats.jvm() != null) {
                nodeStats.jvm().toXContent(builder, params);
            }
            if (nodeStats.threadPool() != null) {
                nodeStats.threadPool().toXContent(builder, params);
            }
            if (nodeStats.network() != null) {
                nodeStats.network().toXContent(builder, params);
            }
            if (nodeStats.fs() != null) {
                nodeStats.fs().toXContent(builder, params);
            }
            if (nodeStats.transport() != null) {
                nodeStats.transport().toXContent(builder, params);
            }
            if (nodeStats.http() != null) {
                nodeStats.http().toXContent(builder, params);
            }

            builder.endObject();
        }
        builder.endObject();

        return builder;
    }
}