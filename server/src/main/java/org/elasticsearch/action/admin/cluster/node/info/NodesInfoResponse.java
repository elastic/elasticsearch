/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.ingest.IngestInfo;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.os.OsInfo;
import org.elasticsearch.monitor.process.ProcessInfo;
import org.elasticsearch.search.aggregations.support.AggregationInfo;
import org.elasticsearch.threadpool.ThreadPoolInfo;
import org.elasticsearch.transport.RemoteClusterServerInfo;
import org.elasticsearch.transport.TransportInfo;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class NodesInfoResponse extends BaseNodesResponse<NodeInfo> implements ToXContentFragment {

    public NodesInfoResponse(StreamInput in) throws IOException {
        super(in);
    }

    public NodesInfoResponse(ClusterName clusterName, List<NodeInfo> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<NodeInfo> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(NodeInfo::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<NodeInfo> nodes) throws IOException {
        out.writeList(nodes);
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
            builder.field("transport_version", nodeInfo.getTransportVersion().id());
            // flavor no longer exists, but we keep it here for backcompat
            builder.field("build_flavor", "default");
            builder.field("build_type", nodeInfo.getBuild().type().displayName());
            builder.field("build_hash", nodeInfo.getBuild().hash());
            if (nodeInfo.getTotalIndexingBuffer() != null) {
                builder.humanReadableField("total_indexing_buffer", "total_indexing_buffer_in_bytes", nodeInfo.getTotalIndexingBuffer());
            }

            builder.startArray("roles");
            for (DiscoveryNodeRole role : nodeInfo.getNode().getRoles()) {
                builder.value(role.roleName());
            }
            builder.endArray();

            if (nodeInfo.getNode().getAttributes().isEmpty() == false) {
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

            if (nodeInfo.getInfo(OsInfo.class) != null) {
                nodeInfo.getInfo(OsInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ProcessInfo.class) != null) {
                nodeInfo.getInfo(ProcessInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(JvmInfo.class) != null) {
                nodeInfo.getInfo(JvmInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(ThreadPoolInfo.class) != null) {
                nodeInfo.getInfo(ThreadPoolInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(TransportInfo.class) != null) {
                nodeInfo.getInfo(TransportInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(HttpInfo.class) != null) {
                nodeInfo.getInfo(HttpInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(RemoteClusterServerInfo.class) != null) {
                nodeInfo.getInfo(RemoteClusterServerInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(PluginsAndModules.class) != null) {
                nodeInfo.getInfo(PluginsAndModules.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(IngestInfo.class) != null) {
                nodeInfo.getInfo(IngestInfo.class).toXContent(builder, params);
            }
            if (nodeInfo.getInfo(AggregationInfo.class) != null) {
                nodeInfo.getInfo(AggregationInfo.class).toXContent(builder, params);
            }

            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
