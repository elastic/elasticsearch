/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class GeoIpDownloaderStatsAction {

    public static final ActionType<Response> INSTANCE = new ActionType<>("cluster:monitor/ingest/geoip/stats");

    private GeoIpDownloaderStatsAction() {/* no instances */}

    public static class Request extends BaseNodesRequest<Request> implements ToXContentObject {

        public Request() {
            super((String[]) null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            // Nothing to hash atm, so just use the action name
            return Objects.hashCode(INSTANCE.name());
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            return true;
        }

        @Override
        public void writeTo(StreamOutput out) {
            TransportAction.localOnly();
        }
    }

    public static class NodeRequest extends TransportRequest {
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        public NodeRequest() {}
    }

    public static class Response extends BaseNodesResponse<NodeResponse> implements Writeable, ToXContentObject {
        public Response(StreamInput in) throws IOException {
            super(in);
        }

        public Response(ClusterName clusterName, List<NodeResponse> nodes, List<FailedNodeException> failures) {
            super(clusterName, nodes, failures);
        }

        public GeoIpDownloaderStats getStats() {
            return getNodes().stream().map(n -> n.stats).filter(Objects::nonNull).findFirst().orElse(GeoIpDownloaderStats.EMPTY);
        }

        @Override
        protected List<NodeResponse> readNodesFrom(StreamInput in) throws IOException {
            return in.readCollectionAsList(NodeResponse::new);
        }

        @Override
        protected void writeNodesTo(StreamOutput out, List<NodeResponse> nodes) throws IOException {
            out.writeCollection(nodes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            GeoIpDownloaderStats stats = getStats();
            builder.startObject();
            builder.field("stats", stats);
            builder.startObject("nodes");
            for (Map.Entry<String, NodeResponse> e : getNodesMap().entrySet()) {
                NodeResponse response = e.getValue();
                if (response.filesInTemp.isEmpty() && response.databases.isEmpty() && response.configDatabases.isEmpty()) {
                    continue;
                }
                builder.startObject(e.getKey());
                if (response.databases.isEmpty() == false) {
                    builder.startArray("databases");
                    for (String database : response.databases) {
                        builder.startObject();
                        builder.field("name", database);
                        builder.endObject();
                    }
                    builder.endArray();
                }
                if (response.filesInTemp.isEmpty() == false) {
                    builder.array("files_in_temp", response.filesInTemp.toArray(String[]::new));
                }
                if (response.configDatabases.isEmpty() == false) {
                    builder.array("config_databases", response.configDatabases.toArray(String[]::new));
                }
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response that = (Response) o;
            return Objects.equals(getNodes(), that.getNodes()) && Objects.equals(failures(), that.failures());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getNodes(), failures());
        }
    }

    public static class NodeResponse extends BaseNodeResponse {

        private final GeoIpDownloaderStats stats;
        private final Set<String> databases;
        private final Set<String> filesInTemp;
        private final Set<String> configDatabases;

        protected NodeResponse(StreamInput in) throws IOException {
            super(in);
            stats = in.readBoolean() ? new GeoIpDownloaderStats(in) : null;
            databases = in.readCollectionAsImmutableSet(StreamInput::readString);
            filesInTemp = in.readCollectionAsImmutableSet(StreamInput::readString);
            configDatabases = in.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)
                ? in.readCollectionAsImmutableSet(StreamInput::readString)
                : null;
        }

        protected NodeResponse(
            DiscoveryNode node,
            GeoIpDownloaderStats stats,
            Set<String> databases,
            Set<String> filesInTemp,
            Set<String> configDatabases
        ) {
            super(node);
            this.stats = stats;
            this.databases = Set.copyOf(databases);
            this.filesInTemp = Set.copyOf(filesInTemp);
            this.configDatabases = Set.copyOf(configDatabases);
        }

        public GeoIpDownloaderStats getStats() {
            return stats;
        }

        public Set<String> getDatabases() {
            return databases;
        }

        public Set<String> getFilesInTemp() {
            return filesInTemp;
        }

        public Set<String> getConfigDatabases() {
            return configDatabases;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(stats != null);
            if (stats != null) {
                stats.writeTo(out);
            }
            out.writeStringCollection(databases);
            out.writeStringCollection(filesInTemp);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
                out.writeStringCollection(configDatabases);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeResponse that = (NodeResponse) o;
            return stats.equals(that.stats)
                && databases.equals(that.databases)
                && filesInTemp.equals(that.filesInTemp)
                && Objects.equals(configDatabases, that.configDatabases);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stats, databases, filesInTemp, configDatabases);
        }
    }
}
