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
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class GeoIpStatsAction {

    public static final ActionType<Response> INSTANCE = new ActionType<>("cluster:monitor/ingest/geoip/stats");
    private static final Comparator<DatabaseInfo> DATABASE_INFO_COMPARATOR = Comparator.comparing(DatabaseInfo::name);

    private GeoIpStatsAction() {/* no instances */}

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

        public GeoIpDownloaderStats getDownloaderStats() {
            return getNodes().stream().map(n -> n.downloaderStats).filter(Objects::nonNull).findFirst().orElse(GeoIpDownloaderStats.EMPTY);
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
            GeoIpDownloaderStats stats = getDownloaderStats();
            builder.startObject();
            builder.field("stats", stats, params);
            builder.startObject("nodes");
            for (Map.Entry<String, NodeResponse> e : getNodesMap().entrySet()) {
                NodeResponse response = e.getValue();
                if (response.filesInTemp.isEmpty() && response.databases.isEmpty() && response.configDatabases.isEmpty()) {
                    continue;
                }
                builder.startObject(e.getKey());
                if (response.databases.isEmpty() == false) {
                    builder.startArray("databases");
                    for (DatabaseInfo database : response.databases) {
                        builder.startObject();
                        builder.field("name", database.name());
                        builder.field("source", database.source());
                        builder.field("archive_md5", database.archiveMd5());
                        builder.field("md5", database.md5());
                        builder.timeField("build_date_in_millis", "build_date", database.buildDateInMillis());
                        builder.field("type", database.type());
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
                builder.startObject("cache_stats");
                CacheStats cacheStats = response.cacheStats;
                builder.field("count", cacheStats.count());
                builder.field("hits", cacheStats.hits());
                builder.field("misses", cacheStats.misses());
                builder.field("evictions", cacheStats.evictions());
                builder.humanReadableField("hits_time_in_millis", "hits_time", new TimeValue(cacheStats.hitsTimeInMillis()));
                builder.humanReadableField("misses_time_in_millis", "misses_time", new TimeValue(cacheStats.missesTimeInMillis()));
                builder.endObject();
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

        private final GeoIpDownloaderStats downloaderStats;
        private final CacheStats cacheStats;
        private final SortedSet<DatabaseInfo> databases;
        private final Set<String> filesInTemp;
        private final Set<String> configDatabases;

        protected NodeResponse(StreamInput in) throws IOException {
            super(in);
            downloaderStats = in.readBoolean() ? new GeoIpDownloaderStats(in) : null;
            if (in.getTransportVersion().onOrAfter(TransportVersions.GEOIP_CACHE_STATS)) {
                cacheStats = in.readBoolean() ? new CacheStats(in) : null;
            } else {
                cacheStats = null;
            }
            databases = new TreeSet<>(DATABASE_INFO_COMPARATOR);
            if (in.getTransportVersion().onOrAfter(TransportVersions.GEOIP_ADDITIONAL_DATABASE_DOWNLOAD_STATS)) {
                databases.addAll(in.readCollectionAsImmutableSet(DatabaseInfo::new));
            } else {
                Set<String> databaseNames = in.readCollectionAsImmutableSet(StreamInput::readString);
                databases.addAll(
                    databaseNames.stream().map(name -> new DatabaseInfo(name, null, null, null, null, null)).collect(Collectors.toSet())
                );
            }
            filesInTemp = in.readCollectionAsImmutableSet(StreamInput::readString);
            configDatabases = in.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)
                ? in.readCollectionAsImmutableSet(StreamInput::readString)
                : null;
        }

        protected NodeResponse(
            DiscoveryNode node,
            GeoIpDownloaderStats downloaderStats,
            CacheStats cacheStats,
            Set<DatabaseInfo> databases,
            Set<String> filesInTemp,
            Set<String> configDatabases
        ) {
            super(node);
            this.downloaderStats = downloaderStats;
            this.cacheStats = cacheStats;
            this.databases = new TreeSet<>(DATABASE_INFO_COMPARATOR);
            this.databases.addAll(databases);
            this.filesInTemp = Set.copyOf(filesInTemp);
            this.configDatabases = Set.copyOf(configDatabases);
        }

        public GeoIpDownloaderStats getDownloaderStats() {
            return downloaderStats;
        }

        public Set<DatabaseInfo> getDatabases() {
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
            out.writeBoolean(downloaderStats != null);
            if (downloaderStats != null) {
                downloaderStats.writeTo(out);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.GEOIP_CACHE_STATS)) {
                out.writeBoolean(cacheStats != null);
                if (cacheStats != null) {
                    cacheStats.writeTo(out);
                }
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.GEOIP_ADDITIONAL_DATABASE_DOWNLOAD_STATS)) {
                out.writeCollection(databases);
            } else {
                out.writeStringCollection(databases.stream().map(DatabaseInfo::name).collect(Collectors.toSet()));
            }

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
            return downloaderStats.equals(that.downloaderStats)
                && Objects.equals(cacheStats, that.cacheStats)
                && databases.equals(that.databases)
                && filesInTemp.equals(that.filesInTemp)
                && Objects.equals(configDatabases, that.configDatabases);
        }

        @Override
        public int hashCode() {
            return Objects.hash(downloaderStats, cacheStats, databases, filesInTemp, configDatabases);
        }
    }
}
