/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GeoIpStatsResponse implements ToXContentObject {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GeoIpStatsResponse, Void> PARSER = new ConstructingObjectParser<>("geoip_stats", a -> {
        Map<String, Object> stats = (Map<String, Object>) a[0];
        List<Tuple<String, NodeInfo>> nodes = (List<Tuple<String, NodeInfo>>) a[1];

        return new GeoIpStatsResponse((int) stats.get("successful_downloads"), (int) stats.get("failed_downloads"),
            ((Number) stats.get("total_download_time")).longValue(), (int) stats.get("databases_count"), (int) stats.get("skipped_updates"),
            nodes.stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)));
    });

    static {
        PARSER.declareObject(constructorArg(), (p, c) -> p.map(), new ParseField("stats"));
        PARSER.declareNamedObjects(constructorArg(), (p, c, name) -> Tuple.tuple(name, NodeInfo.PARSER.apply(p, c)),
            new ParseField("nodes"));
    }

    private final int successfulDownloads;
    private final int failedDownloads;
    private final long totalDownloadTime;
    private final int databasesCount;
    private final int skippedDownloads;
    private final Map<String, NodeInfo> nodes;

    public GeoIpStatsResponse(int successfulDownloads, int failedDownloads, long totalDownloadTime, int databasesCount,
                              int skippedDownloads, Map<String, NodeInfo> nodes) {
        this.successfulDownloads = successfulDownloads;
        this.failedDownloads = failedDownloads;
        this.totalDownloadTime = totalDownloadTime;
        this.databasesCount = databasesCount;
        this.skippedDownloads = skippedDownloads;
        this.nodes = nodes;
    }

    public int getSuccessfulDownloads() {
        return successfulDownloads;
    }

    public int getFailedDownloads() {
        return failedDownloads;
    }

    public long getTotalDownloadTime() {
        return totalDownloadTime;
    }

    public int getDatabasesCount() {
        return databasesCount;
    }

    public int getSkippedDownloads() {
        return skippedDownloads;
    }

    public Map<String, NodeInfo> getNodes() {
        return nodes;
    }

    public static GeoIpStatsResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoIpStatsResponse that = (GeoIpStatsResponse) o;
        return successfulDownloads == that.successfulDownloads
            && failedDownloads == that.failedDownloads
            && totalDownloadTime == that.totalDownloadTime
            && databasesCount == that.databasesCount
            && skippedDownloads == that.skippedDownloads
            && nodes.equals(that.nodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(successfulDownloads, failedDownloads, totalDownloadTime, databasesCount, skippedDownloads, nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("stats");
        {
            builder.field("successful_downloads", successfulDownloads);
            builder.field("failed_downloads", failedDownloads);
            builder.field("skipped_updates", skippedDownloads);
            builder.field("total_download_time", totalDownloadTime);
            builder.field("databases_count", databasesCount);
        }
        builder.endObject();
        builder.field("nodes", nodes);
        builder.endObject();
        return builder;
    }

    public static final class NodeInfo implements ToXContentObject {
        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<NodeInfo, Void> PARSER = new ConstructingObjectParser<>("node_info", a -> {
            List<DatabaseInfo> databases = (List<DatabaseInfo>) a[1];
            return new NodeInfo((Collection<String>) a[0], databases.stream().collect(Collectors.toMap(DatabaseInfo::getName,
                Function.identity())));
        });

        static {
            PARSER.declareStringArray(optionalConstructorArg(), new ParseField("files_in_temp"));
            PARSER.declareObjectArray(optionalConstructorArg(), DatabaseInfo.PARSER, new ParseField("databases"));
        }

        private final List<String> filesInTemp;
        private final Map<String, DatabaseInfo> databases;

        public NodeInfo(Collection<String> filesInTemp, Map<String, DatabaseInfo> databases) {
            this.filesInTemp = List.copyOf(filesInTemp);
            this.databases = Map.copyOf(databases);
        }

        public List<String> getFilesInTemp() {
            return filesInTemp;
        }

        public Map<String, DatabaseInfo> getDatabases() {
            return databases;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("files_in_temp", filesInTemp);
            builder.field("databases", databases.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .collect(Collectors.toList()));
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeInfo nodeInfo = (NodeInfo) o;
            return filesInTemp.equals(nodeInfo.filesInTemp) && databases.equals(nodeInfo.databases);
        }

        @Override
        public int hashCode() {
            return Objects.hash(filesInTemp, databases);
        }
    }

    public static final class DatabaseInfo implements ToXContentObject {

        private static final ConstructingObjectParser<DatabaseInfo, Void> PARSER = new ConstructingObjectParser<>("database_info",
            a -> new DatabaseInfo((String) a[0]));

        static {
            PARSER.declareString(constructorArg(), new ParseField("name"));
        }

        private final String name;

        public DatabaseInfo(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", name);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DatabaseInfo that = (DatabaseInfo) o;
            return name.equals(that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }
}
