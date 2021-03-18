/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class GeoIpStatsResponse {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GeoIpStatsResponse, Void> PARSER = new ConstructingObjectParser<>("geoip_stats", a -> {
        Map<String, Object> stats = (Map<String, Object>) a[0];
        List<Tuple<String, NodeInfo>> nodes = (List<Tuple<String, NodeInfo>>) a[1];

        return new GeoIpStatsResponse((int) stats.get("successful_downloads"), (int) stats.get("failed_downloads"),
            ((Number)stats.get("total_download_time")).longValue(), (int) stats.get("databases_count"), (int) stats.get("skipped_updates"),
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

    public static final class NodeInfo {
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
    }

    public static final class DatabaseInfo {

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
    }
}
