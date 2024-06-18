/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.ingest.geoip.GeoIpDownloader;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.TransportVersions.GEOIP_ADDITIONAL_DATABASE_DOWNLOAD_STATS;

public class GeoIpDownloaderStats implements Task.Status {

    public static final GeoIpDownloaderStats EMPTY = new GeoIpDownloaderStats(0, 0, 0, 0, 0, 0, Collections.emptySortedMap());

    static final ParseField SUCCESSFUL_DOWNLOADS = new ParseField("successful_downloads");
    static final ParseField FAILED_DOWNLOADS = new ParseField("failed_downloads");
    static final ParseField TOTAL_DOWNLOAD_TIME = new ParseField("total_download_time");
    static final ParseField DATABASES_COUNT = new ParseField("databases_count");
    static final ParseField SKIPPED_DOWNLOADS = new ParseField("skipped_updates");
    static final ParseField EXPIRED_DATABASES = new ParseField("expired_databases");

    private final int successfulDownloads;
    private final int failedDownloads;
    private final long totalDownloadTime;
    private final int databasesCount;
    private final int skippedDownloads;
    private final int expiredDatabases;
    private final SortedMap<String, DownloadedDatabaseInfo> downloadedDatabaseInfos;

    public GeoIpDownloaderStats(StreamInput in) throws IOException {
        successfulDownloads = in.readVInt();
        failedDownloads = in.readVInt();
        totalDownloadTime = in.readVLong();
        databasesCount = in.readVInt();
        skippedDownloads = in.readVInt();
        expiredDatabases = in.readVInt();
        if (in.getTransportVersion().onOrAfter(GEOIP_ADDITIONAL_DATABASE_DOWNLOAD_STATS)) {
            int databaseStatsCount = in.readVInt();
            downloadedDatabaseInfos = new TreeMap<>();
            for (int i = 0; i < databaseStatsCount; i++) {
                downloadedDatabaseInfos.put(in.readString(), new DownloadedDatabaseInfo(in));
            }
        } else {
            downloadedDatabaseInfos = Collections.emptySortedMap();
        }
    }

    GeoIpDownloaderStats(
        int successfulDownloads,
        int failedDownloads,
        long totalDownloadTime,
        int databasesCount,
        int skippedDownloads,
        int expiredDatabases,
        SortedMap<String, DownloadedDatabaseInfo> downloadedDatabaseInfos
    ) {
        this.successfulDownloads = successfulDownloads;
        this.failedDownloads = failedDownloads;
        this.totalDownloadTime = totalDownloadTime;
        this.databasesCount = databasesCount;
        this.skippedDownloads = skippedDownloads;
        this.expiredDatabases = expiredDatabases;
        this.downloadedDatabaseInfos = downloadedDatabaseInfos;
    }

    @SuppressWarnings("cast")
    GeoIpDownloaderStats(
        int successfulDownloads,
        int failedDownloads,
        long totalDownloadTime,
        int databasesCount,
        int skippedDownloads,
        int expiredDatabases,
        List<DownloadedDatabaseInfo> downloadedDatabaseInfos
    ) {
        this(
            successfulDownloads,
            failedDownloads,
            totalDownloadTime,
            databasesCount,
            skippedDownloads,
            expiredDatabases,
            (TreeMap<String, DownloadedDatabaseInfo>) downloadedDatabaseInfos.stream()
                .collect(Collectors.toMap(DownloadedDatabaseInfo::name, Function.identity(), (k1, k2) -> k2, TreeMap::new))
        );
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

    public int getExpiredDatabases() {
        return expiredDatabases;
    }

    public GeoIpDownloaderStats skippedDownload() {
        return new GeoIpDownloaderStats(
            successfulDownloads,
            failedDownloads,
            totalDownloadTime,
            databasesCount,
            skippedDownloads + 1,
            expiredDatabases,
            this.downloadedDatabaseInfos
        );
    }

    public GeoIpDownloaderStats successfulDownload(
        String name,
        String md5,
        Long downloadDate,
        Long buildDate,
        String source,
        long downloadTime
    ) {
        Objects.requireNonNull(name);
        TreeMap<String, DownloadedDatabaseInfo> updatedDatabaseInfos;
        DownloadedDatabaseInfo downloadedDatabaseInfo = downloadedDatabaseInfos.get(name);
        DownloadedDatabaseInfo updatedDownloadedDatabaseInfo;
        DownloadedDatabaseInfo.DownloadAttempt success = new DownloadedDatabaseInfo.DownloadAttempt(
            md5,
            downloadDate,
            downloadTime,
            source,
            buildDate,
            null
        );
        if (downloadedDatabaseInfo == null) {
            updatedDownloadedDatabaseInfo = new DownloadedDatabaseInfo(name, success, null);
        } else {
            updatedDownloadedDatabaseInfo = new DownloadedDatabaseInfo(name, success, downloadedDatabaseInfo.failedAttempt());
        }
        updatedDatabaseInfos = new TreeMap<>(downloadedDatabaseInfos);
        updatedDatabaseInfos.put(name, updatedDownloadedDatabaseInfo);
        return new GeoIpDownloaderStats(
            successfulDownloads + 1,
            failedDownloads,
            totalDownloadTime + Math.max(downloadTime, 0),
            databasesCount,
            skippedDownloads,
            expiredDatabases,
            updatedDatabaseInfos
        );
    }

    public GeoIpDownloaderStats failedDownloadDatabaseUnknown(Exception exception) {
        return failedDownload(null, null, exception, null, null, null, null);
    }

    public GeoIpDownloaderStats failedDownload(
        String name,
        String md5,
        Exception exception,
        Long downloadDate,
        Long buildDate,
        String source,
        Long downloadTime
    ) {
        SortedMap<String, DownloadedDatabaseInfo> updatedDatabaseInfos;
        if (name == null) {
            updatedDatabaseInfos = downloadedDatabaseInfos;
        } else {
            DownloadedDatabaseInfo downloadedDatabaseInfo = downloadedDatabaseInfos.get(name);
            DownloadedDatabaseInfo updatedDownloadedDatabaseInfo;
            DownloadedDatabaseInfo.DownloadAttempt failure = new DownloadedDatabaseInfo.DownloadAttempt(
                md5,
                downloadDate,
                downloadTime,
                source,
                buildDate,
                exception.getMessage()
            );
            if (downloadedDatabaseInfo == null) {
                updatedDownloadedDatabaseInfo = new DownloadedDatabaseInfo(name, null, failure);
            } else {
                updatedDownloadedDatabaseInfo = new DownloadedDatabaseInfo(name, downloadedDatabaseInfo.successfulAttempt(), failure);
            }
            updatedDatabaseInfos = new TreeMap<>(downloadedDatabaseInfos);
            updatedDatabaseInfos.put(name, updatedDownloadedDatabaseInfo);
        }
        return new GeoIpDownloaderStats(
            successfulDownloads,
            failedDownloads + 1,
            totalDownloadTime,
            databasesCount,
            skippedDownloads,
            expiredDatabases,
            updatedDatabaseInfos
        );
    }

    public GeoIpDownloaderStats databasesCount(int databasesCount) {
        return new GeoIpDownloaderStats(
            successfulDownloads,
            failedDownloads,
            totalDownloadTime,
            databasesCount,
            skippedDownloads,
            expiredDatabases,
            this.downloadedDatabaseInfos
        );
    }

    public GeoIpDownloaderStats expiredDatabases(int expiredDatabases) {
        return new GeoIpDownloaderStats(
            successfulDownloads,
            failedDownloads,
            totalDownloadTime,
            databasesCount,
            skippedDownloads,
            expiredDatabases,
            this.downloadedDatabaseInfos
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESSFUL_DOWNLOADS.getPreferredName(), successfulDownloads);
        builder.field(FAILED_DOWNLOADS.getPreferredName(), failedDownloads);
        builder.field(TOTAL_DOWNLOAD_TIME.getPreferredName(), totalDownloadTime);
        builder.field(DATABASES_COUNT.getPreferredName(), databasesCount);
        builder.field(SKIPPED_DOWNLOADS.getPreferredName(), skippedDownloads);
        builder.field(EXPIRED_DATABASES.getPreferredName(), expiredDatabases);
        builder.xContentList("downloader_attempts", downloadedDatabaseInfos.values(), params);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(successfulDownloads);
        out.writeVInt(failedDownloads);
        out.writeVLong(totalDownloadTime);
        out.writeVInt(databasesCount);
        out.writeVInt(skippedDownloads);
        out.writeVInt(expiredDatabases);
        if (out.getTransportVersion().onOrAfter(GEOIP_ADDITIONAL_DATABASE_DOWNLOAD_STATS)) {
            out.writeVInt(downloadedDatabaseInfos.size());
            for (Map.Entry<String, DownloadedDatabaseInfo> entry : downloadedDatabaseInfos.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoIpDownloaderStats that = (GeoIpDownloaderStats) o;
        return successfulDownloads == that.successfulDownloads
            && failedDownloads == that.failedDownloads
            && totalDownloadTime == that.totalDownloadTime
            && databasesCount == that.databasesCount
            && skippedDownloads == that.skippedDownloads
            && expiredDatabases == that.expiredDatabases;
    }

    @Override
    public int hashCode() {
        return Objects.hash(successfulDownloads, failedDownloads, totalDownloadTime, databasesCount, skippedDownloads, expiredDatabases);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public String getWriteableName() {
        return GeoIpDownloader.GEOIP_DOWNLOADER;
    }
}
