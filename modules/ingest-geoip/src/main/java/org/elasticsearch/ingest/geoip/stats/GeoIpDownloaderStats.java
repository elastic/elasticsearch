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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class GeoIpDownloaderStats implements Task.Status {

    public static final GeoIpDownloaderStats EMPTY = new GeoIpDownloaderStats(0, 0, 0, 0, 0, 0);

    public static final ConstructingObjectParser<GeoIpDownloaderStats, Void> PARSER = new ConstructingObjectParser<>(
        "geoip_downloader_stats",
        a -> new GeoIpDownloaderStats((int) a[0], (int) a[1], (long) a[2], (int) a[3], (int) a[4], a[5] == null ? 0 : (int) a[5])
    );

    private static final ParseField SUCCESSFUL_DOWNLOADS = new ParseField("successful_downloads");
    private static final ParseField FAILED_DOWNLOADS = new ParseField("failed_downloads");
    private static final ParseField TOTAL_DOWNLOAD_TIME = new ParseField("total_download_time");
    private static final ParseField DATABASES_COUNT = new ParseField("databases_count");
    private static final ParseField SKIPPED_DOWNLOADS = new ParseField("skipped_updates");
    private static final ParseField EXPIRED_DATABASES = new ParseField("expired_databases");

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SUCCESSFUL_DOWNLOADS);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), FAILED_DOWNLOADS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), TOTAL_DOWNLOAD_TIME);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), DATABASES_COUNT);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), SKIPPED_DOWNLOADS);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), EXPIRED_DATABASES);
    }

    private final int successfulDownloads;
    private final int failedDownloads;
    private final long totalDownloadTime;
    private final int databasesCount;
    private final int skippedDownloads;
    private final int expiredDatabases;

    public GeoIpDownloaderStats(StreamInput in) throws IOException {
        successfulDownloads = in.readVInt();
        failedDownloads = in.readVInt();
        totalDownloadTime = in.readVLong();
        databasesCount = in.readVInt();
        skippedDownloads = in.readVInt();
        expiredDatabases = in.readVInt();
    }

    private GeoIpDownloaderStats(
        int successfulDownloads,
        int failedDownloads,
        long totalDownloadTime,
        int databasesCount,
        int skippedDownloads,
        int expiredDatabases
    ) {
        this.successfulDownloads = successfulDownloads;
        this.failedDownloads = failedDownloads;
        this.totalDownloadTime = totalDownloadTime;
        this.databasesCount = databasesCount;
        this.skippedDownloads = skippedDownloads;
        this.expiredDatabases = expiredDatabases;
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
            expiredDatabases
        );
    }

    public GeoIpDownloaderStats successfulDownload(long downloadTime) {
        return new GeoIpDownloaderStats(
            successfulDownloads + 1,
            failedDownloads,
            totalDownloadTime + Math.max(downloadTime, 0),
            databasesCount,
            skippedDownloads,
            expiredDatabases
        );
    }

    public GeoIpDownloaderStats failedDownload() {
        return new GeoIpDownloaderStats(
            successfulDownloads,
            failedDownloads + 1,
            totalDownloadTime,
            databasesCount,
            skippedDownloads,
            expiredDatabases
        );
    }

    public GeoIpDownloaderStats databasesCount(int databasesCount) {
        return new GeoIpDownloaderStats(
            successfulDownloads,
            failedDownloads,
            totalDownloadTime,
            databasesCount,
            skippedDownloads,
            expiredDatabases
        );
    }

    public GeoIpDownloaderStats expiredDatabases(int expiredDatabases) {
        return new GeoIpDownloaderStats(
            successfulDownloads,
            failedDownloads,
            totalDownloadTime,
            databasesCount,
            skippedDownloads,
            expiredDatabases
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
        builder.endObject();
        return builder;
    }

    public static GeoIpDownloaderStats fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(successfulDownloads);
        out.writeVInt(failedDownloads);
        out.writeVLong(totalDownloadTime);
        out.writeVInt(databasesCount);
        out.writeVInt(skippedDownloads);
        out.writeVInt(expiredDatabases);
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
