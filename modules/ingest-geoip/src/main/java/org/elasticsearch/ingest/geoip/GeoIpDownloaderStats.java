/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;

import java.io.IOException;
import java.util.Objects;

public class GeoIpDownloaderStats implements Task.Status {

    public static final GeoIpDownloaderStats EMPTY = new GeoIpDownloaderStats(0, 0, 0, 0, 0);

    private final int successfulDownloads;
    private final int failedDownloads;
    private final long totalDownloadTime;
    private final int databasesCount;
    private final int skippedDownloads;

    public GeoIpDownloaderStats(StreamInput in) throws IOException {
        successfulDownloads = in.readVInt();
        failedDownloads = in.readVInt();
        totalDownloadTime = in.readVLong();
        databasesCount = in.readVInt();
        skippedDownloads = in.readVInt();
    }

    private GeoIpDownloaderStats(int successfulDownloads, int failedDownloads, long totalDownloadTime, int databasesCount,
                                 int skippedDownloads) {
        this.successfulDownloads = successfulDownloads;
        this.failedDownloads = failedDownloads;
        this.totalDownloadTime = totalDownloadTime;
        this.databasesCount = databasesCount;
        this.skippedDownloads = skippedDownloads;
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

    public GeoIpDownloaderStats skippedDownload() {
        return new GeoIpDownloaderStats(successfulDownloads, failedDownloads, totalDownloadTime, databasesCount, skippedDownloads + 1);
    }

    public GeoIpDownloaderStats successfulDownload(long downloadTime) {
        return new GeoIpDownloaderStats(successfulDownloads + 1, failedDownloads, totalDownloadTime + downloadTime, databasesCount,
            skippedDownloads);
    }

    public GeoIpDownloaderStats failedDownload() {
        return new GeoIpDownloaderStats(successfulDownloads, failedDownloads + 1, totalDownloadTime, databasesCount, skippedDownloads);
    }

    public GeoIpDownloaderStats count(int databasesCount) {
        return new GeoIpDownloaderStats(successfulDownloads, failedDownloads, totalDownloadTime, databasesCount, skippedDownloads);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("successful_downloads", successfulDownloads);
        builder.field("failed_downloads", failedDownloads);
        builder.field("total_download_time", totalDownloadTime);
        builder.field("databases_count", databasesCount);
        builder.field("skipped_updates", skippedDownloads);
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
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoIpDownloaderStats that = (GeoIpDownloaderStats) o;
        return successfulDownloads == that.successfulDownloads &&
            failedDownloads == that.failedDownloads &&
            totalDownloadTime == that.totalDownloadTime &&
            databasesCount == that.databasesCount &&
            skippedDownloads == that.skippedDownloads;
    }

    @Override
    public int hashCode() {
        return Objects.hash(successfulDownloads, failedDownloads, totalDownloadTime, databasesCount, skippedDownloads);
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
