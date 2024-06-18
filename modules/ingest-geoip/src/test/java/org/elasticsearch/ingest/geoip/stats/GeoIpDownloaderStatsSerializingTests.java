/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

public class GeoIpDownloaderStatsSerializingTests extends AbstractXContentSerializingTestCase<GeoIpDownloaderStats> {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GeoIpDownloaderStats, Void> GEOIP_STATS_PARSER = new ConstructingObjectParser<>(
        "geoip_downloader_stats",
        a -> new GeoIpDownloaderStats(
            (int) a[0],
            (int) a[1],
            (long) a[2],
            (int) a[3],
            (int) a[4],
            (int) a[5],
            (List<DownloadedDatabaseInfo>) a[6]
        )
    );

    private static final ConstructingObjectParser<DownloadedDatabaseInfo, Void> DOWNLOADED_DATABASE_INFO_PARSER =
        new ConstructingObjectParser<>(
            "geoip_downloaded_database_info",
            a -> new DownloadedDatabaseInfo(
                (String) a[0],
                a[1] == null ? null : (DownloadedDatabaseInfo.DownloadAttempt) a[1],
                a[2] == null ? null : (DownloadedDatabaseInfo.DownloadAttempt) a[2]
            )
        );

    private static final ConstructingObjectParser<DownloadedDatabaseInfo.DownloadAttempt, Void> DOWNLOAD_ATTEMPT_PARSER =
        new ConstructingObjectParser<>(
            "geoip_downloaded_database_info",
            a -> new DownloadedDatabaseInfo.DownloadAttempt(
                (String) a[0],
                a[1] == null ? null : (Long) a[1],
                a[2] == null ? null : (Long) a[2],
                a[3] == null ? null : (String) a[3],
                a[4] == null ? null : (Long) a[4],
                a[5] == null ? null : (String) a[5]
            )
        );

    static {
        GEOIP_STATS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.SUCCESSFUL_DOWNLOADS);
        GEOIP_STATS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.FAILED_DOWNLOADS);
        GEOIP_STATS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.TOTAL_DOWNLOAD_TIME);
        GEOIP_STATS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.DATABASES_COUNT);
        GEOIP_STATS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.SKIPPED_DOWNLOADS);
        GEOIP_STATS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.EXPIRED_DATABASES);
        GEOIP_STATS_PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            DOWNLOADED_DATABASE_INFO_PARSER,
            new ParseField("downloader_attempts")
        );

        DOWNLOADED_DATABASE_INFO_PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("name"));
        DOWNLOADED_DATABASE_INFO_PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            DOWNLOAD_ATTEMPT_PARSER,
            new ParseField("last_success")
        );
        DOWNLOADED_DATABASE_INFO_PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            DOWNLOAD_ATTEMPT_PARSER,
            new ParseField("last_failure")
        );

        DOWNLOAD_ATTEMPT_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("archive_md5"));
        DOWNLOAD_ATTEMPT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), new ParseField("download_date_in_millis"));
        DOWNLOAD_ATTEMPT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), new ParseField("download_time_in_millis"));
        DOWNLOAD_ATTEMPT_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("source"));
        DOWNLOAD_ATTEMPT_PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), new ParseField("build_date_in_millis"));
        DOWNLOAD_ATTEMPT_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), new ParseField("error_message"));
    }

    @Override
    protected GeoIpDownloaderStats doParseInstance(XContentParser parser) throws IOException {
        return GEOIP_STATS_PARSER.parse(parser, null);
    }

    @Override
    protected Writeable.Reader<GeoIpDownloaderStats> instanceReader() {
        return GeoIpDownloaderStats::new;
    }

    @Override
    protected GeoIpDownloaderStats createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected GeoIpDownloaderStats mutateInstance(GeoIpDownloaderStats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    static GeoIpDownloaderStats createRandomInstance() {
        GeoIpDownloaderStats stats = GeoIpDownloaderStats.EMPTY.databasesCount(randomInt(1000));
        int successes = randomInt(20);
        for (int i = 0; i < successes; i++) {
            stats = stats.successfulDownload(
                randomAlphaOfLength(20),
                randomBoolean() ? null : randomAlphaOfLength(20),
                randomBoolean() ? null : randomLong(),
                randomBoolean() ? null : randomLong(),
                randomBoolean() ? null : randomAlphaOfLength(20),
                randomLongBetween(0, 3000)
            );
        }
        int failures = randomInt(20);
        for (int i = 0; i < failures; i++) {
            stats = stats.failedDownload(
                randomAlphaOfLength(20),
                randomBoolean() ? null : randomAlphaOfLength(10),
                new RuntimeException("failed"),
                randomLong(),
                randomBoolean() ? null : randomLong(),
                randomBoolean() ? null : randomAlphaOfLength(20),
                randomLongBetween(0, 3000)
            );
        }
        int skipped = randomInt(20);
        for (int i = 0; i < skipped; i++) {
            stats = stats.skippedDownload();
        }
        return stats;
    }
}
