/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class GeoIpDownloaderStatsSerializingTests extends AbstractXContentSerializingTestCase<GeoIpDownloaderStats> {

    private static final ConstructingObjectParser<GeoIpDownloaderStats, Void> PARSER = new ConstructingObjectParser<>(
        "geoip_downloader_stats",
        a -> new GeoIpDownloaderStats((int) a[0], (int) a[1], (long) a[2], (int) a[3], (int) a[4], a[5] == null ? 0 : (int) a[5])
    );

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.SUCCESSFUL_DOWNLOADS);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.FAILED_DOWNLOADS);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.TOTAL_DOWNLOAD_TIME);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.DATABASES_COUNT);
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), GeoIpDownloaderStats.SKIPPED_DOWNLOADS);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), GeoIpDownloaderStats.EXPIRED_DATABASES);
    }

    @Override
    protected GeoIpDownloaderStats doParseInstance(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
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
            stats = stats.successfulDownload(randomLongBetween(0, 3000));
        }
        int failures = randomInt(20);
        for (int i = 0; i < failures; i++) {
            stats = stats.failedDownload();
        }
        int skipped = randomInt(20);
        for (int i = 0; i < skipped; i++) {
            stats = stats.skippedDownload();
        }
        return stats;
    }
}
