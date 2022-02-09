/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class GeoIpDownloaderStatsSerializingTests extends AbstractSerializingTestCase<GeoIpDownloaderStats> {

    @Override
    protected GeoIpDownloaderStats doParseInstance(XContentParser parser) throws IOException {
        return GeoIpDownloaderStats.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<GeoIpDownloaderStats> instanceReader() {
        return GeoIpDownloaderStats::new;
    }

    @Override
    protected GeoIpDownloaderStats createTestInstance() {
        return createRandomInstance();
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
