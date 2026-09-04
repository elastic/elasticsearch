/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentElasticsearchExtension.DEFAULT_FORMATTER;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SnapshotStatsTests extends AbstractXContentTestCase<SnapshotStats> {

    private static final TransportVersion SNAPSHOT_INDEX_SHARD_STATUS_MISSING_STATS = TransportVersion.fromName(
        "snapshot_index_shard_status_missing_stats"
    );

    @Override
    protected SnapshotStats createTestInstance() {
        // Using less than half of Long.MAX_VALUE for random time values to avoid long overflow in tests that add the two time values
        long startTime = randomLongBetween(0, Long.MAX_VALUE / 2 - 1);
        long time = randomLongBetween(0, Long.MAX_VALUE / 2 - 1);
        int incrementalFileCount = randomIntBetween(0, Integer.MAX_VALUE);
        int totalFileCount = randomIntBetween(0, Integer.MAX_VALUE);
        int processedFileCount = randomIntBetween(0, Integer.MAX_VALUE);
        long incrementalSize = ((long) randomIntBetween(0, Integer.MAX_VALUE)) * 2;
        long totalSize = ((long) randomIntBetween(0, Integer.MAX_VALUE)) * 2;
        long processedSize = ((long) randomIntBetween(0, Integer.MAX_VALUE)) * 2;

        // toXContent() omits the "processed" sub-object if processedFileCount == incrementalFileCount, so here we increase the probability
        // of that scenario so we can make sure the processed values are set as expected in fromXContent().
        if (randomBoolean()) {
            processedFileCount = incrementalFileCount;
            processedSize = incrementalSize;
        }

        return new SnapshotStats(
            startTime,
            time,
            incrementalFileCount,
            totalFileCount,
            processedFileCount,
            incrementalSize,
            totalSize,
            processedSize
        );
    }

    @Override
    protected SnapshotStats doParseInstance(XContentParser parser) throws IOException {
        return fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testHumanReadableOutput() throws IOException {
        long startTime = System.currentTimeMillis();
        int time = randomIntBetween(0, Math.toIntExact(Duration.ofHours(1).toMillis()));
        int incrementalFileCount = randomIntBetween(0, 100);
        int totalFileCount = randomIntBetween(incrementalFileCount, 200);
        int processedFileCount = randomIntBetween(0, incrementalFileCount);
        int incrementalSize = randomIntBetween(0, ByteSizeUnit.MB.toIntBytes(1));
        int totalSize = randomIntBetween(incrementalSize, ByteSizeUnit.MB.toIntBytes(3));
        int processedSize = randomIntBetween(0, incrementalSize);
        SnapshotStats stats = new SnapshotStats(
            startTime,
            time,
            incrementalFileCount,
            totalFileCount,
            processedFileCount,
            incrementalSize,
            totalSize,
            processedSize
        );

        final ObjectPath statsObjectPath;
        final var xContent = randomFrom(XContentType.values()).xContent();
        try (var builder = XContentBuilder.builder(xContent)) {
            builder.humanReadable(true);
            stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
            statsObjectPath = ObjectPath.createFromXContent(xContent, BytesReference.bytes(builder));
        }

        assertThat(statsObjectPath.evaluate("incremental.file_count"), equalTo(incrementalFileCount));
        assertThat(statsObjectPath.evaluate("incremental.size"), equalTo(ByteSizeValue.ofBytes(incrementalSize).toString()));
        assertThat(statsObjectPath.evaluate("incremental.size_in_bytes"), equalTo(incrementalSize));

        // toXContent() omits the "processed" object when processed equals incremental
        if (processedFileCount != incrementalFileCount) {
            assertThat(statsObjectPath.evaluate("processed.file_count"), equalTo(processedFileCount));
            assertThat(statsObjectPath.evaluate("processed.size"), equalTo(ByteSizeValue.ofBytes(processedSize).toString()));
            assertThat(statsObjectPath.evaluate("processed.size_in_bytes"), equalTo(processedSize));
        } else {
            assertThat(statsObjectPath.evaluate("processed"), nullValue());
        }

        assertThat(statsObjectPath.evaluate("total.file_count"), equalTo(totalFileCount));
        assertThat(statsObjectPath.evaluate("total.size"), equalTo(ByteSizeValue.ofBytes(totalSize).toString()));
        assertThat(statsObjectPath.evaluate("total.size_in_bytes"), equalTo(totalSize));

        assertThat(statsObjectPath.evaluate("start_time"), equalTo(DEFAULT_FORMATTER.format(Instant.ofEpochMilli(startTime))));
        assertThat(statsObjectPath.evaluate("start_time_in_millis"), equalTo(startTime));
        assertThat(statsObjectPath.evaluate("time"), equalTo(TimeValue.timeValueMillis(time).toString()));
        assertThat(statsObjectPath.evaluate("time_in_millis"), equalTo(time));
    }

    public void testMissingStats() throws IOException {
        final var populatedStats = createTestInstance();
        final var missingStats = SnapshotStats.forMissingStats();
        assertEquals(0L, missingStats.getStartTime());
        assertEquals(0L, missingStats.getTime());
        assertEquals(-1, missingStats.getTotalFileCount());
        assertEquals(-1, missingStats.getIncrementalFileCount());
        assertEquals(-1, missingStats.getProcessedFileCount());
        assertEquals(-1L, missingStats.getTotalSize());
        assertEquals(-1L, missingStats.getIncrementalSize());
        assertEquals(-1L, missingStats.getProcessedSize());

        // Verify round trip Transport serialization.
        for (var transportVersion : List.of(
            TransportVersion.minimumCompatible(),
            SNAPSHOT_INDEX_SHARD_STATUS_MISSING_STATS,
            TransportVersion.current()
        )) {

            for (var stats : List.of(populatedStats, missingStats)) {
                final var bytesOut = new ByteArrayOutputStream();

                try (var streamOut = new OutputStreamStreamOutput(bytesOut)) {
                    streamOut.setTransportVersion(transportVersion);

                    if (transportVersion.supports(SNAPSHOT_INDEX_SHARD_STATUS_MISSING_STATS) || stats != missingStats) {
                        stats.writeTo(streamOut);
                    } else {
                        assertThrows(IllegalStateException.class, () -> stats.writeTo(streamOut));
                        continue;
                    }
                }

                try (var streamIn = new ByteArrayStreamInput(bytesOut.toByteArray())) {
                    streamIn.setTransportVersion(transportVersion);
                    final var statsRead = new SnapshotStats(streamIn);
                    assertEquals(stats, statsRead);
                }
            }
        }

        // Verify round trip XContent serialization.
        testFromXContent(SnapshotStats::forMissingStats);
    }

    static SnapshotStats fromXContent(XContentParser parser) throws IOException {
        // Parse this old school style instead of using the ObjectParser since there's an impedance mismatch between how the
        // object has historically been written as JSON versus how it is structured in Java.
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        long startTime = 0;
        long time = 0;
        int incrementalFileCount = 0;
        int totalFileCount = 0;
        int processedFileCount = Integer.MIN_VALUE;
        long incrementalSize = 0;
        long totalSize = 0;
        long processedSize = Long.MIN_VALUE;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String currentName = parser.currentName();
            token = parser.nextToken();
            if (currentName.equals(SnapshotStats.Fields.INCREMENTAL)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                    String innerName = parser.currentName();
                    token = parser.nextToken();
                    if (innerName.equals(SnapshotStats.Fields.FILE_COUNT)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                        incrementalFileCount = parser.intValue();
                    } else if (innerName.equals(SnapshotStats.Fields.SIZE_IN_BYTES)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                        incrementalSize = parser.longValue();
                    } else {
                        // Unknown sub field, skip
                        if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                            parser.skipChildren();
                        }
                    }
                }
            } else if (currentName.equals(SnapshotStats.Fields.PROCESSED)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                    String innerName = parser.currentName();
                    token = parser.nextToken();
                    if (innerName.equals(SnapshotStats.Fields.FILE_COUNT)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                        processedFileCount = parser.intValue();
                    } else if (innerName.equals(SnapshotStats.Fields.SIZE_IN_BYTES)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                        processedSize = parser.longValue();
                    } else {
                        // Unknown sub field, skip
                        if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                            parser.skipChildren();
                        }
                    }
                }
            } else if (currentName.equals(SnapshotStats.Fields.TOTAL)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
                    String innerName = parser.currentName();
                    token = parser.nextToken();
                    if (innerName.equals(SnapshotStats.Fields.FILE_COUNT)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                        totalFileCount = parser.intValue();
                    } else if (innerName.equals(SnapshotStats.Fields.SIZE_IN_BYTES)) {
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                        totalSize = parser.longValue();
                    } else {
                        // Unknown sub field, skip
                        if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                            parser.skipChildren();
                        }
                    }
                }
            } else if (currentName.equals(SnapshotStats.Fields.START_TIME_IN_MILLIS)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                startTime = parser.longValue();
            } else if (currentName.equals(SnapshotStats.Fields.TIME_IN_MILLIS)) {
                XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                time = parser.longValue();
            } else {
                // Unknown field, skip
                if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                }
            }
        }
        // Handle the case where the "processed" sub-object is omitted in toXContent() when processedFileCount == incrementalFileCount.
        if (processedFileCount == Integer.MIN_VALUE) {
            assert processedSize == Long.MIN_VALUE;
            processedFileCount = incrementalFileCount;
            processedSize = incrementalSize;
        }
        return new SnapshotStats(
            startTime,
            time,
            incrementalFileCount,
            totalFileCount,
            processedFileCount,
            incrementalSize,
            totalSize,
            processedSize
        );
    }
}
