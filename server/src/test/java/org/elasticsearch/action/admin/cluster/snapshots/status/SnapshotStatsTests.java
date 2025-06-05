/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class SnapshotStatsTests extends AbstractXContentTestCase<SnapshotStats> {

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
