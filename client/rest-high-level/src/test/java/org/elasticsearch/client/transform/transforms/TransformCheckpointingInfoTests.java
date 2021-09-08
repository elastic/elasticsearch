/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.Instant;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class TransformCheckpointingInfoTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            TransformCheckpointingInfoTests::randomTransformCheckpointingInfo,
            TransformCheckpointingInfoTests::toXContent,
            TransformCheckpointingInfo::fromXContent
        ).supportsUnknownFields(false).test();
    }

    public static TransformCheckpointingInfo randomTransformCheckpointingInfo() {
        return new TransformCheckpointingInfo(
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            randomLongBetween(0, 10000),
            randomBoolean() ? null : Instant.ofEpochMilli(randomNonNegativeLong()),
            randomBoolean() ? null : Instant.ofEpochMilli(randomNonNegativeLong())
        );
    }

    public static void toXContent(TransformCheckpointingInfo info, XContentBuilder builder) throws IOException {
        builder.startObject();
        if (info.getLast().getTimestampMillis() > 0) {
            builder.field(TransformCheckpointingInfo.LAST_CHECKPOINT.getPreferredName());
            TransformCheckpointStatsTests.toXContent(info.getLast(), builder);
        }
        if (info.getNext().getTimestampMillis() > 0) {
            builder.field(TransformCheckpointingInfo.NEXT_CHECKPOINT.getPreferredName());
            TransformCheckpointStatsTests.toXContent(info.getNext(), builder);
        }
        builder.field(TransformCheckpointingInfo.OPERATIONS_BEHIND.getPreferredName(), info.getOperationsBehind());
        if (info.getChangesLastDetectedAt() != null) {
            builder.field(TransformCheckpointingInfo.CHANGES_LAST_DETECTED_AT.getPreferredName(), info.getChangesLastDetectedAt());
        }
        if (info.getLastSearchTime() != null) {
            builder.field(TransformCheckpointingInfo.LAST_SEARCH_TIME.getPreferredName(), info.getLastSearchTime());
        }
        builder.endObject();
    }
}
