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

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class TransformCheckpointStatsTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        xContentTester(this::createParser,
            TransformCheckpointStatsTests::randomTransformCheckpointStats,
            TransformCheckpointStatsTests::toXContent,
            TransformCheckpointStats::fromXContent)
                .supportsUnknownFields(true)
                .randomFieldsExcludeFilter(field -> field.startsWith("position"))
                .test();
    }

    public static TransformCheckpointStats randomTransformCheckpointStats() {
        return new TransformCheckpointStats(randomLongBetween(1, 1_000_000),
            randomBoolean() ? null : TransformIndexerPositionTests.randomTransformIndexerPosition(),
            randomBoolean() ? null : TransformProgressTests.randomInstance(),
            randomLongBetween(1, 1_000_000), randomLongBetween(0, 1_000_000));
    }

    public static void toXContent(TransformCheckpointStats stats, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field(TransformCheckpointStats.CHECKPOINT.getPreferredName(), stats.getCheckpoint());
        if (stats.getPosition() != null) {
            builder.field(TransformCheckpointStats.POSITION.getPreferredName());
            TransformIndexerPositionTests.toXContent(stats.getPosition(), builder);
        }
        if (stats.getCheckpointProgress() != null) {
            builder.field(TransformCheckpointStats.CHECKPOINT_PROGRESS.getPreferredName());
            TransformProgressTests.toXContent(stats.getCheckpointProgress(), builder);
        }
        builder.field(TransformCheckpointStats.TIMESTAMP_MILLIS.getPreferredName(), stats.getTimestampMillis());
        builder.field(TransformCheckpointStats.TIME_UPPER_BOUND_MILLIS.getPreferredName(), stats.getTimeUpperBoundMillis());
        builder.endObject();
    }
}
