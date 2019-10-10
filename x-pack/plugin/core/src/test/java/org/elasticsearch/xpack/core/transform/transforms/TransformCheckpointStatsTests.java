/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class TransformCheckpointStatsTests extends AbstractSerializingTransformTestCase<TransformCheckpointStats>
{
    public static TransformCheckpointStats randomDataFrameTransformCheckpointStats() {
        return new TransformCheckpointStats(randomLongBetween(1, 1_000_000),
            TransformIndexerPositionTests.randomTransformIndexerPosition(),
            randomBoolean() ? null : TransformProgressTests.randomTransformProgress(),
            randomLongBetween(1, 1_000_000), randomLongBetween(0, 1_000_000));
    }

    @Override
    protected TransformCheckpointStats doParseInstance(XContentParser parser) throws IOException {
        return TransformCheckpointStats.fromXContent(parser);
    }

    @Override
    protected TransformCheckpointStats createTestInstance() {
        return randomDataFrameTransformCheckpointStats();
    }

    @Override
    protected Reader<TransformCheckpointStats> instanceReader() {
        return TransformCheckpointStats::new;
    }
}
