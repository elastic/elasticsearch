/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class TransformCheckpointStatsTests extends AbstractWireSerializingTestCase<TransformCheckpointStats> {
    public static TransformCheckpointStats randomTransformCheckpointStats() {
        return new TransformCheckpointStats(
            randomLongBetween(1, 1_000_000),
            TransformIndexerPositionTests.randomTransformIndexerPosition(),
            randomBoolean() ? null : TransformProgressTests.randomTransformProgress(),
            randomLongBetween(1, 1_000_000),
            randomLongBetween(0, 1_000_000)
        );
    }

    @Override
    protected TransformCheckpointStats createTestInstance() {
        return randomTransformCheckpointStats();
    }

    @Override
    protected Reader<TransformCheckpointStats> instanceReader() {
        return TransformCheckpointStats::new;
    }
}
