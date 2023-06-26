/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class TransformStatsTests extends AbstractWireSerializingTestCase<TransformStats> {

    public static TransformStats randomTransformStats() {
        return new TransformStats(
            randomAlphaOfLength(10),
            randomFrom(TransformStats.State.values()),
            randomBoolean() ? null : randomAlphaOfLength(100),
            randomBoolean() ? null : NodeAttributeTests.randomNodeAttributes(),
            TransformIndexerStatsTests.randomStats(),
            TransformCheckpointingInfoTests.randomTransformCheckpointingInfo(),
            TransformHealthTests.randomTransformHealth()
        );
    }

    @Override
    protected TransformStats createTestInstance() {
        return randomTransformStats();
    }

    @Override
    protected TransformStats mutateInstance(TransformStats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<TransformStats> instanceReader() {
        return TransformStats::new;
    }
}
