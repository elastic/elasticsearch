/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.time.Instant;

public class TransformCheckpointingInfoTests extends AbstractWireSerializingTestCase<TransformCheckpointingInfo> {

    public static TransformCheckpointingInfo randomTransformCheckpointingInfo() {
        return new TransformCheckpointingInfo(
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            TransformCheckpointStatsTests.randomTransformCheckpointStats(),
            randomNonNegativeLong(),
            randomBoolean() ? null : Instant.ofEpochMilli(randomLongBetween(1, 100000)),
            randomBoolean() ? null : Instant.ofEpochMilli(randomLongBetween(1, 100000))
        );
    }

    @Override
    protected TransformCheckpointingInfo createTestInstance() {
        return randomTransformCheckpointingInfo();
    }

    @Override
    protected TransformCheckpointingInfo mutateInstance(TransformCheckpointingInfo instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<TransformCheckpointingInfo> instanceReader() {
        return TransformCheckpointingInfo::new;
    }
}
