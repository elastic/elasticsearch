/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStatsTests;

import java.util.HashMap;
import java.util.Map;

public class TransformFeatureSetUsageTests extends AbstractWireSerializingTestCase<TransformFeatureSetUsage> {

    @Override
    protected TransformFeatureSetUsage createTestInstance() {
        Map<String, Long> transformCountByState = new HashMap<>();

        if (randomBoolean()) {
            transformCountByState.put(randomFrom(IndexerState.values()).toString(), randomLong());
        }

        return new TransformFeatureSetUsage(randomBoolean(), randomBoolean(), transformCountByState,
                TransformIndexerStatsTests.randomStats());
    }

    @Override
    protected Reader<TransformFeatureSetUsage> instanceReader() {
        return TransformFeatureSetUsage::new;
    }

}
