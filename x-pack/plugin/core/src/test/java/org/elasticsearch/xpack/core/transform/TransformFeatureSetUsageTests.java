/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStatsTests;

import java.util.Arrays;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class TransformFeatureSetUsageTests extends AbstractWireSerializingTestCase<TransformFeatureSetUsage> {

    @Override
    protected TransformFeatureSetUsage createTestInstance() {
        Map<String, Long> transformCountByState = randomSubsetOf(Arrays.asList(IndexerState.values())).stream()
            .collect(toMap(state -> state.value(), state -> randomLong()));
        Map<String, Long> transformCountByFeature = randomList(10, () -> randomAlphaOfLength(10)).stream()
            .collect(toMap(f -> f, f -> randomLong()));
        TransformIndexerStats accumulatedStats = TransformIndexerStatsTests.randomStats();
        return new TransformFeatureSetUsage(transformCountByState, transformCountByFeature, accumulatedStats);
    }

    @Override
    protected Reader<TransformFeatureSetUsage> instanceReader() {
        return TransformFeatureSetUsage::new;
    }
}
