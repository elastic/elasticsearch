/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.datastreams.DataStreamLifecycleFeatureSetUsageTests.generateGlobalRetention;
import static org.elasticsearch.xpack.core.datastreams.DataStreamLifecycleFeatureSetUsageTests.generateRetentionStats;

public class DataStreamFeatureSetUsageTests extends AbstractWireSerializingTestCase<DataStreamFeatureSetUsage> {

    @Override
    protected DataStreamFeatureSetUsage createTestInstance() {
        return new DataStreamFeatureSetUsage(
            new DataStreamFeatureSetUsage.DataStreamStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                generateRetentionStats(),
                generateRetentionStats(),
                randomBoolean() ? Map.of() : Map.of("default", generateGlobalRetention(), "max", generateGlobalRetention())
            )
        );
    }

    @Override
    protected DataStreamFeatureSetUsage mutateInstance(DataStreamFeatureSetUsage instance) {
        var totalDataStreamCount = instance.getStats().totalDataStreamCount();
        var indicesBehindDataStream = instance.getStats().indicesBehindDataStream();
        long failureStoreExplicitlyEnabledDataStreamCount = instance.getStats().failureStoreExplicitlyEnabledDataStreamCount();
        long failureStoreEffectivelyEnabledDataStreamCount = instance.getStats().failureStoreEffectivelyEnabledDataStreamCount();
        long failureStoreIndicesCount = instance.getStats().failureStoreIndicesCount();
        long failuresLifecycleExplicitlyEnabledDataStreamCount = instance.getStats().failuresLifecycleExplicitlyEnabledCount();
        long failuresLifecycleEffectivelyEnabledDataStreamCount = instance.getStats().failuresLifecycleEffectivelyEnabledCount();
        var dataRetentionStats = instance.getStats().failuresLifecycleDataRetentionStats();
        var effectiveRetentionStats = instance.getStats().failuresLifecycleEffectiveRetentionStats();
        var defaultRetention = instance.getStats().globalRetentionStats().get("default");
        var maxRetention = instance.getStats().globalRetentionStats().get("max");

        switch (between(0, 10)) {
            case 0 -> totalDataStreamCount = randomValueOtherThan(totalDataStreamCount, ESTestCase::randomNonNegativeLong);
            case 1 -> indicesBehindDataStream = randomValueOtherThan(indicesBehindDataStream, ESTestCase::randomNonNegativeLong);
            case 2 -> failureStoreExplicitlyEnabledDataStreamCount = randomValueOtherThan(
                failureStoreExplicitlyEnabledDataStreamCount,
                ESTestCase::randomNonNegativeLong
            );
            case 3 -> failureStoreEffectivelyEnabledDataStreamCount = randomValueOtherThan(
                failureStoreEffectivelyEnabledDataStreamCount,
                ESTestCase::randomNonNegativeLong
            );
            case 4 -> failureStoreIndicesCount = randomValueOtherThan(failureStoreIndicesCount, ESTestCase::randomNonNegativeLong);
            case 5 -> failuresLifecycleExplicitlyEnabledDataStreamCount = randomValueOtherThan(
                failuresLifecycleExplicitlyEnabledDataStreamCount,
                ESTestCase::randomNonNegativeLong
            );
            case 6 -> failuresLifecycleEffectivelyEnabledDataStreamCount = randomValueOtherThan(
                failuresLifecycleEffectivelyEnabledDataStreamCount,
                ESTestCase::randomNonNegativeLong
            );
            case 7 -> dataRetentionStats = randomValueOtherThan(
                dataRetentionStats,
                DataStreamLifecycleFeatureSetUsageTests::generateRetentionStats
            );
            case 8 -> effectiveRetentionStats = randomValueOtherThan(
                effectiveRetentionStats,
                DataStreamLifecycleFeatureSetUsageTests::generateRetentionStats
            );
            case 9 -> maxRetention = randomValueOtherThan(maxRetention, DataStreamLifecycleFeatureSetUsageTests::generateGlobalRetention);
            case 10 -> defaultRetention = randomValueOtherThan(
                defaultRetention,
                DataStreamLifecycleFeatureSetUsageTests::generateGlobalRetention
            );
            default -> throw new IllegalStateException("Unexpected randomisation branch");
        }
        Map<String, DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats> map = new HashMap<>();
        if (defaultRetention != null) {
            map.put("default", defaultRetention);
        }
        if (maxRetention != null) {
            map.put("max", maxRetention);
        }
        return new DataStreamFeatureSetUsage(
            new DataStreamFeatureSetUsage.DataStreamStats(
                totalDataStreamCount,
                indicesBehindDataStream,
                failureStoreExplicitlyEnabledDataStreamCount,
                failureStoreEffectivelyEnabledDataStreamCount,
                failureStoreIndicesCount,
                failuresLifecycleExplicitlyEnabledDataStreamCount,
                failuresLifecycleEffectivelyEnabledDataStreamCount,
                dataRetentionStats,
                effectiveRetentionStats,
                map
            )
        );
    }

    @Override
    protected Writeable.Reader<DataStreamFeatureSetUsage> instanceReader() {
        return DataStreamFeatureSetUsage::new;
    }
}
