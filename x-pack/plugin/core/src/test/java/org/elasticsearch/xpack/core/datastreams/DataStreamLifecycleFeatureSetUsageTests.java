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

public class DataStreamLifecycleFeatureSetUsageTests extends AbstractWireSerializingTestCase<DataStreamLifecycleFeatureSetUsage> {

    @Override
    protected DataStreamLifecycleFeatureSetUsage createTestInstance() {
        return randomBoolean()
            ? new DataStreamLifecycleFeatureSetUsage(
                new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomDouble(),
                    randomBoolean()
                )
            )
            : DataStreamLifecycleFeatureSetUsage.DISABLED;
    }

    @Override
    protected DataStreamLifecycleFeatureSetUsage mutateInstance(DataStreamLifecycleFeatureSetUsage instance) {
        if (instance.equals(DataStreamLifecycleFeatureSetUsage.DISABLED)) {
            return new DataStreamLifecycleFeatureSetUsage(DataStreamLifecycleFeatureSetUsage.LifecycleStats.INITIAL);
        }
        return switch (randomInt(4)) {
            case 0 -> new DataStreamLifecycleFeatureSetUsage(
                new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
                    randomValueOtherThan(instance.lifecycleStats.dataStreamsWithLifecyclesCount, ESTestCase::randomLong),
                    instance.lifecycleStats.minRetentionMillis,
                    instance.lifecycleStats.maxRetentionMillis,
                    instance.lifecycleStats.averageRetentionMillis,
                    instance.lifecycleStats.defaultRolloverUsed
                )
            );
            case 1 -> new DataStreamLifecycleFeatureSetUsage(
                new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
                    instance.lifecycleStats.dataStreamsWithLifecyclesCount,
                    randomValueOtherThan(instance.lifecycleStats.minRetentionMillis, ESTestCase::randomLong),
                    instance.lifecycleStats.maxRetentionMillis,
                    instance.lifecycleStats.averageRetentionMillis,
                    instance.lifecycleStats.defaultRolloverUsed
                )
            );
            case 2 -> new DataStreamLifecycleFeatureSetUsage(
                new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
                    instance.lifecycleStats.dataStreamsWithLifecyclesCount,
                    instance.lifecycleStats.minRetentionMillis,
                    randomValueOtherThan(instance.lifecycleStats.maxRetentionMillis, ESTestCase::randomLong),
                    instance.lifecycleStats.averageRetentionMillis,
                    instance.lifecycleStats.defaultRolloverUsed
                )
            );
            case 3 -> new DataStreamLifecycleFeatureSetUsage(
                new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
                    instance.lifecycleStats.dataStreamsWithLifecyclesCount,
                    instance.lifecycleStats.minRetentionMillis,
                    instance.lifecycleStats.maxRetentionMillis,
                    randomValueOtherThan(instance.lifecycleStats.averageRetentionMillis, ESTestCase::randomDouble),
                    instance.lifecycleStats.defaultRolloverUsed
                )
            );
            case 4 -> new DataStreamLifecycleFeatureSetUsage(
                new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
                    instance.lifecycleStats.dataStreamsWithLifecyclesCount,
                    instance.lifecycleStats.minRetentionMillis,
                    instance.lifecycleStats.maxRetentionMillis,
                    instance.lifecycleStats.averageRetentionMillis,
                    instance.lifecycleStats.defaultRolloverUsed == false
                )
            );
            default -> throw new RuntimeException("unreachable");
        };
    }

    @Override
    protected Writeable.Reader<DataStreamLifecycleFeatureSetUsage> instanceReader() {
        return DataStreamLifecycleFeatureSetUsage::new;
    }

}
