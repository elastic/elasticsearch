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

public class DataStreamFeatureSetUsageTests extends AbstractWireSerializingTestCase<DataStreamFeatureSetUsage> {

    @Override
    protected DataStreamFeatureSetUsage createTestInstance() {
        return new DataStreamFeatureSetUsage(
            new DataStreamFeatureSetUsage.DataStreamStats(randomNonNegativeLong(), randomNonNegativeLong()),
            new DataStreamFeatureSetUsage.LifecycleStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomDouble(),
                randomBoolean()
            )
        );
    }

    @Override
    protected DataStreamFeatureSetUsage mutateInstance(DataStreamFeatureSetUsage instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private DataStreamFeatureSetUsage.DataStreamStats mutateDataStreamsStats(DataStreamFeatureSetUsage.DataStreamStats instance) {
        long totalDataStreamCount = instance.totalDataStreamCount();
        long indicesBehindDataStream = instance.indicesBehindDataStream();
        if (randomBoolean()) {
            totalDataStreamCount = randomValueOtherThan(totalDataStreamCount, ESTestCase::randomNonNegativeLong);
        } else {
            indicesBehindDataStream = randomValueOtherThan(indicesBehindDataStream, ESTestCase::randomNonNegativeLong);
        }
        return new DataStreamFeatureSetUsage.DataStreamStats(totalDataStreamCount, indicesBehindDataStream);
    }

    private DataStreamFeatureSetUsage.LifecycleStats mutateLifecycleStats(DataStreamFeatureSetUsage.LifecycleStats instance) {
        long dataStreamsWithLifecyclesCount = instance.dataStreamsWithLifecyclesCount();
        long indicesWithLifecyclesCount = instance.indicesWithLifecyclesCount();
        boolean defaultRolloverUsed = instance.defaultRolloverUsed();
        long minRetentionMillis = instance.minRetentionMillis();
        long maxRetentionMillis = instance.maxRetentionMillis();
        double averageRetentionMillis = instance.averageRetentionMillis();
        switch (randomInt(5)) {
            case 0 -> dataStreamsWithLifecyclesCount = randomValueOtherThan(
                dataStreamsWithLifecyclesCount,
                ESTestCase::randomNonNegativeLong
            );
            case 1 -> indicesWithLifecyclesCount = randomValueOtherThan(indicesWithLifecyclesCount, ESTestCase::randomNonNegativeLong);
            case 2 -> minRetentionMillis = randomValueOtherThan(minRetentionMillis, ESTestCase::randomNonNegativeLong);
            case 3 -> maxRetentionMillis = randomValueOtherThan(maxRetentionMillis, ESTestCase::randomNonNegativeLong);
            case 4 -> averageRetentionMillis = randomValueOtherThan(averageRetentionMillis, ESTestCase::randomDouble);
            default -> defaultRolloverUsed = defaultRolloverUsed && false;
        }
        return new DataStreamFeatureSetUsage.LifecycleStats(
            dataStreamsWithLifecyclesCount,
            indicesWithLifecyclesCount,
            minRetentionMillis,
            maxRetentionMillis,
            averageRetentionMillis,
            defaultRolloverUsed
        );
    }

    @Override
    protected Writeable.Reader<DataStreamFeatureSetUsage> instanceReader() {
        return DataStreamFeatureSetUsage::new;
    }
}
