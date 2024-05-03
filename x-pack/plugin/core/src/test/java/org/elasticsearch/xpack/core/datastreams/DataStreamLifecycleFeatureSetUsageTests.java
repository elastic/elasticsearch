/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.UUID;

import static org.elasticsearch.xpack.core.action.DataStreamLifecycleUsageTransportAction.calculateStats;
import static org.hamcrest.Matchers.is;

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

    public void testLifecycleStats() {
        List<DataStream> dataStreams = List.of(
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                new DataStreamLifecycle()
            ),
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                new DataStreamLifecycle(new DataStreamLifecycle.Retention(TimeValue.timeValueMillis(1000)), null, true)
            ),
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                new DataStreamLifecycle(new DataStreamLifecycle.Retention(TimeValue.timeValueMillis(100)), null, true)
            ),
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                new DataStreamLifecycle(new DataStreamLifecycle.Retention(TimeValue.timeValueMillis(5000)), null, false)
            ),
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                null
            )
        );

        Tuple<Long, LongSummaryStatistics> stats = calculateStats(dataStreams);
        // 3 data streams with an enabled lifecycle
        assertThat(stats.v1(), is(3L));
        LongSummaryStatistics longSummaryStatistics = stats.v2();
        assertThat(longSummaryStatistics.getMax(), is(1000L));
        assertThat(longSummaryStatistics.getMin(), is(100L));
        // only counting the ones with an effective retention in the summary statistics
        assertThat(longSummaryStatistics.getCount(), is(2L));
        assertThat(longSummaryStatistics.getAverage(), is(550.0));
    }

    @Override
    protected Writeable.Reader<DataStreamLifecycleFeatureSetUsage> instanceReader() {
        return DataStreamLifecycleFeatureSetUsage::new;
    }

}
