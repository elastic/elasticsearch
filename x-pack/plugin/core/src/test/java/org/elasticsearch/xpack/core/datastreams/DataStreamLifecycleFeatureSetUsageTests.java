/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.datastreams;

import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.xpack.core.action.DataStreamLifecycleUsageTransportAction.calculateStats;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DataStreamLifecycleFeatureSetUsageTests extends AbstractWireSerializingTestCase<DataStreamLifecycleFeatureSetUsage> {

    @Override
    protected DataStreamLifecycleFeatureSetUsage createTestInstance() {
        return randomBoolean()
            ? new DataStreamLifecycleFeatureSetUsage(
                new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
                    randomNonNegativeLong(),
                    randomBoolean(),
                    generateRetentionStats(),
                    generateRetentionStats(),
                    randomBoolean() ? Map.of() : Map.of("default", generateGlobalRetention(), "max", generateGlobalRetention())
                )
            )
            : DataStreamLifecycleFeatureSetUsage.DISABLED;
    }

    static DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats generateGlobalRetention() {
        return new DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats(randomNonNegativeLong(), randomNonNegativeLong());
    }

    static DataStreamLifecycleFeatureSetUsage.RetentionStats generateRetentionStats() {
        return randomBoolean()
            ? DataStreamLifecycleFeatureSetUsage.RetentionStats.NO_DATA
            : new DataStreamLifecycleFeatureSetUsage.RetentionStats(
                randomNonNegativeLong(),
                randomDoubleBetween(0.0, 110.0, false),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            );
    }

    @Override
    protected DataStreamLifecycleFeatureSetUsage mutateInstance(DataStreamLifecycleFeatureSetUsage instance) {
        if (instance.equals(DataStreamLifecycleFeatureSetUsage.DISABLED)) {
            return new DataStreamLifecycleFeatureSetUsage(DataStreamLifecycleFeatureSetUsage.LifecycleStats.INITIAL);
        }
        var count = instance.lifecycleStats.dataStreamsWithLifecyclesCount;
        var defaultRollover = instance.lifecycleStats.defaultRolloverUsed;
        var dataRetentionStats = instance.lifecycleStats.dataRetentionStats;
        var effectiveRetentionStats = instance.lifecycleStats.effectiveRetentionStats;
        var maxRetention = instance.lifecycleStats.globalRetentionStats.get("max");
        var defaultRetention = instance.lifecycleStats.globalRetentionStats.get("default");
        switch (randomInt(5)) {
            case 0 -> count += (count > 0 ? -1 : 1);
            case 1 -> defaultRollover = defaultRollover == false;
            case 2 -> dataRetentionStats = randomValueOtherThan(
                dataRetentionStats,
                DataStreamLifecycleFeatureSetUsageTests::generateRetentionStats
            );
            case 3 -> effectiveRetentionStats = randomValueOtherThan(
                effectiveRetentionStats,
                DataStreamLifecycleFeatureSetUsageTests::generateRetentionStats
            );
            case 4 -> maxRetention = randomValueOtherThan(maxRetention, DataStreamLifecycleFeatureSetUsageTests::generateGlobalRetention);
            case 5 -> defaultRetention = randomValueOtherThan(
                defaultRetention,
                DataStreamLifecycleFeatureSetUsageTests::generateGlobalRetention
            );
            default -> throw new RuntimeException("unreachable");
        }
        Map<String, DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats> map = new HashMap<>();
        if (defaultRetention != null) {
            map.put("default", defaultRetention);
        }
        if (maxRetention != null) {
            map.put("max", maxRetention);
        }
        return new DataStreamLifecycleFeatureSetUsage(
            new DataStreamLifecycleFeatureSetUsage.LifecycleStats(count, defaultRollover, dataRetentionStats, effectiveRetentionStats, map)
        );
    }

    public void testLifecycleStats() {
        List<DataStream> dataStreams = List.of(
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE
            ),
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                DataStreamLifecycle.createDataLifecycle(true, TimeValue.timeValueSeconds(50), null)
            ),
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                DataStreamLifecycle.createDataLifecycle(true, TimeValue.timeValueMillis(150), null)
            ),
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                DataStreamLifecycle.createDataLifecycle(false, TimeValue.timeValueSeconds(5), null)
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

        // Test empty global retention
        {
            boolean useDefault = randomBoolean();
            RolloverConfiguration rollover = useDefault
                ? DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getDefault(Settings.EMPTY)
                : new RolloverConfiguration(new RolloverConditions());
            DataStreamLifecycleFeatureSetUsage.LifecycleStats stats = calculateStats(dataStreams, rollover, null);

            assertThat(stats.dataStreamsWithLifecyclesCount, is(3L));
            assertThat(stats.defaultRolloverUsed, is(useDefault));
            // Data retention
            assertThat(stats.dataRetentionStats.dataStreamCount(), is(2L));
            assertThat(stats.dataRetentionStats.maxMillis(), is(50_000L));
            assertThat(stats.dataRetentionStats.minMillis(), is(150L));
            assertThat(stats.dataRetentionStats.avgMillis(), is(25_075.0));

            assertThat(stats.effectiveRetentionStats.dataStreamCount(), is(2L));
            assertThat(stats.effectiveRetentionStats.maxMillis(), is(50_000L));
            assertThat(stats.effectiveRetentionStats.minMillis(), is(150L));
            assertThat(stats.effectiveRetentionStats.avgMillis(), is(25_075.0));

            assertThat(stats.globalRetentionStats, equalTo(Map.of()));
        }

        // Test with global retention
        {
            boolean useDefault = randomBoolean();
            RolloverConfiguration rollover = useDefault
                ? DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getDefault(Settings.EMPTY)
                : new RolloverConfiguration(new RolloverConditions());
            TimeValue defaultRetention = TimeValue.timeValueSeconds(10);
            TimeValue maxRetention = TimeValue.timeValueSeconds(20);
            DataStreamLifecycleFeatureSetUsage.LifecycleStats stats = calculateStats(
                dataStreams,
                rollover,
                new DataStreamGlobalRetention(defaultRetention, maxRetention)
            );

            assertThat(stats.dataStreamsWithLifecyclesCount, is(3L));
            assertThat(stats.defaultRolloverUsed, is(useDefault));
            // Data retention
            assertThat(stats.dataRetentionStats.dataStreamCount(), is(2L));
            assertThat(stats.dataRetentionStats.maxMillis(), is(50_000L));
            assertThat(stats.dataRetentionStats.minMillis(), is(150L));
            assertThat(stats.dataRetentionStats.avgMillis(), is(25_075.0));

            // Effective retention
            assertThat(stats.effectiveRetentionStats.dataStreamCount(), is(3L));
            assertThat(stats.effectiveRetentionStats.maxMillis(), is(20_000L));
            assertThat(stats.effectiveRetentionStats.minMillis(), is(150L));
            assertThat(stats.effectiveRetentionStats.avgMillis(), is(10_050.0));

            assertThat(
                stats.globalRetentionStats,
                equalTo(
                    Map.of(
                        "default",
                        new DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats(1L, 10_000L),
                        "max",
                        new DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats(1L, 20_000L)
                    )
                )
            );
        }
    }

    @Override
    protected Writeable.Reader<DataStreamLifecycleFeatureSetUsage> instanceReader() {
        return DataStreamLifecycleFeatureSetUsage::new;
    }

}
