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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.xpack.core.action.DataStreamLifecycleUsageTransportAction.calculateStats;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamLifecycleFeatureSetUsageTests extends AbstractWireSerializingTestCase<DataStreamLifecycleFeatureSetUsage> {

    @Override
    protected DataStreamLifecycleFeatureSetUsage createTestInstance() {
        boolean isStateless = randomBoolean();
        return randomBoolean()
            ? new DataStreamLifecycleFeatureSetUsage(
                new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
                    randomNonNegativeLong(),
                    randomBoolean(),
                    generateTimeThresholdStats(),
                    generateTimeThresholdStats(),
                    isStateless ? null : generateTimeThresholdStats(),
                    randomBoolean() ? Map.of() : Map.of("default", generateGlobalRetention(), "max", generateGlobalRetention()),
                    isStateless ? null : randomNonNegativeLong()
                )
            )
            : DataStreamLifecycleFeatureSetUsage.DISABLED;
    }

    static DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats generateGlobalRetention() {
        return new DataStreamLifecycleFeatureSetUsage.GlobalRetentionStats(randomNonNegativeLong(), randomNonNegativeLong());
    }

    static DataStreamLifecycleFeatureSetUsage.TimeThresholdStats generateTimeThresholdStats() {
        return randomBoolean()
            ? DataStreamLifecycleFeatureSetUsage.TimeThresholdStats.NO_DATA
            : new DataStreamLifecycleFeatureSetUsage.TimeThresholdStats(
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
        var frozenAfterStats = instance.lifecycleStats.frozenAfterStats;
        var maxRetention = instance.lifecycleStats.globalRetentionStats.get("max");
        var defaultRetention = instance.lifecycleStats.globalRetentionStats.get("default");
        var minimumLifecycleEnabledCount = instance.lifecycleStats.minimumLifecycleEnabledCount;
        switch (randomInt(7)) {
            case 0 -> count += (count > 0 ? -1 : 1);
            case 1 -> defaultRollover = defaultRollover == false;
            case 2 -> dataRetentionStats = randomValueOtherThan(
                dataRetentionStats,
                DataStreamLifecycleFeatureSetUsageTests::generateTimeThresholdStats
            );
            case 3 -> effectiveRetentionStats = randomValueOtherThan(
                effectiveRetentionStats,
                DataStreamLifecycleFeatureSetUsageTests::generateTimeThresholdStats
            );
            case 4 -> {
                if (frozenAfterStats == null) {
                    frozenAfterStats = generateTimeThresholdStats();
                } else {
                    frozenAfterStats = randomBoolean()
                        ? null
                        : randomValueOtherThan(frozenAfterStats, DataStreamLifecycleFeatureSetUsageTests::generateTimeThresholdStats);
                }
            }
            case 5 -> maxRetention = randomValueOtherThan(maxRetention, DataStreamLifecycleFeatureSetUsageTests::generateGlobalRetention);
            case 6 -> defaultRetention = randomValueOtherThan(
                defaultRetention,
                DataStreamLifecycleFeatureSetUsageTests::generateGlobalRetention
            );
            case 7 -> {
                if (minimumLifecycleEnabledCount == null) {
                    minimumLifecycleEnabledCount = randomNonNegativeLong();
                } else {
                    minimumLifecycleEnabledCount = randomBoolean()
                        ? null
                        : minimumLifecycleEnabledCount + (minimumLifecycleEnabledCount > 0 ? -1 : 1);
                }
            }
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
            new DataStreamLifecycleFeatureSetUsage.LifecycleStats(
                count,
                defaultRollover,
                dataRetentionStats,
                effectiveRetentionStats,
                frozenAfterStats,
                map,
                minimumLifecycleEnabledCount
            )
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
                DataStreamLifecycle.dataLifecycleBuilder().enabled(true).dataRetention(TimeValue.timeValueSeconds(50)).build()
            ),
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                DataStreamLifecycle.dataLifecycleBuilder().enabled(true).dataRetention(TimeValue.timeValueMillis(150)).build()
            ),
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                DataStreamLifecycle.dataLifecycleBuilder().enabled(false).dataRetention(TimeValue.timeValueSeconds(5)).build()
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
            boolean isStateless = randomBoolean();
            RolloverConfiguration rollover = useDefault
                ? DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getDefault(Settings.EMPTY)
                : new RolloverConfiguration(new RolloverConditions());
            DataStreamLifecycleFeatureSetUsage.LifecycleStats stats = calculateStats(
                dataStreams,
                rollover,
                null,
                isStateless,
                randomBoolean()
            );

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

            if (isStateless) {
                assertThat(stats.frozenAfterStats, nullValue());
            } else {
                assertThat(stats.frozenAfterStats, equalTo(DataStreamLifecycleFeatureSetUsage.TimeThresholdStats.NO_DATA));
            }
        }

        // Test with global retention
        {
            boolean useDefault = randomBoolean();
            boolean isStateless = randomBoolean();
            RolloverConfiguration rollover = useDefault
                ? DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getDefault(Settings.EMPTY)
                : new RolloverConfiguration(new RolloverConditions());
            TimeValue defaultRetention = TimeValue.timeValueSeconds(10);
            TimeValue maxRetention = TimeValue.timeValueSeconds(20);
            DataStreamLifecycleFeatureSetUsage.LifecycleStats stats = calculateStats(
                dataStreams,
                rollover,
                new DataStreamGlobalRetention(defaultRetention, maxRetention),
                isStateless,
                randomBoolean()
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

            if (isStateless) {
                assertThat(stats.frozenAfterStats, nullValue());
            } else {
                assertThat(stats.frozenAfterStats, equalTo(DataStreamLifecycleFeatureSetUsage.TimeThresholdStats.NO_DATA));
            }
        }
    }

    public void testMinimumLifecycleEnabledCount() {
        RolloverConfiguration rollover = DataStreamLifecycle.CLUSTER_LIFECYCLE_DEFAULT_ROLLOVER_SETTING.getDefault(Settings.EMPTY);
        List<DataStream> dataStreams = List.of(
            // Affected: time series without a lifecycle
            timeSeriesDataStream(null),
            timeSeriesDataStream(null),
            // Not affected: time series with an explicit lifecycle, even when it's disabled
            timeSeriesDataStream(DataStreamLifecycle.dataLifecycleBuilder().enabled(true).build()),
            timeSeriesDataStream(DataStreamLifecycle.dataLifecycleBuilder().enabled(false).build()),
            // Not affected: only time series data streams can get the minimum lifecycle
            DataStreamTestHelper.newInstance(
                randomAlphaOfLength(10),
                List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())),
                1L,
                null,
                false,
                null
            )
        );

        assertThat(calculateStats(dataStreams, rollover, null, false, true).minimumLifecycleEnabledCount, is(2L));
        assertThat(calculateStats(dataStreams, rollover, null, false, false).minimumLifecycleEnabledCount, is(0L));
        assertThat(calculateStats(dataStreams, rollover, null, true, randomBoolean()).minimumLifecycleEnabledCount, nullValue());
    }

    private static DataStream timeSeriesDataStream(DataStreamLifecycle lifecycle) {
        return DataStream.builder(randomAlphaOfLength(10), List.of(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString())))
            .setIndexMode(IndexMode.TIME_SERIES)
            .setLifecycle(lifecycle)
            .build();
    }

    @Override
    protected Writeable.Reader<DataStreamLifecycleFeatureSetUsage> instanceReader() {
        return DataStreamLifecycleFeatureSetUsage::new;
    }

}
