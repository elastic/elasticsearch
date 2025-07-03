/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.threadpool.ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SnapshotMetricsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Make sanity checking duration histograms possible
            .put(ESTIMATED_TIME_INTERVAL_SETTING.getKey(), "0s")
            .build();
    }

    public void testSnapshotAPMMetrics() throws Exception {
        final String indexName = randomIdentifier();
        final int numShards = randomIntBetween(1, 10);
        final int numReplicas = randomIntBetween(0, 1);
        createIndex(indexName, numShards, numReplicas);

        indexRandom(true, indexName, randomIntBetween(100, 300));

        IndicesStatsResponse indicesStats = indicesAdmin().prepareStats(indexName).get();
        IndexStats indexStats = indicesStats.getIndex(indexName);
        long totalSizeInBytes = 0;
        for (ShardStats shard : indexStats.getShards()) {
            totalSizeInBytes += shard.getStats().getStore().sizeInBytes();
        }
        logger.info("--> total shards size: {} bytes", totalSizeInBytes);

        final String repositoryName = randomIdentifier();

        // we want to ensure some throttling, but not so much that it makes the test excessively slow.
        // 3 seemed a reasonable multiple to ensure that.
        final int shardSizeMultipleToEnsureThrottling = 3;
        createRepository(
            repositoryName,
            "mock",
            randomRepositorySettings().put(
                "max_snapshot_bytes_per_sec",
                ByteSizeValue.ofBytes(totalSizeInBytes * shardSizeMultipleToEnsureThrottling)
            ).put("max_restore_bytes_per_sec", ByteSizeValue.ofBytes(totalSizeInBytes * shardSizeMultipleToEnsureThrottling))
        );

        // Block the snapshot to test "snapshot shards in progress"
        blockAllDataNodes(repositoryName);
        final String snapshotName = randomIdentifier();
        final long beforeCreateSnapshotNanos = System.nanoTime();
        try {
            clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshotName)
                .setIndices(indexName)
                .setWaitForCompletion(false)
                .get();

            waitForBlockOnAnyDataNode(repositoryName);
            collectMetrics();
            assertSnapshotsInProgressMetricIs(greaterThan(0L));
            assertShardsInProgressMetricIs(hasItem(greaterThan(0L)));
            assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOTS_STARTED), equalTo(1L));
            assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOTS_COMPLETED), equalTo(0L));
            assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_SHARDS_STARTED), greaterThan(0L));
            assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_SHARDS_COMPLETED), equalTo(0L));
        } finally {
            unblockAllDataNodes(repositoryName);
        }

        // wait for snapshot to finish to test the other metrics
        awaitNumberOfSnapshotsInProgress(0);
        final long snapshotElapsedTimeNanos = System.nanoTime() - beforeCreateSnapshotNanos;
        collectMetrics();

        // sanity check blobs, bytes and throttling metrics
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_BLOBS_UPLOADED), greaterThan(0L));
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_BYTES_UPLOADED), greaterThan(0L));
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_CREATE_THROTTLE_DURATION), greaterThan(0L));
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_RESTORE_THROTTLE_DURATION), equalTo(0L));

        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOTS_STARTED), equalTo(1L));
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOTS_COMPLETED), equalTo(1L));

        // Sanity check shard duration observations
        assertDoubleHistogramMetrics(SnapshotMetrics.SNAPSHOT_SHARDS_DURATION, hasSize(numShards));
        assertDoubleHistogramMetrics(
            SnapshotMetrics.SNAPSHOT_SHARDS_DURATION,
            everyItem(lessThan(TimeValue.timeValueNanos(snapshotElapsedTimeNanos).secondsFrac()))
        );

        // Sanity check snapshot observations
        assertDoubleHistogramMetrics(SnapshotMetrics.SNAPSHOT_DURATION, hasSize(1));
        assertDoubleHistogramMetrics(
            SnapshotMetrics.SNAPSHOT_DURATION,
            everyItem(lessThan(TimeValue.timeValueNanos(snapshotElapsedTimeNanos).secondsFrac()))
        );

        // Work out the maximum amount of concurrency per node
        final ThreadPool tp = internalCluster().getDataNodeInstance(ThreadPool.class);
        int snapshotThreadPoolSize = tp.info(ThreadPool.Names.SNAPSHOT).getMax();
        int maximumPerNodeConcurrency = Math.max(snapshotThreadPoolSize, numShards);

        // sanity check duration values
        final long upperBoundTimeSpentOnSnapshotThingsNanos = internalCluster().numDataNodes() * maximumPerNodeConcurrency
            * snapshotElapsedTimeNanos;
        assertThat(
            getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_UPLOAD_DURATION),
            allOf(greaterThan(0L), lessThan(upperBoundTimeSpentOnSnapshotThingsNanos))
        );
        assertThat(
            getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_UPLOAD_READ_DURATION),
            allOf(greaterThan(0L), lessThan(upperBoundTimeSpentOnSnapshotThingsNanos))
        );

        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_SHARDS_STARTED), equalTo((long) numShards));
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_SHARDS_COMPLETED), equalTo((long) numShards));

        assertSnapshotsInProgressMetricIs(equalTo(0L));
        assertShardsInProgressMetricIs(everyItem(equalTo(0L)));

        // Restore the snapshot
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshotName)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .setRenamePattern("(.+)")
            .setRenameReplacement("restored-$1")
            .get();
        collectMetrics();

        // assert we throttled on restore
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_RESTORE_THROTTLE_DURATION), greaterThan(0L));
    }

    private static void assertDoubleHistogramMetrics(String metricName, Matcher<? super List<Double>> matcher) {
        final List<Double> values = allTestTelemetryPlugins().flatMap(testTelemetryPlugin -> {
            final List<Measurement> doubleHistogramMeasurement = testTelemetryPlugin.getDoubleHistogramMeasurement(metricName);
            return doubleHistogramMeasurement.stream().map(Measurement::getDouble);
        }).toList();
        assertThat(values, matcher);
    }

    private static void assertShardsInProgressMetricIs(Matcher<? super List<Long>> matcher) {
        final List<Long> values = allTestTelemetryPlugins().map(testTelemetryPlugin -> {
            final List<Measurement> longGaugeMeasurement = testTelemetryPlugin.getLongGaugeMeasurement(
                SnapshotMetrics.SNAPSHOT_SHARDS_IN_PROGRESS
            );
            return longGaugeMeasurement.getLast().getLong();
        }).toList();
        assertThat(values, matcher);
    }

    private static void assertSnapshotsInProgressMetricIs(Matcher<Long> matcher) {
        final List<Long> values = internalCluster().getCurrentMasterNodeInstance(PluginsService.class)
            .filterPlugins(TestTelemetryPlugin.class)
            .map(testTelemetryPlugin -> {
                final List<Measurement> longGaugeMeasurement = testTelemetryPlugin.getLongGaugeMeasurement(
                    SnapshotMetrics.SNAPSHOTS_IN_PROGRESS
                );
                return longGaugeMeasurement.getLast().getLong();
            })
            .toList();
        assertThat(values, hasSize(1));
        assertThat(values.getFirst(), matcher);
    }

    private static void collectMetrics() {
        allTestTelemetryPlugins().forEach(TestTelemetryPlugin::collect);
    }

    private long getTotalClusterLongCounterValue(String metricName) {
        return allTestTelemetryPlugins().flatMap(testTelemetryPlugin -> testTelemetryPlugin.getLongCounterMeasurement(metricName).stream())
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private static Stream<TestTelemetryPlugin> allTestTelemetryPlugins() {
        return StreamSupport.stream(internalCluster().getDataOrMasterNodeInstances(PluginsService.class).spliterator(), false)
            .flatMap(pluginsService -> pluginsService.filterPlugins(TestTelemetryPlugin.class));
    }
}
