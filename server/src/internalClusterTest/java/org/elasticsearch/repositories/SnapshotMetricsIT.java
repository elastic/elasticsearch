/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.hamcrest.Matcher;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThan;

public class SnapshotMetricsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    public void testUpdateRepository() throws Exception {
        final String repositoryName = randomIdentifier();

        createRepository(repositoryName, "mock");

        final String indexName = randomIdentifier();
        final int numShards = randomIntBetween(1, 10);
        final int numReplicas = randomIntBetween(0, 1);
        createIndex(indexName, numShards, numReplicas);

        indexRandom(true, indexName, randomIntBetween(100, 300));

        // Block the snapshot to test "snapshot shards in progress"
        MockRepository repository = asInstanceOf(MockRepository.class, getRepositoryOnMaster(repositoryName));
        repository.blockOnDataFiles();
        final String snapshotName = randomIdentifier();
        final long beforeCreateSnapshotNanos = System.nanoTime();
        try {
            clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshotName)
                .setIndices(indexName)
                .setWaitForCompletion(false)
                .get();

            waitForBlockOnAnyDataNode(repositoryName);
            collectMetrics();
            assertShardsInProgressMetricIs(hasItem(greaterThan(0L)));
        } finally {
            repository.unblock();
        }

        // wait for snapshot to finish to test the other metrics
        awaitNumberOfSnapshotsInProgress(0);
        final long snapshotElapsedTime = System.nanoTime() - beforeCreateSnapshotNanos;
        collectMetrics();

        // sanity check blobs and bytes metrics
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_BLOBS_UPLOADED), greaterThan(0L));
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_BYTES_UPLOADED), greaterThan(0L));

        // sanity check duration values
        final long upperBoundTimeSpentOnSnapshotThings = internalCluster().numDataNodes() * snapshotElapsedTime;
        assertThat(
            getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_UPLOAD_DURATION),
            allOf(greaterThan(0L), lessThan(upperBoundTimeSpentOnSnapshotThings))
        );
        assertThat(
            getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_UPLOAD_READ_DURATION),
            allOf(greaterThan(0L), lessThan(upperBoundTimeSpentOnSnapshotThings))
        );

        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_SHARDS_STARTED), equalTo((long) numShards));
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_SHARDS_COMPLETED), equalTo((long) numShards));

        assertShardsInProgressMetricIs(everyItem(equalTo(0L)));
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

    private static void collectMetrics() {
        allTestTelemetryPlugins().forEach(TestTelemetryPlugin::collect);
    }

    private long getTotalClusterLongCounterValue(String metricName) {
        return allTestTelemetryPlugins().flatMap(testTelemetryPlugin -> testTelemetryPlugin.getLongCounterMeasurement(metricName).stream())
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private static Stream<TestTelemetryPlugin> allTestTelemetryPlugins() {
        return StreamSupport.stream(internalCluster().getDataNodeInstances(PluginsService.class).spliterator(), false)
            .flatMap(pluginsService -> pluginsService.filterPlugins(TestTelemetryPlugin.class));
    }
}
