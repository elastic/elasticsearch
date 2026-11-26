/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.threadpool.ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * Tests that a node reports {@link RestoreMetrics} for real restores: that the node wires the same instance into {@link RestoreService}
 * and the telemetry provider, that the counters and histograms are recorded when a restore starts and completes, and that only the
 * elected master reports the gauges while a restore is running, including after the master changes.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RestoreMetricsIT extends AbstractSnapshotIntegTestCase {

    private static final Map<String, Object> NOT_REPORTING_SHARD_RESTORING = Map.of(RestoreMetrics.REPORT_SHARD_RESTORING_ATTRIBUTE, false);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // read the wall clock on every poll rather than a cached value, so that ages from different nodes can be compared closely
            .put(ESTIMATED_TIME_INTERVAL_SETTING.getKey(), "0s")
            .build();
    }

    /**
     * A restore in which every shard restores is counted by the master as started and as completed successfully, with its sizes.
     */
    public void testSuccessfulRestore() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final int numberOfShards = between(1, 3);
        final String indexName = createSnapshottedAndDeletedIndex(numberOfShards);

        final RestoreSnapshotResponse response = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY, SNAPSHOT)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .get();
        assertThat(response.getRestoreInfo().successfulShards(), equalTo(numberOfShards));

        assertThat(counterTotal(masterNode, RestoreMetrics.RESTORES_STARTED_TOTAL, NOT_REPORTING_SHARD_RESTORING), equalTo(1L));
        // the completion is recorded when the master removes the completed restore from the cluster state, after the response, and
        // its histograms are recorded after its counters, so wait for all of them
        assertBusy(() -> {
            assertThat(
                counterTotal(masterNode, RestoreMetrics.RESTORES_COMPLETED_TOTAL, Map.of(RestoreMetrics.OUTCOME_ATTRIBUTE, "success")),
                equalTo(1L)
            );
            assertOnlyHistogramValue(masterNode, RestoreMetrics.RESTORES_INDICES_HISTOGRAM, 1L, "success");
            assertOnlyHistogramValue(masterNode, RestoreMetrics.RESTORES_SHARDS_HISTOGRAM, numberOfShards, "success");
        });
        assertThat(counterTotal(masterNode, RestoreMetrics.RESTORES_SHARDS_COMPLETED_TOTAL, Map.of()), equalTo((long) numberOfShards));
        assertThat(plugin(masterNode).getLongCounterMeasurement(RestoreMetrics.RESTORES_SHARDS_FAILED_TOTAL), empty());

        // only the master records restores
        assertThat(plugin(dataNode).getLongCounterMeasurement(RestoreMetrics.RESTORES_STARTED_TOTAL), empty());
        assertThat(plugin(dataNode).getLongCounterMeasurement(RestoreMetrics.RESTORES_COMPLETED_TOTAL), empty());
    }

    /**
     * A restore whose shards no node may hold fails them as {@code no_eligible_node}, and is counted as failed.
     */
    public void testRestoreWithNoEligibleNode() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final int numberOfShards = between(1, 3);
        final String indexName = createSnapshottedAndDeletedIndex(numberOfShards);

        final RestoreSnapshotResponse response = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY, SNAPSHOT)
            .setIndices(indexName)
            .setIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", "no-such-node"))
            .setWaitForCompletion(true)
            .get();
        assertThat(response.getRestoreInfo().failedShards(), equalTo(numberOfShards));

        // the histograms are recorded after the counters, so wait for them too
        assertBusy(() -> {
            assertThat(
                counterTotal(masterNode, RestoreMetrics.RESTORES_COMPLETED_TOTAL, Map.of(RestoreMetrics.OUTCOME_ATTRIBUTE, "failure")),
                equalTo(1L)
            );
            assertOnlyHistogramValue(masterNode, RestoreMetrics.RESTORES_SHARDS_HISTOGRAM, numberOfShards, "failure");
        });
        assertThat(
            counterTotal(
                masterNode,
                RestoreMetrics.RESTORES_SHARDS_FAILED_TOTAL,
                Map.of(RestoreMetrics.FAILURE_REASON_ATTRIBUTE, RestoreInProgress.ShardRestoreFailureReason.NO_ELIGIBLE_NODE.value())
            ),
            equalTo((long) numberOfShards)
        );
    }

    /**
     * While a restore is blocked reading the repository, the master reports it in the gauges and the data node reports nothing. Once
     * it completes, the master reports no restores in progress.
     */
    public void testGaugesWhileRestoring() throws Exception {
        final String masterNode = internalCluster().startMasterOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final int numberOfShards = between(1, 3);
        final String indexName = createSnapshottedAndDeletedIndex(numberOfShards);

        blockAllDataNodes(REPOSITORY);
        final ActionFuture<RestoreSnapshotResponse> restore = clusterAdmin().prepareRestoreSnapshot(
            TEST_REQUEST_TIMEOUT,
            REPOSITORY,
            SNAPSHOT
        ).setIndices(indexName).setWaitForCompletion(true).execute();
        waitForBlockOnAnyDataNode(REPOSITORY);

        assertBusy(() -> assertRestoreInProgressReported(masterNode, numberOfShards));
        assertNoRestoreGauges(dataNode);

        unblockAllDataNodes(REPOSITORY);
        assertThat(restore.get().getRestoreInfo().successfulShards(), equalTo(numberOfShards));
        assertBusy(() -> assertThat(counterTotal(masterNode, RestoreMetrics.RESTORES_COMPLETED_TOTAL, Map.of()), equalTo(1L)));

        collectMetrics();
        assertThat(latestGauges(masterNode, RestoreMetrics.RESTORES_CURRENT).get(NOT_REPORTING_SHARD_RESTORING), equalTo(0L));
        assertThat(latestGauges(masterNode, RestoreMetrics.RESTORES_INDICES_CURRENT).get(Map.of()), equalTo(0L));
    }

    /**
     * When the master changes while a restore is running, the new master reports the restore in the gauges from the cluster state alone,
     * and counts its completion.
     */
    public void testGaugesMoveToNewMaster() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final int numberOfShards = between(1, 3);
        final String indexName = createSnapshottedAndDeletedIndex(numberOfShards);

        blockAllDataNodes(REPOSITORY);
        // do not wait for completion, since the request waiting for it is lost with the master
        clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, REPOSITORY, SNAPSHOT)
            .setIndices(indexName)
            .setWaitForCompletion(false)
            .get();
        waitForBlockOnAnyDataNode(REPOSITORY);

        final String oldMaster = internalCluster().getMasterName();
        assertBusy(() -> assertRestoreInProgressReported(oldMaster, numberOfShards));
        final long ageBeforeFailoverMillis = oldestAgeMillis(oldMaster);
        final long beforeFailoverNanos = System.nanoTime();

        internalCluster().stopCurrentMasterNode();
        ensureStableCluster(3);
        final String newMaster = internalCluster().getMasterName();
        assertThat(newMaster, not(equalTo(oldMaster)));

        final long elapsedBeforeNewMasterReportsMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - beforeFailoverNanos);
        assertBusy(() -> assertRestoreInProgressReported(newMaster, numberOfShards));
        // the shards are still in the attempt that was blocked before the election, and the new master measures its age from the time in
        // the cluster state, so the age carries on from where the old master left it rather than starting again; the margin allows for
        // the two nodes reading the wall clock separately
        assertThat(
            oldestAgeMillis(newMaster),
            greaterThanOrEqualTo(ageBeforeFailoverMillis + elapsedBeforeNewMasterReportsMillis - CLOCK_MARGIN_MILLIS)
        );
        for (String node : internalCluster().getNodeNames()) {
            if (node.equals(newMaster) == false) {
                assertNoRestoreGauges(node);
            }
        }

        unblockAllDataNodes(REPOSITORY);
        assertBusy(
            () -> assertThat(
                counterTotal(newMaster, RestoreMetrics.RESTORES_COMPLETED_TOTAL, Map.of(RestoreMetrics.OUTCOME_ATTRIBUTE, "success")),
                equalTo(1L)
            )
        );
        assertThat(plugin(dataNode).getLongCounterMeasurement(RestoreMetrics.RESTORES_COMPLETED_TOTAL), empty());
    }

    private static final String REPOSITORY = "repository";
    private static final String SNAPSHOT = "snapshot";
    private static final long CLOCK_MARGIN_MILLIS = 100;

    /** The latest oldest-shard age that {@code node} reported, without collecting again. */
    private static long oldestAgeMillis(String node) {
        final Long age = latestGauges(node, RestoreMetrics.RESTORES_SHARDS_OLDEST_AGE_CURRENT).get(Map.of());
        assertNotNull(node + " reported no oldest age", age);
        return age;
    }

    /**
     * Creates an index with enough documents that each shard has data files to read, snapshots it, and deletes it so it can be restored.
     */
    private String createSnapshottedAndDeletedIndex(int numberOfShards) {
        final String indexName = randomIndexName();
        createIndex(indexName, numberOfShards, 0);
        indexRandomDocs(indexName, between(100, 500));
        createRepository(REPOSITORY, "mock");
        createFullSnapshot(REPOSITORY, SNAPSHOT);
        assertAcked(indicesAdmin().prepareDelete(indexName));
        return indexName;
    }

    /** Asserts that {@code node} reports one restore of one index in progress, with every shard queued, restoring or restored. */
    private static void assertRestoreInProgressReported(String node, int numberOfShards) {
        collectMetrics();
        assertThat(latestGauges(node, RestoreMetrics.RESTORES_CURRENT).get(NOT_REPORTING_SHARD_RESTORING), equalTo(1L));
        assertThat(latestGauges(node, RestoreMetrics.RESTORES_INDICES_CURRENT).get(Map.of()), equalTo(1L));
        final Map<Map<String, Object>, Long> byState = latestGauges(node, RestoreMetrics.RESTORES_SHARDS_BY_STATE_CURRENT);
        final long restoring = shardsInState(byState, RestoreMetrics.ShardState.RESTORING);
        assertThat(restoring, greaterThanOrEqualTo(1L));
        assertThat(
            shardsInState(byState, RestoreMetrics.ShardState.QUEUED) + restoring + shardsInState(
                byState,
                RestoreMetrics.ShardState.SUCCEEDED
            ),
            equalTo((long) numberOfShards)
        );
        assertThat(shardsInState(byState, RestoreMetrics.ShardState.FAILED), equalTo(0L));
        assertThat(latestGauges(node, RestoreMetrics.RESTORES_SHARDS_RETRYING_CURRENT).get(Map.of()), equalTo(0L));
        assertThat(latestGauges(node, RestoreMetrics.RESTORES_SHARDS_OLDEST_AGE_CURRENT).get(Map.of()), greaterThanOrEqualTo(0L));
    }

    private static long shardsInState(Map<Map<String, Object>, Long> byState, RestoreMetrics.ShardState state) {
        final Long value = byState.get(Map.of(RestoreMetrics.SHARD_STATE_ATTRIBUTE, state.value()));
        assertNotNull("no measurement for shard state [" + state.value() + "] in " + byState, value);
        return value;
    }

    private static void assertNoRestoreGauges(String node) {
        collectMetrics();
        for (String gauge : List.of(
            RestoreMetrics.RESTORES_CURRENT,
            RestoreMetrics.RESTORES_INDICES_CURRENT,
            RestoreMetrics.RESTORES_SHARDS_BY_STATE_CURRENT,
            RestoreMetrics.RESTORES_SHARDS_RETRYING_CURRENT,
            RestoreMetrics.RESTORES_SHARDS_OLDEST_AGE_CURRENT
        )) {
            assertThat(node + " reported " + gauge, plugin(node).getLongAsyncGaugeMeasurement(gauge), empty());
        }
    }

    /** The latest value that {@code node} reported for the gauge, for each set of attributes. */
    private static Map<Map<String, Object>, Long> latestGauges(String node, String name) {
        final Map<Map<String, Object>, Long> latest = new HashMap<>();
        for (Measurement measurement : plugin(node).getLongAsyncGaugeMeasurement(name)) {
            latest.put(measurement.attributes(), measurement.getLong());
        }
        return latest;
    }

    /** The sum of the counter's increments on {@code node} whose attributes include all of {@code attributes}. */
    private static long counterTotal(String node, String name, Map<String, Object> attributes) {
        return plugin(node).getLongCounterMeasurement(name)
            .stream()
            .filter(measurement -> measurement.attributes().entrySet().containsAll(attributes.entrySet()))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private static void assertOnlyHistogramValue(String node, String name, long expectedValue, String outcome) {
        final List<Measurement> recorded = plugin(node).getLongHistogramMeasurement(name);
        assertThat(recorded.size(), equalTo(1));
        assertThat(recorded.getFirst().getLong(), equalTo(expectedValue));
        assertThat(recorded.getFirst().attributes(), equalTo(Map.of(RestoreMetrics.OUTCOME_ATTRIBUTE, outcome)));
    }

    private static void collectMetrics() {
        for (String node : internalCluster().getNodeNames()) {
            plugin(node).collect();
        }
    }

    private static TestTelemetryPlugin plugin(String node) {
        return internalCluster().getInstance(PluginsService.class, node).filterPlugins(TestTelemetryPlugin.class).findFirst().orElseThrow();
    }
}
