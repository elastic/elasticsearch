/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.snapshots.SnapshotTestUtils.flushMasterQueue;
import static org.elasticsearch.snapshots.SnapshotTestUtils.putShutdownForRemovalMetadata;
import static org.elasticsearch.threadpool.ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SnapshotMetricsIT extends AbstractSnapshotIntegTestCase {

    private static final String REQUIRE_NODE_NAME_SETTING = IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(TestTelemetryPlugin.class, MockTransportService.TestPlugin.class))
            .toList();
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

        indexRandom(true, indexName, randomIntBetween(3000, 5000));

        final String repositoryName = randomIdentifier();
        createRepository(
            repositoryName,
            "mock",
            Settings.builder()
                .put(randomRepositorySettings().build())
                // Making chunk size small and adding throttling increases the likelihood of upload duration being non-zero
                .put("chunk_size", ByteSizeValue.ofKb(1))
                .put(BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey(), ByteSizeValue.ofMb(1))
                .put(BlobStoreRepository.MAX_RESTORE_BYTES_PER_SEC.getKey(), ByteSizeValue.ofMb(1))
        );

        // Block the snapshot to test "snapshot shards in progress"
        blockAllDataNodes(repositoryName);
        final String snapshotName = randomIdentifier();
        final long beforeCreateSnapshotNanos = System.nanoTime();
        final ActionFuture<CreateSnapshotResponse> snapshotFuture;
        try {
            snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshotName)
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .execute();

            // We are able to wait for either the creation to complete (`wait_for_completion=false`), or the snapshot to complete
            // (`wait_for_completion=true`), but not both. To know when the creation listeners complete, we must assertBusy
            assertBusy(() -> {
                collectMetrics();
                assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOTS_STARTED), equalTo(1L));
                assertShardsInProgressMetricIs(hasItem(greaterThan(0L)));
            });
            assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOTS_COMPLETED), equalTo(0L));
            assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_SHARDS_STARTED), greaterThan(0L));
            assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_SHARDS_COMPLETED), equalTo(0L));
        } finally {
            unblockAllDataNodes(repositoryName);
        }

        // wait for snapshot to finish to test the other metrics
        safeGet(snapshotFuture);
        final TimeValue snapshotElapsedTime = TimeValue.timeValueNanos(System.nanoTime() - beforeCreateSnapshotNanos);
        collectMetrics();

        // sanity check blobs, bytes and throttling metrics
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_BLOBS_UPLOADED), greaterThan(0L));
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_BYTES_UPLOADED), greaterThan(0L));

        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOTS_STARTED), equalTo(1L));
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOTS_COMPLETED), equalTo(1L));

        // Sanity check shard duration observations
        assertDoubleHistogramMetrics(SnapshotMetrics.SNAPSHOT_SHARDS_DURATION, hasSize(numShards));
        assertDoubleHistogramMetrics(SnapshotMetrics.SNAPSHOT_SHARDS_DURATION, everyItem(lessThan(snapshotElapsedTime.secondsFrac())));

        // Sanity check snapshot observations
        assertDoubleHistogramMetrics(SnapshotMetrics.SNAPSHOT_DURATION, hasSize(1));
        assertDoubleHistogramMetrics(SnapshotMetrics.SNAPSHOT_DURATION, everyItem(lessThan(snapshotElapsedTime.secondsFrac())));

        // Work out the maximum amount of concurrency per node
        final ThreadPool tp = internalCluster().getDataNodeInstance(ThreadPool.class);
        final int snapshotThreadPoolSize = tp.info(ThreadPool.Names.SNAPSHOT).getMax();
        final int maximumPerNodeConcurrency = Math.max(snapshotThreadPoolSize, numShards);

        // sanity check duration values
        final long upperBoundTimeSpentOnSnapshotThingsMillis = internalCluster().numDataNodes() * maximumPerNodeConcurrency
            * snapshotElapsedTime.millis();
        assertThat(
            getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_UPLOAD_DURATION),
            allOf(greaterThan(0L), lessThan(upperBoundTimeSpentOnSnapshotThingsMillis))
        );

        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_SHARDS_STARTED), equalTo((long) numShards));
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_SHARDS_COMPLETED), equalTo((long) numShards));

        assertShardsInProgressMetricIs(everyItem(equalTo(0L)));

        // assert appropriate attributes are present
        final Map<String, Object> expectedAttrs = Map.of("repo_name", repositoryName, "repo_type", "mock");
        final Map<String, Object> expectedAttrsWithShardStage = Maps.copyMapWithAddedEntry(
            expectedAttrs,
            "stage",
            IndexShardSnapshotStatus.Stage.DONE.name()
        );
        final Map<String, Object> expectedAttrsWithSnapshotState = Maps.copyMapWithAddedEntry(
            expectedAttrs,
            "state",
            SnapshotState.SUCCESS.name()
        );
        assertMetricsHaveAttributes(InstrumentType.LONG_COUNTER, SnapshotMetrics.SNAPSHOTS_STARTED, expectedAttrs);
        assertMetricsHaveAttributes(InstrumentType.LONG_COUNTER, SnapshotMetrics.SNAPSHOTS_COMPLETED, expectedAttrsWithSnapshotState);
        assertMetricsHaveAttributes(InstrumentType.DOUBLE_HISTOGRAM, SnapshotMetrics.SNAPSHOT_DURATION, expectedAttrsWithSnapshotState);

        assertMetricsHaveAttributes(InstrumentType.LONG_COUNTER, SnapshotMetrics.SNAPSHOT_SHARDS_STARTED, expectedAttrs);
        assertMetricsHaveAttributes(InstrumentType.LONG_GAUGE, SnapshotMetrics.SNAPSHOT_SHARDS_IN_PROGRESS, expectedAttrs);
        assertMetricsHaveAttributes(InstrumentType.LONG_COUNTER, SnapshotMetrics.SNAPSHOT_SHARDS_COMPLETED, expectedAttrsWithShardStage);
        assertMetricsHaveAttributes(InstrumentType.DOUBLE_HISTOGRAM, SnapshotMetrics.SNAPSHOT_SHARDS_DURATION, expectedAttrsWithShardStage);

        assertMetricsHaveAttributes(InstrumentType.LONG_COUNTER, SnapshotMetrics.SNAPSHOT_UPLOAD_DURATION, expectedAttrs);
        assertMetricsHaveAttributes(InstrumentType.LONG_COUNTER, SnapshotMetrics.SNAPSHOT_BYTES_UPLOADED, expectedAttrs);
        assertMetricsHaveAttributes(InstrumentType.LONG_COUNTER, SnapshotMetrics.SNAPSHOT_BLOBS_UPLOADED, expectedAttrs);
    }

    public void testThrottlingMetrics() throws Exception {
        final String indexName = randomIdentifier();
        final int numShards = randomIntBetween(1, 10);
        final int numReplicas = randomIntBetween(0, 1);
        createIndex(indexName, numShards, numReplicas);
        indexRandom(true, indexName, randomIntBetween(100, 120));

        // Create a repository with restrictive throttling settings
        final String repositoryName = randomIdentifier();
        final Settings.Builder repositorySettings = randomRepositorySettings().put(
            BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey(),
            ByteSizeValue.ofKb(2)
        )
            .put(BlobStoreRepository.MAX_RESTORE_BYTES_PER_SEC.getKey(), ByteSizeValue.ofKb(2))
            // Small chunk size ensures we don't get stuck throttling for too long
            .put("chunk_size", ByteSizeValue.ofBytes(100));
        createRepository(repositoryName, "mock", repositorySettings, false);

        final String snapshotName = randomIdentifier();
        final ActionFuture<CreateSnapshotResponse> snapshotFuture;

        // Kick off a snapshot
        final long snapshotStartTime = System.currentTimeMillis();
        snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshotName)
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();

        // Poll until we see some throttling occurring
        final long snap_ts0 = System.currentTimeMillis();
        assertBusy(() -> {
            collectMetrics();
            assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_CREATE_THROTTLE_DURATION), greaterThan(0L));
        });
        assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_RESTORE_THROTTLE_DURATION), equalTo(0L));

        // Remove create throttling
        final long snap_ts1 = System.currentTimeMillis();
        createRepository(
            repositoryName,
            "mock",
            repositorySettings.put(BlobStoreRepository.MAX_SNAPSHOT_BYTES_PER_SEC.getKey(), ByteSizeValue.ZERO),
            false
        );
        final long snap_ts2 = System.currentTimeMillis();

        // wait for the snapshot to finish
        safeGet(snapshotFuture);
        final long snap_ts3 = System.currentTimeMillis();

        logger.info(
            "saw throttling in [{}] remove throttling took [{}], snapshot took [{}]",
            TimeValue.timeValueMillis(snap_ts1 - snap_ts0),
            TimeValue.timeValueMillis(snap_ts2 - snap_ts1),
            TimeValue.timeValueMillis(snap_ts3 - snap_ts2)
        );

        // Work out the maximum amount of concurrency per node
        final ThreadPool tp = internalCluster().getDataNodeInstance(ThreadPool.class);
        final int snapshotThreadPoolSize = tp.info(ThreadPool.Names.SNAPSHOT).getMax();
        final int maximumPerNodeConcurrency = Math.max(snapshotThreadPoolSize, numShards);

        // we should also have incurred some read duration due to the throttling
        final long upperBoundTimeSpentOnSnapshotThingsMillis = internalCluster().numDataNodes() * maximumPerNodeConcurrency * (System
            .currentTimeMillis() - snapshotStartTime);
        assertThat(
            getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_UPLOAD_READ_DURATION),
            allOf(greaterThan(0L), lessThan(upperBoundTimeSpentOnSnapshotThingsMillis))
        );

        // Restore the snapshot
        final long restore_ts0 = System.currentTimeMillis();
        ActionFuture<RestoreSnapshotResponse> restoreFuture = clusterAdmin().prepareRestoreSnapshot(
            TEST_REQUEST_TIMEOUT,
            repositoryName,
            snapshotName
        ).setIndices(indexName).setWaitForCompletion(true).setRenamePattern("(.+)").setRenameReplacement("restored-$1").execute();

        final long restore_ts1 = System.currentTimeMillis();
        // assert we throttled on restore
        assertBusy(() -> {
            collectMetrics();
            assertThat(getTotalClusterLongCounterValue(SnapshotMetrics.SNAPSHOT_RESTORE_THROTTLE_DURATION), greaterThan(0L));
        });

        final long restore_ts2 = System.currentTimeMillis();
        // Remove restore throttling
        createRepository(
            repositoryName,
            "mock",
            repositorySettings.put(BlobStoreRepository.MAX_RESTORE_BYTES_PER_SEC.getKey(), ByteSizeValue.ZERO),
            false
        );
        safeGet(restoreFuture);
        final long restore_ts3 = System.currentTimeMillis();

        logger.info(
            "saw throttling in [{}] remove throttling took [{}], restore took [{}]",
            TimeValue.timeValueMillis(restore_ts1 - restore_ts0),
            TimeValue.timeValueMillis(restore_ts2 - restore_ts1),
            TimeValue.timeValueMillis(restore_ts3 - restore_ts2)
        );

        // assert appropriate attributes are present
        final Map<String, Object> expectedAttrs = Map.of("repo_name", repositoryName, "repo_type", "mock");
        assertMetricsHaveAttributes(InstrumentType.LONG_COUNTER, SnapshotMetrics.SNAPSHOT_UPLOAD_READ_DURATION, expectedAttrs);
        assertMetricsHaveAttributes(InstrumentType.LONG_COUNTER, SnapshotMetrics.SNAPSHOT_RESTORE_THROTTLE_DURATION, expectedAttrs);
        assertMetricsHaveAttributes(InstrumentType.LONG_COUNTER, SnapshotMetrics.SNAPSHOT_CREATE_THROTTLE_DURATION, expectedAttrs);
    }

    public void testByStateCounts_InitAndQueuedShards() {
        final String indexName = randomIdentifier();
        final int numShards = randomIntBetween(2, 10);
        final int numReplicas = randomIntBetween(0, 1);
        createIndex(indexName, numShards, numReplicas);

        indexRandom(true, indexName, randomIntBetween(100, 300));

        final String repositoryName = randomIdentifier();
        createRepository(repositoryName, "mock");
        // Block repo reads so we can queue snapshots
        blockAllDataNodes(repositoryName);

        final String snapshotName = randomIdentifier();
        final ActionFuture<CreateSnapshotResponse> firstSnapshotFuture;
        final ActionFuture<CreateSnapshotResponse> secondSnapshotFuture;
        try {
            firstSnapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshotName)
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .execute();

            waitForBlockOnAnyDataNode(repositoryName);
            safeAwait(
                (ActionListener<Void> l) -> flushMasterQueue(internalCluster().getCurrentMasterNodeInstance(ClusterService.class), l)
            );

            // Should be {numShards} in INIT state, and 1 STARTED snapshot
            Map<SnapshotsInProgress.ShardState, Long> shardStates = getShardStates();
            assertThat(shardStates.get(SnapshotsInProgress.ShardState.INIT), equalTo((long) numShards));
            Map<SnapshotsInProgress.State, Long> snapshotStates = getSnapshotStates();
            assertThat(snapshotStates.get(SnapshotsInProgress.State.STARTED), equalTo(1L));

            // Queue up another snapshot
            secondSnapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, randomIdentifier())
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .execute();

            awaitNumberOfSnapshotsInProgress(2);

            // Should be {numShards} in QUEUED and INIT states, and 2 STARTED snapshots
            shardStates = getShardStates();
            assertThat(shardStates.get(SnapshotsInProgress.ShardState.INIT), equalTo((long) numShards));
            assertThat(shardStates.get(SnapshotsInProgress.ShardState.QUEUED), equalTo((long) numShards));
            snapshotStates = getSnapshotStates();
            assertThat(snapshotStates.get(SnapshotsInProgress.State.STARTED), equalTo(2L));
        } finally {
            unblockAllDataNodes(repositoryName);
        }

        // All statuses should return to zero when the snapshots complete
        safeGet(firstSnapshotFuture);
        safeGet(secondSnapshotFuture);
        getShardStates().forEach((key, value) -> assertThat(value, equalTo(0L)));
        getSnapshotStates().forEach((key, value) -> assertThat(value, equalTo(0L)));

        // Ensure all common attributes are present
        assertMetricsHaveAttributes(
            InstrumentType.LONG_GAUGE,
            SnapshotMetrics.SNAPSHOT_SHARDS_BY_STATE,
            Map.of("repo_name", repositoryName, "repo_type", "mock")
        );
        assertMetricsHaveAttributes(
            InstrumentType.LONG_GAUGE,
            SnapshotMetrics.SNAPSHOTS_BY_STATE,
            Map.of("repo_name", repositoryName, "repo_type", "mock")
        );
    }

    public void testByStateCounts_PausedForRemovalShards() throws Exception {
        final String indexName = randomIdentifier();
        final int numShards = randomIntBetween(2, 10);
        final int numReplicas = randomIntBetween(0, 1);

        final String nodeForRemoval = internalCluster().startDataOnlyNode();

        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                .put(REQUIRE_NODE_NAME_SETTING, nodeForRemoval)
                .build()
        );
        indexRandom(true, indexName, randomIntBetween(100, 300));

        final String repositoryName = randomIdentifier();
        createRepository(repositoryName, "mock");

        // block the node to be removed
        blockNodeOnAnyFiles(repositoryName, nodeForRemoval);

        final ClusterService clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture;
        try {
            // Kick off a snapshot
            snapshotFuture = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, randomIdentifier())
                .setIndices(indexName)
                .setWaitForCompletion(true)
                .execute();

            // Wait till we're blocked
            waitForBlock(nodeForRemoval, repositoryName);

            // Put shutdown metadata
            putShutdownForRemovalMetadata(nodeForRemoval, clusterService);
        } finally {
            unblockAllDataNodes(repositoryName);
        }

        // Wait for snapshot to be paused
        safeAwait(createSnapshotPausedListener(clusterService, repositoryName, indexName, numShards));

        final Map<SnapshotsInProgress.ShardState, Long> shardStates = getShardStates();
        assertThat(shardStates.get(SnapshotsInProgress.ShardState.PAUSED_FOR_NODE_REMOVAL), equalTo((long) numShards));
        final Map<SnapshotsInProgress.State, Long> snapshotStates = getSnapshotStates();
        assertThat(snapshotStates.get(SnapshotsInProgress.State.STARTED), equalTo(1L));

        // clear shutdown metadata to allow snapshot to complete
        clearShutdownMetadata(clusterService);

        // All statuses should return to zero when the snapshot completes
        safeGet(snapshotFuture);
        getShardStates().forEach((key, value) -> assertThat(value, equalTo(0L)));
        getSnapshotStates().forEach((key, value) -> assertThat(value, equalTo(0L)));

        // Ensure all common attributes are present
        assertMetricsHaveAttributes(
            InstrumentType.LONG_GAUGE,
            SnapshotMetrics.SNAPSHOT_SHARDS_BY_STATE,
            Map.of("repo_name", repositoryName, "repo_type", "mock")
        );
        assertMetricsHaveAttributes(
            InstrumentType.LONG_GAUGE,
            SnapshotMetrics.SNAPSHOTS_BY_STATE,
            Map.of("repo_name", repositoryName, "repo_type", "mock")
        );
    }

    public void testByStateCounts_WaitingShards() {
        final String indexName = randomIdentifier();
        final String boundNode = internalCluster().startDataOnlyNode();
        final String destinationNode = internalCluster().startDataOnlyNode();

        // Create with single shard so we can reliably delay relocation
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(REQUIRE_NODE_NAME_SETTING, boundNode)
                .build()
        );
        indexRandom(true, indexName, randomIntBetween(100, 300));

        final String repositoryName = randomIdentifier();
        createRepository(repositoryName, "mock");

        final MockTransportService transportService = MockTransportService.getInstance(destinationNode);
        final CyclicBarrier handoffRequestBarrier = new CyclicBarrier(2);
        transportService.addRequestHandlingBehavior(
            PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT,
            (handler, request, channel, task) -> {
                safeAwait(handoffRequestBarrier);
                safeAwait(handoffRequestBarrier);
                handler.messageReceived(request, channel, task);
            }
        );

        // Force the index to move to another node
        client().admin()
            .indices()
            .prepareUpdateSettings(indexName)
            .setSettings(Settings.builder().put(REQUIRE_NODE_NAME_SETTING, destinationNode).build())
            .get();

        // Wait for hand-off request to be blocked (the shard should be relocating now)
        safeAwait(handoffRequestBarrier);

        // Kick off a snapshot
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = clusterAdmin().prepareCreateSnapshot(
            TEST_REQUEST_TIMEOUT,
            repositoryName,
            randomIdentifier()
        ).setIndices(indexName).setWaitForCompletion(true).execute();

        // Wait for the snapshot to start
        awaitNumberOfSnapshotsInProgress(1);

        // Wait till we see a shard in WAITING state
        createSnapshotInStateListener(clusterService(), repositoryName, indexName, 1, SnapshotsInProgress.ShardState.WAITING);

        // Metrics should have 1 WAITING shard and 1 STARTED snapshot
        final Map<SnapshotsInProgress.ShardState, Long> shardStates = getShardStates();
        assertThat(shardStates.get(SnapshotsInProgress.ShardState.WAITING), equalTo(1L));
        final Map<SnapshotsInProgress.State, Long> snapshotStates = getSnapshotStates();
        assertThat(snapshotStates.get(SnapshotsInProgress.State.STARTED), equalTo(1L));

        // allow the relocation to complete
        safeAwait(handoffRequestBarrier);

        // All statuses should return to zero when the snapshot completes
        safeGet(snapshotFuture);
        getShardStates().forEach((key, value) -> assertThat(value, equalTo(0L)));
        getSnapshotStates().forEach((key, value) -> assertThat(value, equalTo(0L)));

        // Ensure all common attributes are present
        assertMetricsHaveAttributes(
            InstrumentType.LONG_GAUGE,
            SnapshotMetrics.SNAPSHOT_SHARDS_BY_STATE,
            Map.of("repo_name", repositoryName, "repo_type", "mock")
        );
        assertMetricsHaveAttributes(
            InstrumentType.LONG_GAUGE,
            SnapshotMetrics.SNAPSHOTS_BY_STATE,
            Map.of("repo_name", repositoryName, "repo_type", "mock")
        );
    }

    private Map<SnapshotsInProgress.ShardState, Long> getShardStates() {
        collectMetrics();

        return allTestTelemetryPlugins().flatMap(testTelemetryPlugin -> {
            final List<Measurement> longGaugeMeasurement = testTelemetryPlugin.getLongGaugeMeasurement(
                SnapshotMetrics.SNAPSHOT_SHARDS_BY_STATE
            );
            final Map<SnapshotsInProgress.ShardState, Long> shardStates = new HashMap<>();
            // last one in wins
            for (Measurement measurement : longGaugeMeasurement) {
                shardStates.put(
                    SnapshotsInProgress.ShardState.valueOf(measurement.attributes().get("state").toString()),
                    measurement.getLong()
                );
            }
            return shardStates.entrySet().stream();
        }).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
    }

    private Map<SnapshotsInProgress.State, Long> getSnapshotStates() {
        collectMetrics();

        return allTestTelemetryPlugins().flatMap(testTelemetryPlugin -> {
            final List<Measurement> longGaugeMeasurement = testTelemetryPlugin.getLongGaugeMeasurement(SnapshotMetrics.SNAPSHOTS_BY_STATE);
            final Map<SnapshotsInProgress.State, Long> shardStates = new HashMap<>();
            // last one in wins
            for (Measurement measurement : longGaugeMeasurement) {
                shardStates.put(SnapshotsInProgress.State.valueOf(measurement.attributes().get("state").toString()), measurement.getLong());
            }
            return shardStates.entrySet().stream();
        }).collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
    }

    private static void assertMetricsHaveAttributes(
        InstrumentType instrumentType,
        String metricName,
        Map<String, Object> expectedAttributes
    ) {
        final List<Measurement> clusterMeasurements = getClusterMeasurements(instrumentType, metricName);
        assertThat(clusterMeasurements, not(empty()));
        clusterMeasurements.forEach(recordingMetric -> {
            for (Map.Entry<String, Object> entry : expectedAttributes.entrySet()) {
                assertThat(recordingMetric.attributes(), hasEntry(entry.getKey(), entry.getValue()));
            }
        });
    }

    private static List<Measurement> getClusterMeasurements(InstrumentType instrumentType, String metricName) {
        return allTestTelemetryPlugins().flatMap(
            testTelemetryPlugin -> ((RecordingMeterRegistry) testTelemetryPlugin.getTelemetryProvider(Settings.EMPTY).getMeterRegistry())
                .getRecorder()
                .getMeasurements(instrumentType, metricName)
                .stream()
        ).toList();
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
