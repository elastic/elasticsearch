/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.LagDetector;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test that SLM stats can be lost due to master shutdown,
 * and then recovered by registering them before snapshotting.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SLMStatDisruptionIT extends AbstractSnapshotIntegTestCase {

    private static final String NEVER_EXECUTE_CRON_SCHEDULE = "* * * 31 FEB ? *";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            MockRepository.Plugin.class,
            MockTransportService.TestPlugin.class,
            LocalStateCompositeXPackPlugin.class,
            IndexLifecycle.class,
            SnapshotLifecycle.class,
            DataStreamsPlugin.class,
            TestDelayedRepoPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED, false)
            .put(DEFAULT_SETTINGS)
            .build();
    }

    // copied from AbstractDisruptionTestCase.DEFAULT_SETTINGS
    public static final Settings DEFAULT_SETTINGS = Settings.builder()
        .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "5s") // for hitting simulated network failures quickly
        .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1) // for hitting simulated network failures quickly
        .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "5s") // for hitting simulated network failures quickly
        .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1) // for hitting simulated network failures quickly
        .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "5s") // <-- for hitting simulated network failures quickly
        .put(LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.getKey(), "5s") // remove lagging nodes quickly so they can rejoin
        .put(TransportSettings.CONNECT_TIMEOUT.getKey(), "10s") // Network delay disruption waits for the min between this
        // value and the time of disruption and does not recover immediately
        // when disruption is stop. We should make sure we recover faster
        // then the default of 30s, causing ensureGreen and friends to time out
        .build();

    public static class TestDelayedRepoPlugin extends Plugin implements RepositoryPlugin {

        // Use static vars since instantiated by plugin system
        private static final AtomicBoolean doDelay = new AtomicBoolean(true);
        private static final CountDownLatch delayedRepoLatch = new CountDownLatch(1);

        static void removeDelay() {
            delayedRepoLatch.countDown();
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics
        ) {
            return Map.of(
                TestDelayedRepo.TYPE,
                metadata -> new TestDelayedRepo(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings, () -> {
                    // Only delay the first request
                    if (doDelay.getAndSet(false)) {
                        try {
                            assertTrue(delayedRepoLatch.await(1, TimeUnit.MINUTES));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                })
            );
        }
    }

    static class TestDelayedRepo extends FsRepository {
        private static final String TYPE = "delayed";
        private final Runnable delayFn;

        protected TestDelayedRepo(
            RepositoryMetadata metadata,
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            Runnable delayFn
        ) {
            super(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
            this.delayFn = delayFn;
        }

        @Override
        protected void snapshotFile(SnapshotShardContext context, BlobStoreIndexShardSnapshot.FileInfo fileInfo) throws IOException {
            delayFn.run();
            super.snapshotFile(context, fileInfo);
        }
    }

    /**
     * Test that if there is a currently running snapshot it is not inferred to be a failure
     */
    public void testCurrentlyRunningSnapshotNotRecordedAsFailure() throws Exception {
        final String idxName = "test-idx";
        final String repoName = "test-repo";
        final String policyName = "test-policy";

        internalCluster().startMasterOnlyNodes(1);
        final String masterNode = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        createRandomIndex(idxName, dataNode);
        createRepository(repoName, TestDelayedRepo.TYPE);
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        ensureGreen();

        String snapshotA = executePolicy(masterNode, policyName);
        logger.info("Created snapshot A: " + snapshotA);

        // wait until snapshotA is preregistered and in progress before starting snapshotB
        assertBusy(() -> assertSnapshotRunning(policyName, snapshotA));
        assertBusy(() -> assertPreRegistered(List.of(snapshotA), policyName), 1, TimeUnit.MINUTES);

        String snapshotB = executePolicy(masterNode, policyName);
        logger.info("Created snapshot B: " + snapshotB);

        // wait until both snapshots are preregistered before allowing snapshotA to continue
        assertBusy(() -> assertPreRegistered(List.of(snapshotA, snapshotB), policyName), 1, TimeUnit.MINUTES);

        // remove delay from snapshotA
        TestDelayedRepoPlugin.removeDelay();

        waitForSnapshot(repoName, snapshotA);
        waitForSnapshot(repoName, snapshotB);

        assertBusy(() -> {
            assertSnapshotSuccess(repoName, snapshotA);
            assertSnapshotSuccess(repoName, snapshotB);
            assertMetadata(policyName, 2, 0, 0, List.of());

        }, 1, TimeUnit.MINUTES);
    }

    /**
     * Test that after successful snapshot preRegisteredRuns status is 0.
     */
    public void testSuccessSnapshot() throws Exception {
        final String idxName = "test-idx";
        final String repoName = "test-repo";
        final String policyName = "test-policy";

        internalCluster().startMasterOnlyNodes(1);
        final String masterNode = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        createRandomIndex(idxName, dataNode);
        createRepository(repoName, "mock");
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        ensureGreen();

        String snapshotName = executePolicy(masterNode, policyName);
        logger.info("Created snapshot: " + snapshotName);

        waitForSnapshot(repoName, snapshotName);

        assertBusy(() -> {
            assertSnapshotSuccess(repoName, snapshotName);
            assertMetadata(policyName, 1, 0, 0, List.of());
        }, 1, TimeUnit.MINUTES);
    }

    /**
     * Test that after a failure then a success, preRegisteredRuns from failure is added to invocationsSinceLastSuccess.
     */
    public void testFailSnapshotFailStatsThenSuccessRecoverStats() throws Exception {
        final String idxName = "test-idx";
        final String repoName = "test-repo";
        final String policyName = "test-policy";

        internalCluster().startMasterOnlyNodes(1);
        final String masterNode = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        // Listener that stops disrupting network only after snapshot completion
        CountDownLatch latch = new CountDownLatch(1);
        internalCluster().clusterService(masterNode).addListener(new WaitForSnapshotListener(repoName, networkDisruption, latch));

        createRandomIndex(idxName, dataNode);
        createRepository(repoName, "mock");
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        ensureGreen();

        networkDisruption.startDisrupting();
        String snapshotName = executePolicy(masterNode, policyName);
        logger.info("Created snapshot: " + snapshotName);

        // wait for snapshot to complete and network disruption to stop
        assertTrue(latch.await(1, TimeUnit.MINUTES));

        // Restart master so failure stat is lost. this relies on a race condition as node shutdown must happen before stats are stored in
        // cluster state, but this is not guaranteed. If the following assert fails consider this race condition as a possible culprit.
        internalCluster().restartNode(masterNode);

        assertBusy(() -> {
            assertSnapshotPartial(repoName, snapshotName);
            assertMetadata(policyName, 0, 0, 0, List.of(snapshotName));
        }, 1, TimeUnit.MINUTES);

        awaitNoMoreRunningOperations();
        ensureGreen();

        // Now execute again, and succeed. The failure from the previous run will now be recorded.
        final String snapshotName2 = executePolicy(masterNode, policyName);
        assertNotEquals(snapshotName, snapshotName2);
        logger.info("Created snapshot: " + snapshotName2);

        waitForSnapshot(repoName, snapshotName2);

        assertBusy(() -> {
            assertSnapshotSuccess(repoName, snapshotName2);
            // Check stats, this time past failure should be accounted for
            assertMetadata(policyName, 1, 1, 0, List.of());
        }, 1, TimeUnit.MINUTES);
    }

    /**
     * Test that after a failure then a failure that successfully sets stats
     * preRegisteredRuns from failure is added to invocationsSinceLastSuccess.
     */
    public void testFailSnapshotFailStatsRecoverStats() throws Exception {
        final String idxName = "test-idx";
        final String repoName = "test-repo";
        final String policyName = "test-policy";

        internalCluster().startMasterOnlyNodes(1);
        final String masterNode = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        // Listener that stops disrupting network only after snapshot completion
        CountDownLatch latch = new CountDownLatch(1);
        internalCluster().clusterService(masterNode).addListener(new WaitForSnapshotListener(repoName, networkDisruption, latch));

        createRandomIndex(idxName, dataNode);
        createRepository(repoName, "mock");
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        awaitNoMoreRunningOperations();
        ensureGreen();

        networkDisruption.startDisrupting();
        String snapshotName = executePolicy(masterNode, policyName);
        logger.info("Created snapshot: " + snapshotName);

        // wait for snapshot to complete and network disruption to stop
        assertTrue(latch.await(1, TimeUnit.MINUTES));

        // Restart master so failure stat is lost. this relies on a race condition as node shutdown must happen before stats are stored in
        // cluster state, but this is not guaranteed. If the following assert fails consider this race condition as a possible culprit.
        internalCluster().restartNode(masterNode);

        assertBusy(() -> {
            assertSnapshotPartial(repoName, snapshotName);
            assertMetadata(policyName, 0, 0, 0, List.of(snapshotName));
        }, 1, TimeUnit.MINUTES);

        awaitNoMoreRunningOperations();
        ensureGreen();

        // Now execute again, but don't fail the stat upload. The failure from the previous run will now be recorded.
        CountDownLatch latch2 = new CountDownLatch(1);
        internalCluster().clusterService(masterNode).addListener(new WaitForSnapshotListener(repoName, networkDisruption, latch2));

        networkDisruption.startDisrupting();
        final String snapshotName2 = executePolicy(masterNode, policyName);
        assertNotEquals(snapshotName, snapshotName2);
        logger.info("Created snapshot: " + snapshotName2);

        // wait for snapshot to complete and network disruption to stop
        assertTrue(latch2.await(1, TimeUnit.MINUTES));

        assertBusy(() -> {
            assertSnapshotPartial(repoName, snapshotName2);
            // Check metadata, this time past failure should be accounted for
            assertMetadata(policyName, 0, 2, 2, List.of());
        }, 1, TimeUnit.MINUTES);
    }

    /**
     * Test that after a failed snapshot with a master restart during stat upload, update of invocationsSinceLastSuccess is lost.
     */
    public void testFailedSnapshotFailStats() throws Exception {
        final String idxName = "test-idx";
        final String repoName = "test-repo";
        final String policyName = "test-policy";

        internalCluster().startMasterOnlyNodes(1);
        final String masterNode = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        CountDownLatch latch = new CountDownLatch(1);
        internalCluster().clusterService(masterNode).addListener(new WaitForSnapshotListener(repoName, networkDisruption, latch));

        createRandomIndex(idxName, dataNode);
        createRepository(repoName, "mock");
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        ensureGreen();

        networkDisruption.startDisrupting();
        String snapshotName = executePolicy(masterNode, policyName);

        // wait for snapshot to complete and network disruption to stop
        assertTrue(latch.await(1, TimeUnit.MINUTES));

        // restart master so failure stat is lost
        internalCluster().restartNode(masterNode);

        assertBusy(() -> {
            assertSnapshotPartial(repoName, snapshotName);
            assertMetadata(policyName, 0, 0, 0, null);
        }, 1, TimeUnit.MINUTES);
    }

    /**
     * Confirm normal behavior during failure that successfully sets stats in cluster state.
     */
    public void testFailedSnapshotSubmitStats() throws Exception {
        final String idxName = "test-idx";
        final String repoName = "test-repo";
        final String policyName = "test-policy";

        internalCluster().startMasterOnlyNodes(1);
        final String masterNode = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        CountDownLatch latch = new CountDownLatch(1);
        internalCluster().clusterService(masterNode).addListener(new WaitForSnapshotListener(repoName, networkDisruption, latch));

        createRandomIndex(idxName, dataNode);
        createRepository(repoName, "mock");
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        ensureGreen();

        networkDisruption.startDisrupting();
        String snapshotName = executePolicy(masterNode, policyName);

        // wait for snapshot to complete and network disruption to stop
        assertTrue(latch.await(1, TimeUnit.MINUTES));

        assertBusy(() -> {
            assertSnapshotPartial(repoName, snapshotName);
            logger.info("--> Verified that snapshot was not successful");
        }, 1, TimeUnit.MINUTES);

        assertBusy(() -> assertMetadata(policyName, 0, 1, 1, List.of()), 1, TimeUnit.MINUTES);
    }

    private void assertMetadata(String policyName, long taken, long failure, long invocationsSinceLastSuccess, List<String> preRegistered) {
        var snapshotLifecycleMetadata = getSnapshotLifecycleMetadata();
        var snapshotLifecyclePolicyMetadata = snapshotLifecycleMetadata.getSnapshotConfigurations().get(policyName);
        assertStats(snapshotLifecycleMetadata, policyName, taken, failure);
        if (taken > 0) {
            assertNotNull(snapshotLifecyclePolicyMetadata.getLastSuccess());
        } else {
            assertNull(snapshotLifecyclePolicyMetadata.getLastSuccess());
        }
        if (failure > 0) {
            assertNotNull(snapshotLifecyclePolicyMetadata.getLastFailure());
        } else {
            assertNull(snapshotLifecyclePolicyMetadata.getLastFailure());
        }
        assertEquals(invocationsSinceLastSuccess, snapshotLifecyclePolicyMetadata.getInvocationsSinceLastSuccess());

        if (preRegistered != null) {
            assertPreRegistered(preRegistered, policyName);
        }
    }

    private SnapshotLifecycleMetadata getSnapshotLifecycleMetadata() {
        final ClusterStateResponse clusterStateResponse = client().admin().cluster().state(new ClusterStateRequest()).actionGet();
        ClusterState state = clusterStateResponse.getState();
        return state.metadata().custom(SnapshotLifecycleMetadata.TYPE);
    }

    private SnapshotInfo getSnapshotInfo(String repository, String snapshot) {
        GetSnapshotsResponse snapshotsStatusResponse = client(internalCluster().getMasterName()).admin()
            .cluster()
            .prepareGetSnapshots(repository)
            .setSnapshots(snapshot)
            .get();
        return snapshotsStatusResponse.getSnapshots().get(0);
    }

    private void assertSnapshotSuccess(String repository, String snapshot) {
        SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshot);
        assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
        assertEquals(1, snapshotInfo.successfulShards());
        assertEquals(0, snapshotInfo.failedShards());
        logger.info("Checked snapshot exists and is state SUCCESS");
    }

    private void assertSnapshotPartial(String repository, String snapshot) {
        SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshot);
        assertEquals(SnapshotState.PARTIAL, snapshotInfo.state());
        assertEquals(0, snapshotInfo.successfulShards());
        assertEquals(1, snapshotInfo.failedShards());
        logger.info("Checked snapshot exists and is state PARTIAL");
    }

    private void assertStats(SnapshotLifecycleMetadata snapshotLifecycleMetadata, String policyName, long taken, long failed) {
        var stats = snapshotLifecycleMetadata.getStats().getMetrics().get(policyName);
        if (taken == 0 && failed == 0) {
            assertTrue(stats == null || (stats.getSnapshotTakenCount() == 0 && stats.getSnapshotFailedCount() == 0));
        } else {
            assertNotNull(stats);
            assertEquals(taken, stats.getSnapshotTakenCount());
            assertEquals(failed, stats.getSnapshotFailedCount());
        }
    }

    private void assertSnapshotRunning(String policyId, String snapshot) {
        final ClusterStateResponse clusterStateResponse = client().admin().cluster().state(new ClusterStateRequest()).actionGet();
        ClusterState state = clusterStateResponse.getState();
        SnapshotsInProgress snapshots = state.custom(SnapshotsInProgress.TYPE);
        Set<SnapshotId> snapshotIds = SnapshotLifecycleTask.currentlyRunningSnapshots(policyId, state);
        Set<String> snapshotNames = snapshotIds.stream().map(SnapshotId::getName).collect(Collectors.toSet());
        assertTrue(snapshotNames.contains(snapshot));
    }

    private void assertPreRegistered(List<String> expected, String policyName) {
        var snapshotLifecycleMetadata = getSnapshotLifecycleMetadata();
        var snapshotLifecyclePolicyMetadata = snapshotLifecycleMetadata.getSnapshotConfigurations().get(policyName);
        List<String> preRegisteredNames = snapshotLifecyclePolicyMetadata.getPreRegisteredSnapshots()
            .stream()
            .map(SnapshotId::getName)
            .collect(Collectors.toList());
        assertEquals(expected, preRegisteredNames);
    }

    private void createRandomIndex(String idxName, String dataNodeName) throws InterruptedException {
        Settings settings = indexSettings(1, 0).put("index.routing.allocation.require._name", dataNodeName).build();
        createIndex(idxName, settings);

        logger.info("--> indexing some data");
        final int numdocs = randomIntBetween(10, 100);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numdocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = prepareIndex(idxName).setId(Integer.toString(i)).setSource("field1", "bar " + i);
        }
        indexRandom(true, builders);
        indicesAdmin().refresh(new RefreshRequest(idxName)).actionGet();
    }

    private void createSnapshotPolicy(String policyName, String snapshotNamePattern, String schedule, String repoId, String indexPattern) {
        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(indexPattern));
        snapConfig.put("ignore_unavailable", false);
        snapConfig.put("partial", true);

        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
            policyName,
            snapshotNamePattern,
            schedule,
            repoId,
            snapConfig,
            SnapshotRetentionConfiguration.EMPTY
        );

        PutSnapshotLifecycleAction.Request putLifecycle = new PutSnapshotLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            policyName,
            policy
        );
        try {
            client().execute(PutSnapshotLifecycleAction.INSTANCE, putLifecycle).get();
        } catch (Exception e) {
            logger.error("failed to create slm policy", e);
            fail("failed to create policy " + policy + " got: " + e);
        }
    }

    /**
     * Execute the given policy and return the generated snapshot name
     */
    private String executePolicy(String node, String policyId) throws ExecutionException, InterruptedException {
        ExecuteSnapshotLifecycleAction.Request executeReq = new ExecuteSnapshotLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            policyId
        );
        ExecuteSnapshotLifecycleAction.Response resp = client(node).execute(ExecuteSnapshotLifecycleAction.INSTANCE, executeReq).get();
        return resp.getSnapshotName();
    }

    private void waitForSnapshot(String repo, String snapshotName) throws Exception {
        assertBusy(() -> {
            try {
                SnapshotsStatusResponse s = getSnapshotStatus(repo, snapshotName);
                assertThat("expected a snapshot but none were returned", s.getSnapshots().size(), equalTo(1));
                SnapshotStatus status = s.getSnapshots().get(0);
                logger.info("--> waiting for snapshot {} to be completed, got: {}", snapshotName, status.getState());
                assertThat(status.getState(), equalTo(SnapshotsInProgress.State.SUCCESS));
            } catch (SnapshotMissingException e) {
                logger.error("expected a snapshot but it was missing", e);
                fail("expected a snapshot with name " + snapshotName + " but it does not exist");
            }
        });
    }

    // ClusterChangeListener that wait for snapshot to complete then stops network disruption
    private SnapshotsStatusResponse getSnapshotStatus(String repo, String snapshotName) {
        return clusterAdmin().prepareSnapshotStatus(TEST_REQUEST_TIMEOUT, repo).setSnapshots(snapshotName).get();
    }

    static class WaitForSnapshotListener implements ClusterStateListener {
        private final String repoName;
        private final NetworkDisruption networkDisruption;
        private final CountDownLatch latch;

        WaitForSnapshotListener(String repoName, NetworkDisruption networkDisruption, CountDownLatch latch) {
            this.repoName = repoName;
            this.networkDisruption = networkDisruption;
            this.latch = latch;
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            SnapshotsInProgress snapshots = event.state().custom(SnapshotsInProgress.TYPE);
            if (snapshots != null && snapshots.isEmpty() == false) {
                final SnapshotsInProgress.Entry snapshotEntry = snapshots.forRepo(repoName).get(0);
                if (snapshotEntry.state() == SnapshotsInProgress.State.SUCCESS) {
                    final RepositoryMetadata metadata = RepositoriesMetadata.get(event.state()).repository(repoName);
                    if (metadata.pendingGeneration() > snapshotEntry.repositoryStateId()) {
                        networkDisruption.stopDisrupting();
                        latch.countDown();
                    }
                }
            }
        }
    }
}
