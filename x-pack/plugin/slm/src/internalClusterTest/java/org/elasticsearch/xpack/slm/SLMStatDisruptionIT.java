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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.LagDetector;
import org.elasticsearch.cluster.coordination.LeaderChecker;
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
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.RegisteredPolicySnapshots;
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
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
            TestDelayedRepoPlugin.class,
            TestRestartBeforeListenersRepoPlugin.class
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

    public static class TestRestartBeforeListenersRepoPlugin extends Plugin implements RepositoryPlugin {

        // Use static vars since instantiated by plugin system
        private static Runnable onResponse;

        static void setOnResponse(Runnable onResponse) {
            TestRestartBeforeListenersRepoPlugin.onResponse = onResponse;
        }

        static void clearOnResponse() {
            onResponse = () -> {};
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
                TestRestartBeforeListenersRepo.TYPE,
                metadata -> new TestRestartBeforeListenersRepo(
                    metadata,
                    env,
                    namedXContentRegistry,
                    clusterService,
                    bigArrays,
                    recoverySettings,
                    // add wrapper lambda so can change underlying runnable with static setOnResponse() method
                    () -> onResponse.run()
                )
            );
        }
    }

    /**
     * Repo which forces a runnable to be run after snapshot finalization, but before callbacks
     * which receive the SnapshotInfo, specifically the SLM callback which will save failure stats.
     */
    static class TestRestartBeforeListenersRepo extends FsRepository {
        private static final String TYPE = "restart_before_listeners";

        private final Runnable beforeResponseRunnable;

        protected TestRestartBeforeListenersRepo(
            RepositoryMetadata metadata,
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            Runnable beforeResponseRunnable
        ) {
            super(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings);
            this.beforeResponseRunnable = beforeResponseRunnable;
        }

        @Override
        public void finalizeSnapshot(FinalizeSnapshotContext fsc) {
            var newFinalizeContext = new FinalizeSnapshotContext(
                fsc.updatedShardGenerations(),
                fsc.repositoryStateId(),
                fsc.clusterMetadata(),
                fsc.snapshotInfo(),
                fsc.repositoryMetaVersion(),
                fsc,
                () -> {
                    // run the passed lambda before calling the usual callback
                    // this is where the cluster can be restarted before SLM is called back with the snapshotInfo
                    beforeResponseRunnable.run();
                    fsc.onDone();
                }
            );
            super.finalizeSnapshot(newFinalizeContext);
        }
    }

    @Before
    public void clearRepoFinalizeRunnable() {
        TestRestartBeforeListenersRepoPlugin.clearOnResponse();
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

        // wait until snapshotA is registered before starting snapshotB
        assertBusy(() -> assertRegistered(policyName, List.of(snapshotA)), 1, TimeUnit.MINUTES);

        // create another snapshot while A is still running
        String snapshotB = executePolicy(masterNode, policyName);
        logger.info("Created snapshot B: " + snapshotB);

        // wait until both snapshots are registered before allowing snapshotA to continue
        assertBusy(() -> assertRegistered(policyName, List.of(snapshotA, snapshotB)), 1, TimeUnit.MINUTES);

        // remove delay from snapshotA allowing it to finish
        TestDelayedRepoPlugin.removeDelay();

        waitForSnapshot(repoName, snapshotA);
        waitForSnapshot(repoName, snapshotB);

        assertBusy(() -> {
            assertSnapshotSuccess(repoName, snapshotA);
            assertSnapshotSuccess(repoName, snapshotB);
            assertMetadata(policyName, 2, 0, 0);
            assertRegistered(policyName, List.of());

        }, 1, TimeUnit.MINUTES);
    }

    /**
     * Test that after successful snapshot registered is empty
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
        createRepository(repoName, TestRestartBeforeListenersRepo.TYPE);
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        ensureGreen();

        String snapshotName = executePolicy(masterNode, policyName);
        logger.info("Created snapshot: " + snapshotName);

        waitForSnapshot(repoName, snapshotName);

        assertBusy(() -> {
            assertSnapshotSuccess(repoName, snapshotName);
            assertMetadata(policyName, 1, 0, 0);
            assertRegistered(policyName, List.of());
        }, 1, TimeUnit.MINUTES);
    }

    /**
     * Test that after a failure which fails stats uploads, then a success,
     * registered snapshot from failure is added to invocationsSinceLastSuccess.
     */
    public void testFailSnapshotFailStatsThenSuccessRecoverStats() throws Exception {
        final String idxName = "test-idx";
        final String repoName = "test-repo";
        final String policyName = "test-policy";

        internalCluster().startMasterOnlyNodes(1);
        final String masterNode = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // Add network disruption so snapshot fails with PARTIAL status
        NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        // wire into repo so code can be run on test thread after snapshot finalize, but before SLM is called back
        var runDuringFinalize = new RunDuringFinalize();
        TestRestartBeforeListenersRepoPlugin.setOnResponse(runDuringFinalize.finalizeThreadRunnable());

        createRandomIndex(idxName, dataNode);
        createRepository(repoName, TestRestartBeforeListenersRepo.TYPE);
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        ensureGreen();

        networkDisruption.startDisrupting();
        String snapshotName = executePolicy(masterNode, policyName);
        logger.info("Created snapshot: " + snapshotName);

        // restart snapshot after snapshot finalize, but before SLM callback called
        runDuringFinalize.awaitAndRun(() -> {
            try {
                internalCluster().restartNode(masterNode);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertBusy(() -> {
            assertSnapshotPartial(repoName, snapshotName);
            assertMetadata(policyName, 0, 0, 0);
            assertRegistered(policyName, List.of(snapshotName));
        }, 1, TimeUnit.MINUTES);

        awaitNoMoreRunningOperations();
        ensureGreen();

        // Now execute again, and succeed. The failure from the previous run will now be recorded.

        TestRestartBeforeListenersRepoPlugin.clearOnResponse();

        final String snapshotName2 = executePolicy(masterNode, policyName);
        assertNotEquals(snapshotName, snapshotName2);
        logger.info("Created snapshot: " + snapshotName2);

        waitForSnapshot(repoName, snapshotName2);

        assertBusy(() -> {
            assertSnapshotSuccess(repoName, snapshotName2);
            // Check stats, this time past failure should be accounted for
            assertMetadata(policyName, 1, 1, 0);
            assertRegistered(policyName, List.of());
        }, 1, TimeUnit.MINUTES);
    }

    /**
     * Test that after a failure then a failure that successfully sets stats
     * registeredRuns from failure is added to invocationsSinceLastSuccess.
     */
    public void testFailSnapshotFailStatsRecoverStats() throws Exception {
        final String idxName = "test-idx";
        final String repoName = "test-repo";
        final String policyName = "test-policy";

        internalCluster().startMasterOnlyNodes(1);
        final String masterNode = internalCluster().getMasterName();
        final String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        // Add network disruption so snapshot fails with PARTIAL status
        NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        // wire into repo so code can be run on test thread after snapshot finalize, but before SLM is called back
        var runDuringFinalize = new RunDuringFinalize();
        TestRestartBeforeListenersRepoPlugin.setOnResponse(runDuringFinalize.finalizeThreadRunnable());

        createRandomIndex(idxName, dataNode);
        createRepository(repoName, TestRestartBeforeListenersRepo.TYPE);
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        awaitNoMoreRunningOperations();
        ensureGreen();

        networkDisruption.startDisrupting();
        String snapshotName = executePolicy(masterNode, policyName);
        logger.info("Created snapshot: " + snapshotName);

        // restart snapshot after snapshot finalize, but before SLM callback called
        runDuringFinalize.awaitAndRun(() -> {
            try {
                internalCluster().restartNode(masterNode);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertBusy(() -> {
            assertSnapshotPartial(repoName, snapshotName);
            assertMetadata(policyName, 0, 0, 0);
            assertRegistered(policyName, List.of(snapshotName));
        }, 1, TimeUnit.MINUTES);

        awaitNoMoreRunningOperations();
        ensureGreen();

        // Now execute again, but don't fail the stat upload. The failure from the previous run will now be recorded.
        var runDuringFinalize2 = new RunDuringFinalize();
        TestRestartBeforeListenersRepoPlugin.setOnResponse(runDuringFinalize2.finalizeThreadRunnable());

        networkDisruption.startDisrupting();
        final String snapshotName2 = executePolicy(masterNode, policyName);
        assertNotEquals(snapshotName, snapshotName2);
        logger.info("Created snapshot: " + snapshotName2);

        runDuringFinalize2.awaitAndRun(networkDisruption::stopDisrupting);

        assertBusy(() -> {
            assertSnapshotPartial(repoName, snapshotName2);
            // Check metadata, this time past failure should be accounted for
            assertMetadata(policyName, 0, 2, 2);
            assertRegistered(policyName, List.of());
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

        // Add network disruption so snapshot fails with PARTIAL status
        NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        // wire into repo so code can be run on test thread after snapshot finalize, but before SLM is called back
        var runDuringFinalize = new RunDuringFinalize();
        TestRestartBeforeListenersRepoPlugin.setOnResponse(runDuringFinalize.finalizeThreadRunnable());

        createRandomIndex(idxName, dataNode);
        createRepository(repoName, TestRestartBeforeListenersRepo.TYPE);
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        ensureGreen();

        networkDisruption.startDisrupting();
        String snapshotName = executePolicy(masterNode, policyName);

        // restart snapshot after snapshot finalize, but before SLM callback called
        runDuringFinalize.awaitAndRun(() -> {
            try {
                internalCluster().restartNode(masterNode);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        assertBusy(() -> {
            assertSnapshotPartial(repoName, snapshotName);
            assertMetadata(policyName, 0, 0, 0);
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

        // Add network disruption so snapshot fails with PARTIAL status
        NetworkDisruption networkDisruption = isolateMasterDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        // wire into repo so code can be run on test thread after snapshot finalize, but before SLM is called back
        var runDuringFinalize = new RunDuringFinalize();
        TestRestartBeforeListenersRepoPlugin.setOnResponse(runDuringFinalize.finalizeThreadRunnable());

        createRandomIndex(idxName, dataNode);
        createRepository(repoName, TestRestartBeforeListenersRepo.TYPE);
        createSnapshotPolicy(policyName, "snap", NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);

        ensureGreen();

        networkDisruption.startDisrupting();
        String snapshotName = executePolicy(masterNode, policyName);

        // wait for snapshot to complete and network disruption to stop
        runDuringFinalize.awaitAndRun(networkDisruption::stopDisrupting);

        assertBusy(() -> {
            assertSnapshotPartial(repoName, snapshotName);
            assertMetadata(policyName, 0, 1, 1);
        }, 1, TimeUnit.MINUTES);
    }

    private void assertMetadata(String policyName, long taken, long failure, long invocationsSinceLastSuccess) {
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
    }

    private SnapshotLifecycleMetadata getSnapshotLifecycleMetadata() {
        final ClusterStateResponse clusterStateResponse = client().admin()
            .cluster()
            .state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT))
            .actionGet();
        ClusterState state = clusterStateResponse.getState();
        return state.metadata().getProject().custom(SnapshotLifecycleMetadata.TYPE);
    }

    private RegisteredPolicySnapshots getRegisteredSnapshots() {
        final ClusterStateResponse clusterStateResponse = client().admin()
            .cluster()
            .state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT))
            .actionGet();
        ClusterState state = clusterStateResponse.getState();
        return state.metadata().getProject().custom(RegisteredPolicySnapshots.TYPE, RegisteredPolicySnapshots.EMPTY);
    }

    private SnapshotInfo getSnapshotInfo(String repository, String snapshot) {
        GetSnapshotsResponse snapshotsStatusResponse = clusterAdmin().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repository)
            .setSnapshots(snapshot)
            .get();
        return snapshotsStatusResponse.getSnapshots().get(0);
    }

    private SnapshotsStatusResponse getSnapshotStatus(String repo, String snapshotName) {
        return clusterAdmin().prepareSnapshotStatus(TEST_REQUEST_TIMEOUT, repo).setSnapshots(snapshotName).get();
    }

    private void assertSnapshotSuccess(String repository, String snapshot) {
        try {
            SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshot);
            assertEquals(SnapshotState.SUCCESS, snapshotInfo.state());
            assertEquals(1, snapshotInfo.successfulShards());
            assertEquals(0, snapshotInfo.failedShards());
            logger.info("Checked snapshot exists and is state SUCCESS");
        } catch (SnapshotMissingException e) {
            fail("expected a snapshot with name " + snapshot + " but it does yet not exist");
        }
    }

    private void assertSnapshotPartial(String repository, String snapshot) {
        try {
            SnapshotInfo snapshotInfo = getSnapshotInfo(repository, snapshot);
            assertEquals(SnapshotState.PARTIAL, snapshotInfo.state());
            assertEquals(0, snapshotInfo.successfulShards());
            assertEquals(1, snapshotInfo.failedShards());
            logger.info("Checked snapshot exists and is state PARTIAL");
        } catch (SnapshotMissingException e) {
            fail("expected a snapshot with name " + snapshot + " but it does yet not exist");
        }
    }

    private void assertStats(SnapshotLifecycleMetadata snapshotLifecycleMetadata, String policyName, long taken, long failed) {
        var stats = snapshotLifecycleMetadata.getStats().getMetrics().get(policyName);
        logger.info("stats: " + stats);
        if (taken == 0 && failed == 0) {
            assertTrue(stats == null || (stats.getSnapshotTakenCount() == 0 && stats.getSnapshotFailedCount() == 0));
        } else {
            assertNotNull(stats);
            assertEquals(taken, stats.getSnapshotTakenCount());
            assertEquals(failed, stats.getSnapshotFailedCount());
        }
    }

    private void assertRegistered(String policyName, List<String> expected) {
        var registered = getRegisteredSnapshots();
        var policySnaps = registered.getSnapshotsByPolicy(policyName).stream().map(SnapshotId::getName).toList();
        assertEquals(expected, policySnaps);
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

    /**
     * The purpose of this class is to allow a cluster restart to occur after a snapshot has been finalized, but before the SLM callback
     * which processes snapshotInfo has been called. TestRestartBeforeListenersRepo allows a runnable to be called at this time. But,
     * internalCluster().restartNode() cannot be called on the thread running finalize. It is likely missing necessary context, or simply
     * cannot be called by a thread on the master node. This helper class uses latches to run a callback on the test thread while the
     * master node is waiting between finalizing the snapshot and before calling the SLM callback.
     */
    static class RunDuringFinalize {
        private final CountDownLatch latch1 = new CountDownLatch(1);
        private final CountDownLatch latch2 = new CountDownLatch(1);

        Runnable finalizeThreadRunnable() {
            return () -> {
                latch1.countDown();
                try {
                    // While this is waiting the runnable called in awaitAndRan will be called by another thread.
                    // The runnable is usually a cluster restart. During a restart, 10s is waited before this waiting thread is preempted.
                    // To keep the tests fast, this wait is kept to 1s. This is actually a race condition which could cause a test to fail,
                    // as the SLM callback could be called. But, after 1s, the restart should have started so this is unlikely.
                    latch2.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
        }

        void awaitAndRun(Runnable runnable) throws InterruptedException {
            assertTrue(latch1.await(1, TimeUnit.MINUTES));
            // this is where the cluster restart occurs
            runnable.run();
            latch2.countDown();
        }
    }
}
