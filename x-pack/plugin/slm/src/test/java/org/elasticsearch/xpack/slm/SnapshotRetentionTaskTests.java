/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.slm.history.SnapshotHistoryStore;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.elasticsearch.xpack.slm.history.SnapshotHistoryItem.DELETE_OPERATION;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class SnapshotRetentionTaskTests extends ESTestCase {

    public void testGetAllPoliciesWithRetentionEnabled() {
        SnapshotLifecyclePolicy policyWithout = new SnapshotLifecyclePolicy(
            "policyWithout",
            "snap",
            "1 * * * * ?",
            "repo",
            null,
            SnapshotRetentionConfiguration.EMPTY
        );
        SnapshotLifecyclePolicy policyWithout2 = new SnapshotLifecyclePolicy(
            "policyWithout2",
            "snap",
            "1 * * * * ?",
            "repo",
            null,
            new SnapshotRetentionConfiguration(null, null, null)
        );
        SnapshotLifecyclePolicy policyWith = new SnapshotLifecyclePolicy(
            "policyWith",
            "snap",
            "1 * * * * ?",
            "repo",
            null,
            new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null)
        );

        // Test with no SLM metadata
        ClusterState state = ClusterState.builder(new ClusterName("cluster")).build();
        assertThat(SnapshotRetentionTask.getAllPoliciesWithRetentionEnabled(state), equalTo(Collections.emptyMap()));

        // Test with empty SLM metadata
        Metadata metadata = Metadata.builder()
            .putCustom(
                SnapshotLifecycleMetadata.TYPE,
                new SnapshotLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING, new SnapshotLifecycleStats())
            )
            .build();
        state = ClusterState.builder(new ClusterName("cluster")).metadata(metadata).build();
        assertThat(SnapshotRetentionTask.getAllPoliciesWithRetentionEnabled(state), equalTo(Collections.emptyMap()));

        // Test with metadata containing only a policy without retention
        state = createState(policyWithout);
        assertThat(SnapshotRetentionTask.getAllPoliciesWithRetentionEnabled(state), equalTo(Collections.emptyMap()));

        // Test with metadata containing a couple of policies
        state = createState(policyWithout, policyWithout2, policyWith);
        Map<String, SnapshotLifecyclePolicy> policyMap = SnapshotRetentionTask.getAllPoliciesWithRetentionEnabled(state);
        assertThat(policyMap.size(), equalTo(1));
        assertThat(policyMap.get("policyWith"), equalTo(policyWith));
    }

    public void testRetentionTaskSuccess() throws Exception {
        retentionTaskTest(true);
    }

    public void testRetentionTaskFailure() throws Exception {
        retentionTaskTest(false);
    }

    private void retentionTaskTest(final boolean deletionSuccess) throws Exception {
        ThreadPool threadPool = new TestThreadPool("slm-test");
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(SLM_HISTORY_INDEX_ENABLED_SETTING))
        );
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);
            var clientThreadPool = createThreadPool()
        ) {
            final var noOpClient = new NoOpClient(clientThreadPool);
            final String policyId = "policy";
            final String repoId = "repo";
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                policyId,
                "snap",
                "1 * * * * ?",
                repoId,
                null,
                new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null)
            );

            ClusterState state = createState(policy);
            ClusterServiceUtils.setState(clusterService, state);

            final SnapshotInfo eligibleSnapshot = new SnapshotInfo(
                new Snapshot(repoId, new SnapshotId("name", "uuid")),
                Collections.singletonList("index"),
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                1L,
                1,
                Collections.emptyList(),
                true,
                Collections.singletonMap("policy", policyId),
                0L,
                Collections.emptyMap()
            );

            Set<SnapshotId> deleted = ConcurrentHashMap.newKeySet();
            Set<String> deletedSnapshotsInHistory = ConcurrentHashMap.newKeySet();
            CountDownLatch deletionLatch = new CountDownLatch(1);
            CountDownLatch historyLatch = new CountDownLatch(1);

            MockSnapshotRetentionTask retentionTask = new MockSnapshotRetentionTask(
                noOpClient,
                clusterService,
                new SnapshotLifecycleTaskTests.VerifyingHistoryStore(noOpClient, clusterService, (historyItem) -> {
                    assertEquals(deletionSuccess, historyItem.isSuccess());
                    if (historyItem.isSuccess() == false) {
                        assertThat(historyItem.getErrorDetails(), containsString("deletion_failed"));
                    }
                    assertEquals(policyId, historyItem.getPolicyId());
                    assertEquals(repoId, historyItem.getRepository());
                    assertEquals(DELETE_OPERATION, historyItem.getOperation());
                    deletedSnapshotsInHistory.add(historyItem.getSnapshotName());
                    historyLatch.countDown();
                }),
                () -> {
                    final var result = Collections.singletonMap(repoId, List.of(Tuple.tuple(eligibleSnapshot.snapshotId(), policyId)));
                    logger.info("--> retrieving snapshots [{}]", result);
                    return result;
                },
                (deletionPolicyId, repo, snapId, slmStats, listener) -> {
                    logger.info("--> deleting {} from repo {}", snapId, repo);
                    deleted.add(snapId);
                    if (deletionSuccess) {
                        listener.onResponse(AcknowledgedResponse.TRUE);
                    } else {
                        listener.onFailure(new RuntimeException("deletion_failed"));
                    }
                    deletionLatch.countDown();
                },
                System::nanoTime
            );

            long time = System.currentTimeMillis();
            retentionTask.triggered(new SchedulerEngine.Event(SnapshotRetentionService.SLM_RETENTION_JOB_ID, time, time));

            safeAwait(deletionLatch);

            assertThat("something should have been deleted", deleted, not(empty()));
            assertThat("one snapshot should have been deleted", deleted, hasSize(1));
            assertThat(deleted, contains(eligibleSnapshot.snapshotId()));

            boolean historySuccess = historyLatch.await(10, TimeUnit.SECONDS);
            assertThat("expected history entries for 1 snapshot deletions", historySuccess, equalTo(true));
            assertThat(deletedSnapshotsInHistory, contains(eligibleSnapshot.snapshotId().getName()));
        } finally {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testErrStillRunsFailureHandlerWhenRetrieving() throws Exception {
        ThreadPool threadPool = new TestThreadPool("slm-test");
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(SLM_HISTORY_INDEX_ENABLED_SETTING))
        );
        final String policyId = "policy";
        final String repoId = "repo";
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);
            var clientThreadPool = createThreadPool()
        ) {
            final var noOpClient = new NoOpClient(clientThreadPool) {

                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    if (action == TransportSLMGetExpiredSnapshotsAction.INSTANCE) {
                        logger.info("--> called");
                        listener.onResponse((Response) new TransportSLMGetExpiredSnapshotsAction.Response(Map.of()));
                    } else {
                        super.doExecute(action, request, listener);
                    }
                }
            };
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                policyId,
                "snap",
                "1 * * * * ?",
                repoId,
                null,
                new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null)
            );

            ClusterState state = createState(policy);
            ClusterServiceUtils.setState(clusterService, state);

            SnapshotRetentionTask task = new SnapshotRetentionTask(
                noOpClient,
                clusterService,
                System::nanoTime,
                new SnapshotLifecycleTaskTests.VerifyingHistoryStore(
                    noOpClient,
                    clusterService,
                    (historyItem) -> fail("should never write history")
                )
            );

            AtomicReference<Exception> errHandlerCalled = new AtomicReference<>(null);
            task.getSnapshotsEligibleForDeletion(
                Collections.singleton(repoId),
                Map.of(policyId, new SnapshotLifecyclePolicy(policyId, "test", "* * * * *", repoId, null, null)),
                new ActionListener<>() {
                    @Override
                    public void onResponse(Map<String, List<Tuple<SnapshotId, String>>> snapshotsToBeDeleted) {
                        logger.info("--> forcing failure");
                        throw new ElasticsearchException("forced failure");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        errHandlerCalled.set(e);
                    }
                }
            );

            assertNotNull(errHandlerCalled.get());
            assertThat(errHandlerCalled.get().getMessage(), equalTo("forced failure"));
        } finally {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testErrStillRunsFailureHandlerWhenDeleting() throws Exception {
        ThreadPool threadPool = new TestThreadPool("slm-test");
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(SLM_HISTORY_INDEX_ENABLED_SETTING))
        );
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);
            var clientThreadPool = createThreadPool()
        ) {
            final var noOpClient = new NoOpClient(clientThreadPool) {
                @Override
                @SuppressWarnings("unchecked")
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    if (request instanceof DeleteSnapshotRequest) {
                        logger.info("--> called");
                        listener.onResponse((Response) AcknowledgedResponse.TRUE);
                    } else {
                        super.doExecute(action, request, listener);
                    }
                }
            };
            final String policyId = "policy";
            final String repoId = "repo";
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                policyId,
                "snap",
                "1 * * * * ?",
                repoId,
                null,
                new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null)
            );

            ClusterState state = createState(policy);
            ClusterServiceUtils.setState(clusterService, state);

            SnapshotRetentionTask task = new SnapshotRetentionTask(
                noOpClient,
                clusterService,
                System::nanoTime,
                new SnapshotLifecycleTaskTests.VerifyingHistoryStore(
                    noOpClient,
                    clusterService,
                    (historyItem) -> fail("should never write history")
                )
            );

            AtomicReference<SnapshotLifecycleStats> slmStats = new AtomicReference<>(new SnapshotLifecycleStats());
            AtomicBoolean onFailureCalled = new AtomicBoolean(false);
            task.deleteSnapshot("policy", "foo", new SnapshotId("name", "uuid"), slmStats, new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    logger.info("--> forcing failure");
                    throw new ElasticsearchException("forced failure");
                }

                @Override
                public void onFailure(Exception e) {
                    onFailureCalled.set(true);
                }
            });

            assertThat(onFailureCalled.get(), equalTo(true));

            var expectedPolicyStats = Map.of(policyId, new SnapshotLifecycleStats.SnapshotPolicyStats(policyId, 0, 0, 1, 1));
            assertThat(slmStats.get(), equalTo(new SnapshotLifecycleStats(0, 0, 0, 0, expectedPolicyStats)));
        } finally {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testSkipWhileStopping() throws Exception {
        doTestSkipDuringMode(OperationMode.STOPPING);
    }

    public void testSkipWhileStopped() throws Exception {
        doTestSkipDuringMode(OperationMode.STOPPED);
    }

    private void doTestSkipDuringMode(OperationMode mode) throws Exception {
        ThreadPool threadPool = new TestThreadPool("slm-test");
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(SLM_HISTORY_INDEX_ENABLED_SETTING))
        );
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);
            var clientThreadPool = createThreadPool()
        ) {
            final var noOpClient = new NoOpClient(clientThreadPool);
            final String policyId = "policy";
            final String repoId = "repo";
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                policyId,
                "snap",
                "1 * * * * ?",
                repoId,
                null,
                new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null)
            );

            ClusterState state = createState(mode, policy);
            ClusterServiceUtils.setState(clusterService, state);

            SnapshotRetentionTask task = new MockSnapshotRetentionTask(
                noOpClient,
                clusterService,
                new SnapshotLifecycleTaskTests.VerifyingHistoryStore(
                    noOpClient,
                    clusterService,
                    (historyItem) -> fail("should never write history")
                ),
                () -> {
                    fail("should not retrieve snapshots");
                    return null;
                },
                (a, b, c, d, e) -> fail("should not delete snapshots"),
                System::nanoTime
            );

            long time = System.currentTimeMillis();
            task.triggered(new SchedulerEngine.Event(SnapshotRetentionService.SLM_RETENTION_JOB_ID, time, time));
        } finally {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testRunManuallyWhileStopping() throws Exception {
        doTestRunManuallyDuringMode(OperationMode.STOPPING);
    }

    public void testRunManuallyWhileStopped() throws Exception {
        doTestRunManuallyDuringMode(OperationMode.STOPPED);
    }

    private void doTestRunManuallyDuringMode(OperationMode mode) throws Exception {
        ThreadPool threadPool = new TestThreadPool("slm-test");
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(SLM_HISTORY_INDEX_ENABLED_SETTING))
        );
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, settings);
            var clientThreadPool = createThreadPool()
        ) {
            final var noOpClient = new NoOpClient(clientThreadPool);
            final String policyId = "policy";
            final String repoId = "repo";
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
                policyId,
                "snap",
                "1 * * * * ?",
                repoId,
                null,
                new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null)
            );

            ClusterState state = createState(mode, policy);
            ClusterServiceUtils.setState(clusterService, state);

            AtomicBoolean retentionWasRun = new AtomicBoolean(false);
            MockSnapshotRetentionTask task = new MockSnapshotRetentionTask(
                noOpClient,
                clusterService,
                new SnapshotLifecycleTaskTests.VerifyingHistoryStore(noOpClient, clusterService, (historyItem) -> {}),
                () -> {
                    retentionWasRun.set(true);
                    return Collections.emptyMap();
                },
                (deletionPolicyId, repo, snapId, slmStats, listener) -> {},
                System::nanoTime
            );

            long time = System.currentTimeMillis();
            task.triggered(new SchedulerEngine.Event(SnapshotRetentionService.SLM_RETENTION_MANUAL_JOB_ID, time, time));

            assertTrue("retention should be run manually even if SLM is disabled", retentionWasRun.get());
        } finally {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public ClusterState createState(SnapshotLifecyclePolicy... policies) {
        return createState(OperationMode.RUNNING, policies);
    }

    public ClusterState createState(OperationMode mode, SnapshotLifecyclePolicy... policies) {
        Map<String, SnapshotLifecyclePolicyMetadata> policyMetadataMap = Arrays.stream(policies)
            .map(
                policy -> SnapshotLifecyclePolicyMetadata.builder()
                    .setPolicy(policy)
                    .setHeaders(Collections.emptyMap())
                    .setModifiedDate(randomNonNegativeLong())
                    .setVersion(randomNonNegativeLong())
                    .build()
            )
            .collect(Collectors.toMap(pm -> pm.getPolicy().getId(), pm -> pm));

        Metadata metadata = Metadata.builder()
            .putCustom(SnapshotLifecycleMetadata.TYPE, new SnapshotLifecycleMetadata(policyMetadataMap, mode, new SnapshotLifecycleStats()))
            .build();
        return ClusterState.builder(new ClusterName("cluster")).metadata(metadata).build();
    }

    private static class MockSnapshotRetentionTask extends SnapshotRetentionTask {
        private final Supplier<Map<String, List<Tuple<SnapshotId, String>>>> snapshotRetriever;
        private final DeleteSnapshotMock deleteRunner;

        MockSnapshotRetentionTask(
            Client client,
            ClusterService clusterService,
            SnapshotHistoryStore historyStore,
            Supplier<Map<String, List<Tuple<SnapshotId, String>>>> snapshotRetriever,
            DeleteSnapshotMock deleteRunner,
            LongSupplier nanoSupplier
        ) {
            super(client, clusterService, nanoSupplier, historyStore);
            this.snapshotRetriever = snapshotRetriever;
            this.deleteRunner = deleteRunner;
        }

        @Override
        void getSnapshotsEligibleForDeletion(
            Collection<String> repositories,
            Map<String, SnapshotLifecyclePolicy> policies,
            ActionListener<Map<String, List<Tuple<SnapshotId, String>>>> listener
        ) {
            listener.onResponse(this.snapshotRetriever.get());
        }

        @Override
        void deleteSnapshot(
            String policyId,
            String repo,
            SnapshotId snapshot,
            AtomicReference<SnapshotLifecycleStats> slmStats,
            ActionListener<AcknowledgedResponse> listener
        ) {
            deleteRunner.apply(policyId, repo, snapshot, slmStats, listener);
        }
    }

    @FunctionalInterface
    interface DeleteSnapshotMock {
        void apply(
            String policyId,
            String repo,
            SnapshotId snapshot,
            AtomicReference<SnapshotLifecycleStats> slmStats,
            ActionListener<AcknowledgedResponse> listener
        );
    }
}
