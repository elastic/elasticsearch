/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.slm.history.SnapshotHistoryItem.DELETE_OPERATION;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class SnapshotRetentionTaskTests extends ESTestCase {

    public void testGetAllPoliciesWithRetentionEnabled() {
        SnapshotLifecyclePolicy policyWithout = new SnapshotLifecyclePolicy("policyWithout", "snap", "1 * * * * ?",
            "repo", null, SnapshotRetentionConfiguration.EMPTY);
        SnapshotLifecyclePolicy policyWithout2 = new SnapshotLifecyclePolicy("policyWithout2", "snap", "1 * * * * ?",
            "repo", null, new SnapshotRetentionConfiguration(null, null, null));
        SnapshotLifecyclePolicy policyWith = new SnapshotLifecyclePolicy("policyWith", "snap", "1 * * * * ?",
            "repo", null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null));

        // Test with no SLM metadata
        ClusterState state = ClusterState.builder(new ClusterName("cluster")).build();
        assertThat(SnapshotRetentionTask.getAllPoliciesWithRetentionEnabled(state), equalTo(Collections.emptyMap()));

        // Test with empty SLM metadata
        Metadata metadata = Metadata.builder()
            .putCustom(SnapshotLifecycleMetadata.TYPE,
                new SnapshotLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING, new SnapshotLifecycleStats()))
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

    public void testSnapshotEligibleForDeletion() {
        final String repoName = "repo";
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy("policy", "snap", "1 * * * * ?",
            repoName, null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null));
        SnapshotLifecyclePolicy policyWithNoRetention = new SnapshotLifecyclePolicy("policy", "snap", "1 * * * * ?",
            repoName, null, randomBoolean() ? null : SnapshotRetentionConfiguration.EMPTY);
        Map<String, SnapshotLifecyclePolicy> policyMap = Collections.singletonMap("policy", policy);
        Map<String, SnapshotLifecyclePolicy> policyWithNoRetentionMap = Collections.singletonMap("policy", policyWithNoRetention);
        Function<SnapshotInfo, Map<String, List<SnapshotInfo>>> mkInfos = i ->
            Collections.singletonMap(repoName, Collections.singletonList(i));

        // Test when user metadata is null
        SnapshotInfo info = new SnapshotInfo(
            new Snapshot(repoName, new SnapshotId("name", "uuid")),
            Collections.singletonList("index"),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            1L,
            1,
            Collections.emptyList(),
            true,
            null,
            0L,
            Collections.emptyMap());
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));

        // Test when no retention is configured
        info = new SnapshotInfo(
            new Snapshot(repoName, new SnapshotId("name", "uuid")),
            Collections.singletonList("index"),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            1L,
            1,
            Collections.emptyList(),
            true,
            null,
            0L,
            Collections.emptyMap());
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyWithNoRetentionMap), equalTo(false));

        // Test when user metadata is a map that doesn't contain "policy"
        info = new SnapshotInfo(
            new Snapshot(repoName, new SnapshotId("name", "uuid")),
            Collections.singletonList("index"),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            1L,
            1,
            Collections.emptyList(),
            true,
            Collections.singletonMap("foo", "bar"),
            0L,
            Collections.emptyMap());
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));

        // Test with an ancient snapshot that should be expunged
        info = new SnapshotInfo(
            new Snapshot(repoName, new SnapshotId("name", "uuid")),
            Collections.singletonList("index"),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            1L,
            1,
            Collections.emptyList(),
            true,
            Collections.singletonMap("policy", "policy"),
            0L,
            Collections.emptyMap());
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(true));

        // Test with a snapshot that's start date is old enough to be expunged (but the finish date is not)
        long time = System.currentTimeMillis() - TimeValue.timeValueDays(30).millis() - 1;
        info = new SnapshotInfo(
            new Snapshot(repoName, new SnapshotId("name", "uuid")),
            Collections.singletonList("index"),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            time + TimeValue.timeValueDays(4).millis(),
            1,
            Collections.emptyList(),
            true,
            Collections.singletonMap("policy", "policy"),
            time,
            Collections.emptyMap());
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(true));

        // Test with a fresh snapshot that should not be expunged
        info = new SnapshotInfo(
            new Snapshot(repoName, new SnapshotId("name", "uuid")),
            Collections.singletonList("index"),
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            System.currentTimeMillis() + 1,
            1,
            Collections.emptyList(),
            true,
            Collections.singletonMap("policy", "policy"),
            System.currentTimeMillis(),
            Collections.emptyMap());
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));
    }

    public void testRetentionTaskSuccess() throws Exception {
        retentionTaskTest(true);
    }

    public void testRetentionTaskFailure() throws Exception {
        retentionTaskTest(false);
    }

    private void retentionTaskTest(final boolean deletionSuccess) throws Exception {
        ThreadPool threadPool = new TestThreadPool("slm-test");
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
             Client noOpClient = new NoOpClient("slm-test")) {

            final String policyId = "policy";
            final String repoId = "repo";
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyId, "snap", "1 * * * * ?",
                repoId, null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null));

            ClusterState state = createState(policy);
            ClusterServiceUtils.setState(clusterService, state);

            final SnapshotInfo eligibleSnapshot = new SnapshotInfo(
                    new Snapshot(repoId, new SnapshotId("name", "uuid")), Collections.singletonList("index"),
                Collections.emptyList(), Collections.emptyList(), null, 1L, 1, Collections.emptyList(), true,
                Collections.singletonMap("policy", policyId), 0L, Collections.emptyMap());
            final SnapshotInfo ineligibleSnapshot = new SnapshotInfo(
                new Snapshot(repoId, new SnapshotId("name2", "uuid2")),
                Collections.singletonList("index"),
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                System.currentTimeMillis() + 1,
                1,
                Collections.emptyList(),
                true,
                Collections.singletonMap("policy", policyId),
                System.currentTimeMillis(),
                Collections.emptyMap());

            Set<SnapshotId> deleted = ConcurrentHashMap.newKeySet();
            Set<String> deletedSnapshotsInHistory = ConcurrentHashMap.newKeySet();
            CountDownLatch deletionLatch = new CountDownLatch(1);
            CountDownLatch historyLatch = new CountDownLatch(1);

            MockSnapshotRetentionTask retentionTask = new MockSnapshotRetentionTask(noOpClient, clusterService,
                new SnapshotLifecycleTaskTests.VerifyingHistoryStore(noOpClient, ZoneOffset.UTC,
                    (historyItem) -> {
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
                    List<SnapshotInfo> snaps = new ArrayList<>(2);
                    snaps.add(eligibleSnapshot);
                    snaps.add(ineligibleSnapshot);
                    logger.info("--> retrieving snapshots [{}]", snaps);
                    return Collections.singletonMap(repoId, snaps);
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
                System::nanoTime);

            long time = System.currentTimeMillis();
            retentionTask.triggered(new SchedulerEngine.Event(SnapshotRetentionService.SLM_RETENTION_JOB_ID, time, time));

            deletionLatch.await(10, TimeUnit.SECONDS);

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
        final String policyId = "policy";
        final String repoId = "repo";
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
             Client noOpClient = new NoOpClient("slm-test") {

                 @Override
                 @SuppressWarnings("unchecked")
                 protected <Request extends ActionRequest, Response extends ActionResponse>
                 void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                     if (request instanceof GetSnapshotsRequest) {
                         logger.info("--> called");
                         listener.onResponse((Response) new GetSnapshotsResponse(
                             Collections.emptyList(), Collections.emptyMap(), null, 0, 0));
                     } else {
                         super.doExecute(action, request, listener);
                     }
                 }
             }) {
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyId, "snap", "1 * * * * ?",
                repoId, null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null));

            ClusterState state = createState(policy);
            ClusterServiceUtils.setState(clusterService, state);

            SnapshotRetentionTask task = new SnapshotRetentionTask(noOpClient, clusterService,
                System::nanoTime,
                new SnapshotLifecycleTaskTests.VerifyingHistoryStore(noOpClient, ZoneOffset.UTC,
                    (historyItem) -> fail("should never write history"))
            );

            AtomicReference<Exception> errHandlerCalled = new AtomicReference<>(null);
            task.getAllRetainableSnapshots(Collections.singleton(repoId), new ActionListener<Map<String, List<SnapshotInfo>>>() {
                @Override
                public void onResponse(Map<String, List<SnapshotInfo>> stringListMap) {
                    logger.info("--> forcing failure");
                    throw new ElasticsearchException("forced failure");
                }

                @Override
                public void onFailure(Exception e) {
                    fail("we have another err handler that should have been called");
                }
            }, errHandlerCalled::set);

            assertBusy(() -> {
                assertNotNull(errHandlerCalled.get());
                assertThat(errHandlerCalled.get().getMessage(), equalTo("forced failure"));
            });

        } finally {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testErrStillRunsFailureHandlerWhenDeleting() throws Exception {
        ThreadPool threadPool = new TestThreadPool("slm-test");
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
             Client noOpClient = new NoOpClient("slm-test") {

                 @Override
                 @SuppressWarnings("unchecked")
                 protected <Request extends ActionRequest, Response extends ActionResponse>
                 void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                     if (request instanceof DeleteSnapshotRequest) {
                         logger.info("--> called");
                         listener.onResponse((Response) AcknowledgedResponse.TRUE);
                     } else {
                         super.doExecute(action, request, listener);
                     }
                 }
             }) {
            final String policyId = "policy";
            final String repoId = "repo";
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyId, "snap", "1 * * * * ?",
                repoId, null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null));

            ClusterState state = createState(policy);
            ClusterServiceUtils.setState(clusterService, state);

            SnapshotRetentionTask task = new SnapshotRetentionTask(noOpClient, clusterService,
                System::nanoTime,
                new SnapshotLifecycleTaskTests.VerifyingHistoryStore(noOpClient, ZoneOffset.UTC,
                    (historyItem) -> fail("should never write history"))
            );

            AtomicBoolean onFailureCalled = new AtomicBoolean(false);
            task.deleteSnapshot("policy", "foo", new SnapshotId("name", "uuid"),
                new SnapshotLifecycleStats(0, 0, 0, 0, new HashMap<>()), new ActionListener<AcknowledgedResponse>() {
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
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
             Client noOpClient = new NoOpClient("slm-test")) {
            final String policyId = "policy";
            final String repoId = "repo";
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyId, "snap", "1 * * * * ?",
                repoId, null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null));

            ClusterState state = createState(mode, policy);
            ClusterServiceUtils.setState(clusterService, state);

            SnapshotRetentionTask task = new MockSnapshotRetentionTask(noOpClient, clusterService,
                new SnapshotLifecycleTaskTests.VerifyingHistoryStore(noOpClient, ZoneOffset.UTC,
                    (historyItem) -> fail("should never write history")),
                    () -> {
                    fail("should not retrieve snapshots");
                    return null;
                },
                (a, b, c, d, e) -> fail("should not delete snapshots"),
                System::nanoTime);

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
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
             Client noOpClient = new NoOpClient("slm-test")) {
            final String policyId = "policy";
            final String repoId = "repo";
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyId, "snap", "1 * * * * ?",
                repoId, null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null));

            ClusterState state = createState(mode, policy);
            ClusterServiceUtils.setState(clusterService, state);

            AtomicBoolean retentionWasRun = new AtomicBoolean(false);
            MockSnapshotRetentionTask task = new MockSnapshotRetentionTask(noOpClient, clusterService,
                new SnapshotLifecycleTaskTests.VerifyingHistoryStore(noOpClient, ZoneOffset.UTC, (historyItem) -> {
                }),
                    () -> {
                    retentionWasRun.set(true);
                    return Collections.emptyMap();
                },
                (deletionPolicyId, repo, snapId, slmStats, listener) -> {
                },
                System::nanoTime);

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
            .map(policy -> SnapshotLifecyclePolicyMetadata.builder()
                .setPolicy(policy)
                .setHeaders(Collections.emptyMap())
                .setModifiedDate(randomNonNegativeLong())
                .setVersion(randomNonNegativeLong())
                .build())
            .collect(Collectors.toMap(pm -> pm.getPolicy().getId(), pm -> pm));

        Metadata metadata = Metadata.builder()
            .putCustom(SnapshotLifecycleMetadata.TYPE,
                new SnapshotLifecycleMetadata(policyMetadataMap, mode, new SnapshotLifecycleStats()))
            .build();
        return ClusterState.builder(new ClusterName("cluster"))
            .metadata(metadata)
            .build();
    }

    private static class MockSnapshotRetentionTask extends SnapshotRetentionTask {
        private final Supplier<Map<String, List<SnapshotInfo>>> snapshotRetriever;
        private final DeleteSnapshotMock deleteRunner;

        MockSnapshotRetentionTask(Client client,
                                  ClusterService clusterService,
                                  SnapshotHistoryStore historyStore,
                                  Supplier<Map<String, List<SnapshotInfo>>> snapshotRetriever,
                                  DeleteSnapshotMock deleteRunner,
                                  LongSupplier nanoSupplier) {
            super(client, clusterService, nanoSupplier, historyStore);
            this.snapshotRetriever = snapshotRetriever;
            this.deleteRunner = deleteRunner;
        }

        @Override
        void getAllRetainableSnapshots(Collection<String> repositories,
                                       ActionListener<Map<String, List<SnapshotInfo>>> listener,
                                       Consumer<Exception> errorHandler) {
            listener.onResponse(this.snapshotRetriever.get());
        }

        @Override
        void deleteSnapshot(String policyId, String repo, SnapshotId snapshot, SnapshotLifecycleStats slmStats,
                            ActionListener<AcknowledgedResponse> listener) {
            deleteRunner.apply(policyId, repo, snapshot, slmStats, listener);
        }
    }

    @FunctionalInterface
    interface DeleteSnapshotMock {
        void apply(String policyId, String repo, SnapshotId snapshot, SnapshotLifecycleStats slmStats,
                   ActionListener<AcknowledgedResponse> listener);
    }
}
