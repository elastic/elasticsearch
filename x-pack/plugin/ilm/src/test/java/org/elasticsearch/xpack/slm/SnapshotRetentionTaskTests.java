/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.slm.history.SnapshotHistoryItem.DELETE_OPERATION;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy("policy", "snap", "1 * * * * ?",
            "repo", null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null));
        SnapshotLifecyclePolicy policyWithNoRetention = new SnapshotLifecyclePolicy("policy", "snap", "1 * * * * ?",
            "repo", null, randomBoolean() ? null : SnapshotRetentionConfiguration.EMPTY);
        Map<String, SnapshotLifecyclePolicy> policyMap = Collections.singletonMap("policy", policy);
        Map<String, SnapshotLifecyclePolicy> policyWithNoRetentionMap = Collections.singletonMap("policy", policyWithNoRetention);
        Function<SnapshotInfo, Map<String, List<SnapshotInfo>>> mkInfos = i ->
            Collections.singletonMap("repo", Collections.singletonList(i));

        // Test when user metadata is null
        SnapshotInfo info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            Collections.emptyList(),0L, null, 1L, 1, Collections.emptyList(), true, null);
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));

        // Test when no retention is configured
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"), Collections.emptyList(),
            0L, null, 1L, 1, Collections.emptyList(), true, null);
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyWithNoRetentionMap), equalTo(false));

        // Test when user metadata is a map that doesn't contain "policy"
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"), Collections.emptyList(),
            0L, null, 1L, 1, Collections.emptyList(), true, Collections.singletonMap("foo", "bar"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));

        // Test with an ancient snapshot that should be expunged
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"), Collections.emptyList(),
            0L, null, 1L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(true));

        // Test with a snapshot that's start date is old enough to be expunged (but the finish date is not)
        long time = System.currentTimeMillis() - TimeValue.timeValueDays(30).millis() - 1;
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"), Collections.emptyList(),
            time, null, time + TimeValue.timeValueDays(4).millis(), 1, Collections.emptyList(),
            true, Collections.singletonMap("policy", "policy"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(true));

        // Test with a fresh snapshot that should not be expunged
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"), Collections.emptyList(),
            System.currentTimeMillis(), null, System.currentTimeMillis() + 1,
            1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
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

            final SnapshotInfo eligibleSnapshot = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
                Collections.emptyList(), 0L, null, 1L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", policyId));
            final SnapshotInfo ineligibleSnapshot = new SnapshotInfo(new SnapshotId("name2", "uuid2"), Collections.singletonList("index"),
                Collections.emptyList(), System.currentTimeMillis(), null, System.currentTimeMillis() + 1, 1,
                Collections.emptyList(), true, Collections.singletonMap("policy", policyId));

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
                threadPool,
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
                        listener.onResponse(new AcknowledgedResponse(true));
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

    public void testSuccessfulTimeBoundedDeletion() throws Exception {
        timeBoundedDeletion(true);
    }

    public void testFailureTimeBoundedDeletion() throws Exception {
        timeBoundedDeletion(false);
    }

    private void timeBoundedDeletion(final boolean deletionSuccess) throws Exception {
        ThreadPool threadPool = new TestThreadPool("slm-test");
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
             Client noOpClient = new NoOpClient("slm-test")) {

            final String policyId = "policy";
            final String repoId = "repo";
            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyId, "snap", "1 * * * * ?",
                repoId, null, new SnapshotRetentionConfiguration(null, null, 1));

            ClusterState state = createState(policy);
            state = ClusterState.builder(state)
                .metadata(Metadata.builder(state.metadata())
                    .transientSettings(Settings.builder()
                        .put(LifecycleSettings.SLM_RETENTION_DURATION, "500ms")
                        .build())).build();
            ClusterServiceUtils.setState(clusterService, state);

            final SnapshotInfo snap1 = new SnapshotInfo(new SnapshotId("name1", "uuid1"), Collections.singletonList("index"),
                Collections.emptyList(), 0L, null, 1L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", policyId));
            final SnapshotInfo snap2 = new SnapshotInfo(new SnapshotId("name2", "uuid2"), Collections.singletonList("index"),
                Collections.emptyList(), 1L, null, 2L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", policyId));
            final SnapshotInfo snap3 = new SnapshotInfo(new SnapshotId("name3", "uuid3"), Collections.singletonList("index"),
                Collections.emptyList(), 2L, null, 3L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", policyId));
            final SnapshotInfo snap4 = new SnapshotInfo(new SnapshotId("name4", "uuid4"), Collections.singletonList("index"),
                Collections.emptyList(), 3L, null, 4L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", policyId));
            final SnapshotInfo snap5 = new SnapshotInfo(new SnapshotId("name5", "uuid5"), Collections.singletonList("index"),
                Collections.emptyList(), 4L, null, 5L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", policyId));

            final Set<SnapshotId> deleted = ConcurrentHashMap.newKeySet();
            // We're expected two deletions before they hit the "taken too long" test, so have a latch of 2
            CountDownLatch deletionLatch = new CountDownLatch(2);
            CountDownLatch historyLatch = new CountDownLatch(2);
            Set<String> deletedSnapshotsInHistory = ConcurrentHashMap.newKeySet();
            AtomicLong nanos = new AtomicLong(System.nanoTime());
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
                threadPool,
                () -> {
                    List<SnapshotInfo> snaps = Arrays.asList(snap1, snap2, snap3, snap4, snap5);
                    logger.info("--> retrieving snapshots [{}]", snaps);
                    return Collections.singletonMap(repoId, snaps);
                },
                (deletionPolicyId, repo, snapId, slmStats, listener) -> {
                    logger.info("--> deleting {}", snapId);
                    // Don't pause until snapshot 2
                    if (snapId.equals(snap2.snapshotId())) {
                        logger.info("--> pausing for 501ms while deleting snap2 to simulate deletion past a threshold");
                        nanos.addAndGet(TimeValue.timeValueMillis(501).nanos());
                    }
                    deleted.add(snapId);
                    if (deletionSuccess) {
                        listener.onResponse(new AcknowledgedResponse(true));
                    } else {
                        listener.onFailure(new RuntimeException("deletion_failed"));
                    }
                    deletionLatch.countDown();
                },
                nanos::get);

            long time = System.currentTimeMillis();
            retentionTask.triggered(new SchedulerEngine.Event(SnapshotRetentionService.SLM_RETENTION_JOB_ID, time, time));

            boolean success = deletionLatch.await(10, TimeUnit.SECONDS);

            assertThat("expected 2 snapshot deletions within 10 seconds, deleted: " + deleted, success, equalTo(true));

            assertNotNull("something should have been deleted", deleted);
            assertThat("two snapshots should have been deleted", deleted.size(), equalTo(2));
            assertThat(deleted, containsInAnyOrder(snap1.snapshotId(), snap2.snapshotId()));

            boolean historySuccess = historyLatch.await(10, TimeUnit.SECONDS);
            assertThat("expected history entries for 2 snapshot deletions", historySuccess, equalTo(true));
            assertThat(deletedSnapshotsInHistory, containsInAnyOrder(snap1.snapshotId().getName(), snap2.snapshotId().getName()));
        } finally {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testOkToDeleteSnapshots() {
        final Snapshot snapshot = new Snapshot("repo", new SnapshotId("name", "uuid"));

        SnapshotsInProgress inProgress = SnapshotsInProgress.of(
            List.of(new SnapshotsInProgress.Entry(
                snapshot, true, false, SnapshotsInProgress.State.INIT,
                Collections.singletonList(new IndexId("name", "id")), 0, 0,
                ImmutableOpenMap.<ShardId, SnapshotsInProgress.ShardSnapshotStatus>builder().build(), Collections.emptyMap(),
                VersionUtils.randomVersion(random()))));
        ClusterState state = ClusterState.builder(new ClusterName("cluster"))
            .putCustom(SnapshotsInProgress.TYPE, inProgress)
            .build();

        assertThat(SnapshotRetentionTask.okayToDeleteSnapshots(state), equalTo(false));

        SnapshotDeletionsInProgress delInProgress = SnapshotDeletionsInProgress.of(
                Collections.singletonList(new SnapshotDeletionsInProgress.Entry(
                        Collections.singletonList(snapshot.getSnapshotId()), snapshot.getRepository(), 0, 0)));
        state = ClusterState.builder(new ClusterName("cluster"))
            .putCustom(SnapshotDeletionsInProgress.TYPE, delInProgress)
            .build();

        assertThat(SnapshotRetentionTask.okayToDeleteSnapshots(state), equalTo(false));

        RepositoryCleanupInProgress cleanupInProgress =
            new RepositoryCleanupInProgress(List.of(new RepositoryCleanupInProgress.Entry("repo", 0)));
        state = ClusterState.builder(new ClusterName("cluster"))
            .putCustom(RepositoryCleanupInProgress.TYPE, cleanupInProgress)
            .build();

        assertThat(SnapshotRetentionTask.okayToDeleteSnapshots(state), equalTo(false));

        RestoreInProgress restoreInProgress = mock(RestoreInProgress.class);
        state = ClusterState.builder(new ClusterName("cluster"))
            .putCustom(RestoreInProgress.TYPE, restoreInProgress)
            .build();

        assertThat(SnapshotRetentionTask.okayToDeleteSnapshots(state), equalTo(false));

        restoreInProgress = mock(RestoreInProgress.class);
        when(restoreInProgress.isEmpty()).thenReturn(true);
        state = ClusterState.builder(new ClusterName("cluster"))
            .putCustom(RestoreInProgress.TYPE, restoreInProgress)
            .build();

        assertThat(SnapshotRetentionTask.okayToDeleteSnapshots(state), equalTo(true));
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
                             Collections.singleton(GetSnapshotsResponse.Response.snapshots(repoId, Collections.emptyList()))));
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
                    (historyItem) -> fail("should never write history")),
                threadPool);

            AtomicReference<Exception> errHandlerCalled = new AtomicReference<>(null);
            task.getAllRetainableSnapshots(Collections.singleton(repoId), new ActionListener<>() {
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

            assertNotNull(errHandlerCalled.get());
            assertThat(errHandlerCalled.get().getMessage(), equalTo("forced failure"));
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
                         listener.onResponse((Response) new AcknowledgedResponse(true));
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
                    (historyItem) -> fail("should never write history")),
                threadPool);

            AtomicBoolean onFailureCalled = new AtomicBoolean(false);
            task.deleteSnapshot("policy", "foo", new SnapshotId("name", "uuid"),
                new SnapshotLifecycleStats(0, 0, 0, 0, new HashMap<>()), new ActionListener<>() {
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
                threadPool,
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
                threadPool,
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
                                  ThreadPool threadPool,
                                  Supplier<Map<String, List<SnapshotInfo>>> snapshotRetriever,
                                  DeleteSnapshotMock deleteRunner,
                                  LongSupplier nanoSupplier) {
            super(client, clusterService, nanoSupplier, historyStore, threadPool);
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
