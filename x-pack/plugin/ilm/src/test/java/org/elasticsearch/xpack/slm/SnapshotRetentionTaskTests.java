/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

@TestLogging(value = "org.elasticsearch.xpack.slm:TRACE", reason = "I want to log")
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
        MetaData metaData = MetaData.builder()
            .putCustom(SnapshotLifecycleMetadata.TYPE, new SnapshotLifecycleMetadata(Collections.emptyMap(), OperationMode.RUNNING))
            .build();
        state = ClusterState.builder(new ClusterName("cluster")).metaData(metaData).build();
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
            0L, null, 1L, 1, Collections.emptyList(), true, null);
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));

        // Test when no retention is configured
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            0L, null, 1L, 1, Collections.emptyList(), true, null);
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyWithNoRetentionMap), equalTo(false));

        // Test when user metadata is a map that doesn't contain "policy"
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            0L, null, 1L, 1, Collections.emptyList(), true, Collections.singletonMap("foo", "bar"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));

        // Test with an ancient snapshot that should be expunged
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            0L, null, 1L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(true));

        // Test with a snapshot that's start date is old enough to be expunged (but the finish date is not)
        long time = System.currentTimeMillis() - TimeValue.timeValueDays(30).millis() - 1;
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            time, null, time + TimeValue.timeValueDays(4).millis(), 1, Collections.emptyList(),
            true, Collections.singletonMap("policy", "policy"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(true));

        // Test with a fresh snapshot that should not be expunged
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            System.currentTimeMillis(), null, System.currentTimeMillis() + 1,
            1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));
    }

    public void testRetentionTask() throws Exception {
        try (ThreadPool threadPool = new TestThreadPool("slm-test");
             ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
             Client noOpClient = new NoOpClient("slm-test")) {

            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy("policy", "snap", "1 * * * * ?",
                "repo", null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30), null, null));

            ClusterState state = createState(policy);
            ClusterServiceUtils.setState(clusterService, state);

            final SnapshotInfo eligibleSnapshot = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
                0L, null, 1L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
            final SnapshotInfo ineligibleSnapshot = new SnapshotInfo(new SnapshotId("name2", "uuid2"), Collections.singletonList("index"),
                System.currentTimeMillis(), null, System.currentTimeMillis() + 1, 1,
                Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));

            AtomicReference<List<SnapshotInfo>> deleted = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            MockSnapshotRetentionTask retentionTask = new MockSnapshotRetentionTask(noOpClient, clusterService,
                () -> {
                    List<SnapshotInfo> snaps = new ArrayList<>(2);
                    snaps.add(eligibleSnapshot);
                    snaps.add(ineligibleSnapshot);
                    logger.info("--> retrieving snapshots [{}]", snaps);
                    return Collections.singletonMap("repo", snaps);
                },
                snapsToDelete -> {
                    logger.info("--> deleting {}", snapsToDelete);
                    deleted.set(snapsToDelete.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
                    latch.countDown();
                });

            long time = System.currentTimeMillis();
            retentionTask.triggered(new SchedulerEngine.Event(SnapshotRetentionService.SLM_RETENTION_JOB_ID, time, time));

            latch.await(10, TimeUnit.SECONDS);

            assertNotNull("something should have been deleted", deleted.get());
            assertThat("one snapshot should have been deleted", deleted.get().size(), equalTo(1));
            assertThat(deleted.get().get(0), equalTo(eligibleSnapshot));

            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    public void testTimeBoundedDeletion() throws Exception {
        try (ThreadPool threadPool = new TestThreadPool("slm-test");
             ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
             Client noOpClient = new NoOpClient("slm-test")) {

            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy("policy", "snap", "1 * * * * ?",
                "repo", null, new SnapshotRetentionConfiguration(null, null,1));

            ClusterState state = createState(policy);
            state = ClusterState.builder(state)
                .metaData(MetaData.builder(state.metaData())
                    .transientSettings(Settings.builder()
                        .put(LifecycleSettings.SLM_RETENTION_DURATION, "500ms")
                        .build())).build();
            ClusterServiceUtils.setState(clusterService, state);

            final SnapshotInfo snap1 = new SnapshotInfo(new SnapshotId("name1", "uuid1"), Collections.singletonList("index"),
                0L, null, 1L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
            final SnapshotInfo snap2 = new SnapshotInfo(new SnapshotId("name2", "uuid2"), Collections.singletonList("index"),
                1L, null, 2L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
            final SnapshotInfo snap3 = new SnapshotInfo(new SnapshotId("name3", "uuid3"), Collections.singletonList("index"),
                2L, null, 3L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
            final SnapshotInfo snap4 = new SnapshotInfo(new SnapshotId("name4", "uuid4"), Collections.singletonList("index"),
                3L, null, 4L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
            final SnapshotInfo snap5 = new SnapshotInfo(new SnapshotId("name5", "uuid5"), Collections.singletonList("index"),
                4L, null, 5L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));

            final Set<SnapshotId> deleted = ConcurrentHashMap.newKeySet();
            // We're expected two deletions before they hit the "taken too long" test, so have a latch of 2
            CountDownLatch latch = new CountDownLatch(2);
            OverrideDeleteSnapshotRetentionTask retentionTask = new OverrideDeleteSnapshotRetentionTask(noOpClient, clusterService,
                () -> {
                    List<SnapshotInfo> snaps = Arrays.asList(snap1, snap2, snap3, snap4, snap5);
                    logger.info("--> retrieving snapshots [{}]", snaps);
                    return Collections.singletonMap("repo", snaps);
                },
                (repo, snapshotId) -> {
                    // Don't pause until snapshot 2
                    if (snapshotId.equals(snap2.snapshotId())) {
                        try {
                            logger.info("--> pausing for 501ms while deleting snap2 to simulate deletion past a threshold");
                            Thread.sleep(501);
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                    deleted.add(snapshotId);
                    latch.countDown();
                });

            long time = System.currentTimeMillis();
            retentionTask.triggered(new SchedulerEngine.Event(SnapshotRetentionService.SLM_RETENTION_JOB_ID, time, time));

            boolean success = latch.await(10, TimeUnit.SECONDS);

            assertThat("expected 2 snapshot deletions within 10 seconds, deleted: " + deleted, success, equalTo(true));

            assertNotNull("something should have been deleted", deleted);
            assertThat("two snapshots should have been deleted", deleted.size(), equalTo(2));
            assertThat(deleted, containsInAnyOrder(snap1.snapshotId(), snap2.snapshotId()));

            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.SECONDS);
        }

    }

    public ClusterState createState(SnapshotLifecyclePolicy... policies) {
        Map<String, SnapshotLifecyclePolicyMetadata> policyMetadataMap = Arrays.stream(policies)
            .map(policy -> SnapshotLifecyclePolicyMetadata.builder()
                .setPolicy(policy)
                .setHeaders(Collections.emptyMap())
                .setModifiedDate(randomNonNegativeLong())
                .setVersion(randomNonNegativeLong())
                .build())
            .collect(Collectors.toMap(pm -> pm.getPolicy().getId(), pm -> pm));

        MetaData metaData = MetaData.builder()
            .putCustom(SnapshotLifecycleMetadata.TYPE, new SnapshotLifecycleMetadata(policyMetadataMap, OperationMode.RUNNING))
            .build();
        return ClusterState.builder(new ClusterName("cluster"))
            .metaData(metaData)
            .build();
    }

    private static class MockSnapshotRetentionTask extends SnapshotRetentionTask {

        private final Supplier<Map<String, List<SnapshotInfo>>> snapshotRetriever;
        private final Consumer<Map<String, List<SnapshotInfo>>> snapshotDeleter;

        MockSnapshotRetentionTask(Client client,
                                  ClusterService clusterService,
                                  Supplier<Map<String, List<SnapshotInfo>>> snapshotRetriever,
                                  Consumer<Map<String, List<SnapshotInfo>>> snapshotDeleter) {
            super(client, clusterService);
            this.snapshotRetriever = snapshotRetriever;
            this.snapshotDeleter = snapshotDeleter;
        }

        @Override
        void getAllSuccessfulSnapshots(Collection<String> repositories,
                                       ActionListener<Map<String, List<SnapshotInfo>>> listener,
                                       Consumer<Exception> errorHandler) {
            listener.onResponse(this.snapshotRetriever.get());
        }

        @Override
        void deleteSnapshots(Map<String, List<SnapshotInfo>> snapshotsToDelete, TimeValue maxDeleteTime) {
            this.snapshotDeleter.accept(snapshotsToDelete);
        }
    }

    private static class OverrideDeleteSnapshotRetentionTask extends SnapshotRetentionTask {
        private final Supplier<Map<String, List<SnapshotInfo>>> snapshotRetriever;
        private final BiConsumer<String, SnapshotId> deleteRunner;

        OverrideDeleteSnapshotRetentionTask(Client client,
                                            ClusterService clusterService,
                                            Supplier<Map<String, List<SnapshotInfo>>> snapshotRetriever,
                                            BiConsumer<String, SnapshotId> deleteRunner) {
            super(client, clusterService);
            this.snapshotRetriever = snapshotRetriever;
            this.deleteRunner = deleteRunner;
        }

        @Override
        void getAllSuccessfulSnapshots(Collection<String> repositories,
                                       ActionListener<Map<String, List<SnapshotInfo>>> listener,
                                       Consumer<Exception> errorHandler) {
            listener.onResponse(this.snapshotRetriever.get());
        }

        @Override
        void deleteSnapshot(String repo, SnapshotId snapshot) {
            deleteRunner.accept(repo, snapshot);
        }
    }
}
