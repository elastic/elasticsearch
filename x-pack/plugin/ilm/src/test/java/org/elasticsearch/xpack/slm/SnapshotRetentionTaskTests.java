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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotRetentionConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class SnapshotRetentionTaskTests extends ESTestCase {

    public void testGetAllPoliciesWithRetentionEnabled() {
        SnapshotLifecyclePolicy policyWithout = new SnapshotLifecyclePolicy("policyWithout", "snap", "1 * * * * ?",
            "repo", null, SnapshotRetentionConfiguration.EMPTY);
        SnapshotLifecyclePolicy policyWithout2 = new SnapshotLifecyclePolicy("policyWithout2", "snap", "1 * * * * ?",
            "repo", null, new SnapshotRetentionConfiguration(null));
        SnapshotLifecyclePolicy policyWith = new SnapshotLifecyclePolicy("policyWith", "snap", "1 * * * * ?",
            "repo", null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30)));

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
            "repo", null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30)));
        SnapshotLifecyclePolicy policyWithNoRetention = new SnapshotLifecyclePolicy("policy", "snap", "1 * * * * ?",
            "repo", null, randomBoolean() ? null : SnapshotRetentionConfiguration.EMPTY);
        Map<String, SnapshotLifecyclePolicy> policyMap = Collections.singletonMap("policy", policy);
        Map<String, SnapshotLifecyclePolicy> policyWithNoRetentionMap = Collections.singletonMap("policy", policyWithNoRetention);
        Function<SnapshotInfo, List<Tuple<String, SnapshotInfo>>> mkInfos = i -> Collections.singletonList(new Tuple<>("repo", i));

        // Test when user metadata is null
        SnapshotInfo info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            0L, "reason", 1L, 1, Collections.emptyList(), true, null);
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));

        // Test when no retention is configured
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            0L, "reason", 1L, 1, Collections.emptyList(), true, null);
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyWithNoRetentionMap), equalTo(false));

        // Test when user metadata is a map that doesn't contain "policy"
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            0L, "reason", 1L, 1, Collections.emptyList(), true, Collections.singletonMap("foo", "bar"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));

        // Test with an ancient snapshot that should be expunged
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            0L, "reason", 1L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(true));

        // Test with a snapshot that's start date is old enough to be expunged (but the finish date is not)
        long time = System.currentTimeMillis() - TimeValue.timeValueDays(30).millis() - 1;
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            time, "reason", time + TimeValue.timeValueDays(4).millis(), 1, Collections.emptyList(),
            true, Collections.singletonMap("policy", "policy"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(true));

        // Test with a fresh snapshot that should not be expunged
        info = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
            System.currentTimeMillis(), "reason", System.currentTimeMillis() + 1,
            1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
        assertThat(SnapshotRetentionTask.snapshotEligibleForDeletion(info, mkInfos.apply(info), policyMap), equalTo(false));
    }

    public void testRetentionTask() throws Exception {
        try (ThreadPool threadPool = new TestThreadPool("slm-test");
             ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
             Client noOpClient = new NoOpClient("slm-test")) {

            SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy("policy", "snap", "1 * * * * ?",
                "repo", null, new SnapshotRetentionConfiguration(TimeValue.timeValueDays(30)));

            ClusterState state = createState(policy);
            ClusterServiceUtils.setState(clusterService, state);

            final SnapshotInfo eligibleSnapshot = new SnapshotInfo(new SnapshotId("name", "uuid"), Collections.singletonList("index"),
                0L, "reason", 1L, 1, Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));
            final SnapshotInfo ineligibleSnapshot = new SnapshotInfo(new SnapshotId("name2", "uuid2"), Collections.singletonList("index"),
                System.currentTimeMillis(), "reason", System.currentTimeMillis() + 1, 1,
                Collections.emptyList(), true, Collections.singletonMap("policy", "policy"));

            AtomicReference<List<SnapshotInfo>> deleted = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            MockSnapshotRetentionTask retentionTask = new MockSnapshotRetentionTask(noOpClient, clusterService,
                () -> {
                    List<Tuple<String, SnapshotInfo>> snaps = new ArrayList<>(2);
                    snaps.add(new Tuple<>("repo", eligibleSnapshot));
                    snaps.add(new Tuple<>("repo", ineligibleSnapshot));
                    logger.info("--> retrieving snapshots [{}]", snaps);
                    return snaps;
                },
                snapsToDelete -> {
                    logger.info("--> deleting {}", snapsToDelete);
                    deleted.set(snapsToDelete.stream().map(Tuple::v2).collect(Collectors.toList()));
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

    private class MockSnapshotRetentionTask extends SnapshotRetentionTask {

        private final Supplier<List<Tuple<String, SnapshotInfo>>> snapshotRetriever;
        private final Consumer<List<Tuple<String, SnapshotInfo>>> snapshotDeleter;

        MockSnapshotRetentionTask(Client client,
                                  ClusterService clusterService,
                                  Supplier<List<Tuple<String, SnapshotInfo>>> snapshotRetriever,
                                  Consumer<List<Tuple<String, SnapshotInfo>>> snapshotDeleter) {
            super(client, clusterService);
            this.snapshotRetriever = snapshotRetriever;
            this.snapshotDeleter = snapshotDeleter;
        }

        @Override
        void getAllSnapshots(Collection<String> repositories,
                             ActionListener<List<Tuple<String, SnapshotInfo>>> listener,
                             Consumer<Exception> errorHandler) {
            listener.onResponse(this.snapshotRetriever.get());
        }

        @Override
        void deleteSnapshots(List<Tuple<String, SnapshotInfo>> snapshotsToDelete) {
            this.snapshotDeleter.accept(snapshotsToDelete);
        }
    }
}
