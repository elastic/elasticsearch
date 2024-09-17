/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.slm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.RegisteredPolicySnapshots;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotInfoUtils;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.elasticsearch.xpack.slm.history.SnapshotHistoryItem;
import org.elasticsearch.xpack.slm.history.SnapshotHistoryStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.ilm.LifecycleSettings.SLM_HISTORY_INDEX_ENABLED_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class SnapshotLifecycleTaskTests extends ESTestCase {

    public void testGetSnapMetadata() {
        final String id = randomAlphaOfLength(4);
        final SnapshotLifecyclePolicyMetadata slpm = makePolicyMeta(id);
        final SnapshotLifecycleMetadata meta = new SnapshotLifecycleMetadata(
            Collections.singletonMap(id, slpm),
            OperationMode.RUNNING,
            new SnapshotLifecycleStats()
        );

        final ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(SnapshotLifecycleMetadata.TYPE, meta).build())
            .build();

        final Optional<SnapshotLifecyclePolicyMetadata> o = SnapshotLifecycleTask.getSnapPolicyMetadata(
            SnapshotLifecycleService.getJobId(slpm),
            state
        );

        assertTrue("the policy metadata should be retrieved from the cluster state", o.isPresent());
        assertThat(o.get(), equalTo(slpm));

        assertFalse(SnapshotLifecycleTask.getSnapPolicyMetadata("bad-jobid", state).isPresent());
    }

    public void testSkipCreatingSnapshotWhenJobDoesNotMatch() {
        final String id = randomAlphaOfLength(4);
        final SnapshotLifecyclePolicyMetadata slpm = makePolicyMeta(id);
        final SnapshotLifecycleMetadata meta = new SnapshotLifecycleMetadata(
            Collections.singletonMap(id, slpm),
            OperationMode.RUNNING,
            new SnapshotLifecycleStats()
        );

        final ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(SnapshotLifecycleMetadata.TYPE, meta).build())
            .build();

        final ThreadPool threadPool = new TestThreadPool("test");
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(SLM_HISTORY_INDEX_ENABLED_SETTING))
        );
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(state, threadPool, settings)) {
            VerifyingClient client = new VerifyingClient(threadPool, (a, r, l) -> {
                fail("should not have tried to take a snapshot");
                return null;
            });
            SnapshotHistoryStore historyStore = new VerifyingHistoryStore(
                null,
                clusterService,
                item -> fail("should not have tried to store an item")
            );

            SnapshotLifecycleTask task = new SnapshotLifecycleTask(client, clusterService, historyStore);

            // Trigger the event, but since the job name does not match, it should
            // not run the function to create a snapshot
            task.triggered(new SchedulerEngine.Event("nonexistent-job", System.currentTimeMillis(), System.currentTimeMillis()));
        }

        threadPool.shutdownNow();
    }

    public void testCreateSnapshotOnTrigger() throws Exception {
        final String id = randomAlphaOfLength(4);
        final SnapshotLifecyclePolicyMetadata slpm = makePolicyMeta(id);
        final SnapshotLifecycleMetadata meta = new SnapshotLifecycleMetadata(
            Collections.singletonMap(id, slpm),
            OperationMode.RUNNING,
            new SnapshotLifecycleStats()
        );

        final ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(SnapshotLifecycleMetadata.TYPE, meta).build())
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.builder("nodeId").name("nodeId").build())
                    .localNodeId("nodeId")
                    .masterNodeId("nodeId")
            )
            .build();

        final ThreadPool threadPool = new TestThreadPool("test");
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(SLM_HISTORY_INDEX_ENABLED_SETTING))
        );
        final String createSnapResponse = Strings.format("""
            {
              "snapshot": {
                "snapshot": "snapshot_1",
                "uuid": "bcP3ClgCSYO_TP7_FCBbBw",
                "version_id": %s,
                "version": "%s",
                "indices": [],
                "include_global_state": true,
                "state": "SUCCESS",
                "start_time": "2019-03-19T22:19:53.542Z",
                "start_time_in_millis": 1553033993542,
                "end_time": "2019-03-19T22:19:53.567Z",
                "end_time_in_millis": 1553033993567,
                "duration_in_millis": 25,
                "failures": [],
                "shards": {
                  "total": 0,
                  "failed": 0,
                  "successful": 0
                }
              }
            }""", Version.CURRENT.id, Version.CURRENT);

        final AtomicBoolean clientCalled = new AtomicBoolean(false);
        final SetOnce<String> snapshotName = new SetOnce<>();
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(state, threadPool, settings)) {
            // This verifying client will verify that we correctly invoked
            // client.admin().createSnapshot(...) with the appropriate
            // request. It also returns a mock real response
            VerifyingClient client = new VerifyingClient(threadPool, (action, request, listener) -> {
                assertFalse(clientCalled.getAndSet(true));
                assertThat(action, sameInstance(TransportCreateSnapshotAction.TYPE));
                assertThat(request, instanceOf(CreateSnapshotRequest.class));

                CreateSnapshotRequest req = (CreateSnapshotRequest) request;

                SnapshotLifecyclePolicy policy = slpm.getPolicy();
                assertThat(req.snapshot(), startsWith(policy.getName() + "-"));
                assertThat(req.repository(), equalTo(policy.getRepository()));
                snapshotName.set(req.snapshot());
                if (req.indices().length > 0) {
                    assertThat(Arrays.asList(req.indices()), equalTo(policy.getConfig().get("indices")));
                }
                boolean globalState = policy.getConfig().get("include_global_state") == null
                    || Boolean.parseBoolean((String) policy.getConfig().get("include_global_state"));
                assertThat(req.includeGlobalState(), equalTo(globalState));

                try {
                    return SnapshotInfoUtils.createSnapshotResponseFromXContent(
                        createParser(JsonXContent.jsonXContent, createSnapResponse)
                    );
                } catch (IOException e) {
                    fail("failed to parse snapshot response");
                    return null;
                }
            });
            final AtomicBoolean historyStoreCalled = new AtomicBoolean(false);
            SnapshotHistoryStore historyStore = new VerifyingHistoryStore(null, clusterService, item -> {
                assertFalse(historyStoreCalled.getAndSet(true));
                final SnapshotLifecyclePolicy policy = slpm.getPolicy();
                assertEquals(policy.getId(), item.getPolicyId());
                assertEquals(policy.getRepository(), item.getRepository());
                assertEquals(policy.getConfig(), item.getSnapshotConfiguration());
                assertEquals(snapshotName.get(), item.getSnapshotName());
            });

            SnapshotLifecycleTask task = new SnapshotLifecycleTask(client, clusterService, historyStore);
            // Trigger the event with a matching job name for the policy
            task.triggered(
                new SchedulerEngine.Event(SnapshotLifecycleService.getJobId(slpm), System.currentTimeMillis(), System.currentTimeMillis())
            );

            assertBusy(() -> {
                assertTrue("snapshot should be triggered once", clientCalled.get());
                assertTrue("history store should be called once", historyStoreCalled.get());
            });
        } finally {
            threadPool.shutdownNow();
        }
    }

    public void testPartialFailureSnapshot() throws Exception {
        final String id = randomAlphaOfLength(4);
        final SnapshotLifecyclePolicyMetadata slpm = makePolicyMeta(id);
        final SnapshotLifecycleMetadata meta = new SnapshotLifecycleMetadata(
            Collections.singletonMap(id, slpm),
            OperationMode.RUNNING,
            new SnapshotLifecycleStats()
        );

        final ClusterState state = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(SnapshotLifecycleMetadata.TYPE, meta).build())
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.builder("nodeId").name("nodeId").build())
                    .localNodeId("nodeId")
                    .masterNodeId("nodeId")
            )
            .build();

        final ThreadPool threadPool = new TestThreadPool("test");
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(SLM_HISTORY_INDEX_ENABLED_SETTING))
        );
        final AtomicBoolean clientCalled = new AtomicBoolean(false);
        final SetOnce<String> snapshotName = new SetOnce<>();
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(state, threadPool, settings)) {
            VerifyingClient client = new VerifyingClient(threadPool, (action, request, listener) -> {
                assertFalse(clientCalled.getAndSet(true));
                assertThat(action, sameInstance(TransportCreateSnapshotAction.TYPE));
                assertThat(request, instanceOf(CreateSnapshotRequest.class));

                CreateSnapshotRequest req = (CreateSnapshotRequest) request;

                SnapshotLifecyclePolicy policy = slpm.getPolicy();
                assertThat(req.snapshot(), startsWith(policy.getName() + "-"));
                assertThat(req.repository(), equalTo(policy.getRepository()));
                snapshotName.set(req.snapshot());
                if (req.indices().length > 0) {
                    assertThat(Arrays.asList(req.indices()), equalTo(policy.getConfig().get("indices")));
                }
                boolean globalState = policy.getConfig().get("include_global_state") == null
                    || Boolean.parseBoolean((String) policy.getConfig().get("include_global_state"));
                assertThat(req.includeGlobalState(), equalTo(globalState));

                long startTime = randomNonNegativeLong();
                long endTime = randomLongBetween(startTime, Long.MAX_VALUE);
                return new CreateSnapshotResponse(
                    new SnapshotInfo(
                        new Snapshot(req.repository(), new SnapshotId(req.snapshot(), "uuid")),
                        Arrays.asList(req.indices()),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        "snapshot started",
                        endTime,
                        3,
                        Collections.singletonList(new SnapshotShardFailure("nodeId", new ShardId("index", "uuid", 0), "forced failure")),
                        req.includeGlobalState(),
                        req.userMetadata(),
                        startTime,
                        Collections.emptyMap()
                    )
                );
            });

            final AtomicBoolean historyStoreCalled = new AtomicBoolean(false);
            SnapshotHistoryStore historyStore = new VerifyingHistoryStore(null, clusterService, item -> {
                assertFalse(historyStoreCalled.getAndSet(true));
                final SnapshotLifecyclePolicy policy = slpm.getPolicy();
                assertEquals(policy.getId(), item.getPolicyId());
                assertEquals(policy.getRepository(), item.getRepository());
                assertEquals(policy.getConfig(), item.getSnapshotConfiguration());
                assertEquals(snapshotName.get(), item.getSnapshotName());
                assertFalse("item should be a failure", item.isSuccess());
                assertThat(
                    item.getErrorDetails(),
                    containsString("failed to create snapshot successfully, 1 out of 3 total shards failed")
                );
            });

            SnapshotLifecycleTask task = new SnapshotLifecycleTask(client, clusterService, historyStore);
            // Trigger the event with a matching job name for the policy
            task.triggered(
                new SchedulerEngine.Event(SnapshotLifecycleService.getJobId(slpm), System.currentTimeMillis(), System.currentTimeMillis())
            );

            assertBusy(() -> {
                assertTrue("snapshot should be triggered once", clientCalled.get());
                assertTrue("history store should be called once", historyStoreCalled.get());
            });
        } finally {
            threadPool.shutdownNow();
        }
    }

    public void testDeletedPoliciesHaveRegisteredRemoved() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnap = randSnapshotId();

        final String deletedPolicy = randomAlphaOfLength(10);
        final SnapshotId snapForDeletedPolicy = randSnapshotId();

        SnapshotLifecycleTask.WriteJobStatus writeJobStatus = randomBoolean()
            ? SnapshotLifecycleTask.WriteJobStatus.success(policyId, initiatingSnap, randomLong(), randomLong())
            : SnapshotLifecycleTask.WriteJobStatus.failure(policyId, initiatingSnap, randomLong(), new RuntimeException());

        // deletedPolicy is no longer defined
        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(initiatingSnap), deletedPolicy, List.of(snapForDeletedPolicy));
        // behavior is same whether initiatingSnap still in progress
        var inProgress = Map.of(policyId, randomBoolean() ? List.of(initiatingSnap) : List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(definedSlmPolicies, registeredSnapshots, inProgress);

        ClusterState newClusterState = writeJobStatus.execute(clusterState);
        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata().custom(RegisteredPolicySnapshots.TYPE);

        assertEquals(List.of(), newRegisteredPolicySnapshots.getSnapshots());
    }

    public void testOtherDefinedPoliciesUneffected() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnap = randSnapshotId();

        final String otherPolicy = randomAlphaOfLength(10);
        final SnapshotId otherSnapRunning = randSnapshotId();
        final SnapshotId otherSnapNotRunning = randSnapshotId();

        SnapshotLifecycleTask.WriteJobStatus writeJobStatus = randomBoolean()
            ? SnapshotLifecycleTask.WriteJobStatus.success(policyId, initiatingSnap, randomLong(), randomLong())
            : SnapshotLifecycleTask.WriteJobStatus.failure(policyId, initiatingSnap, randomLong(), new RuntimeException());

        var definedSlmPolicies = List.of(policyId, otherPolicy);
        var registeredSnapshots = Map.of(policyId, List.of(initiatingSnap), otherPolicy, List.of(otherSnapRunning, otherSnapNotRunning));
        var inProgress = Map.of(policyId, List.<SnapshotId>of(), otherPolicy, List.of(otherSnapRunning));
        ClusterState clusterState = buildClusterState(definedSlmPolicies, registeredSnapshots, inProgress);

        ClusterState newClusterState = writeJobStatus.execute(clusterState);
        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata().custom(RegisteredPolicySnapshots.TYPE);

        assertEquals(List.of(otherSnapRunning, otherSnapNotRunning), newRegisteredPolicySnapshots.getSnapshotsByPolicy(otherPolicy));
        assertEquals(List.of(), newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId));
    }

    public void testInitiatingSnapRemovedButStillRunningRemains() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnap = randSnapshotId();

        SnapshotLifecycleTask.WriteJobStatus writeJobStatus = randomBoolean()
            ? SnapshotLifecycleTask.WriteJobStatus.success(policyId, initiatingSnap, randomLong(), randomLong())
            : SnapshotLifecycleTask.WriteJobStatus.failure(policyId, initiatingSnap, randomLong(), new RuntimeException());

        final SnapshotId stillRunning = randSnapshotId();

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(stillRunning, initiatingSnap));
        // behavior is same whether initiatingSnap still in progress
        var inProgress = Map.of(policyId, randomBoolean() ? List.of(stillRunning, initiatingSnap) : List.of(stillRunning));
        ClusterState clusterState = buildClusterState(definedSlmPolicies, registeredSnapshots, inProgress);

        ClusterState newClusterState = writeJobStatus.execute(clusterState);
        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata().custom(RegisteredPolicySnapshots.TYPE);

        assertEquals(List.of(stillRunning), newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId));
    }

    public void testInferFailureInitiatedBySuccess() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnapshot = randSnapshotId();
        final SnapshotId previousFailedSnapshot = randSnapshotId();
        // currently running snapshots
        final SnapshotId stillRunning = randSnapshotId();

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(stillRunning, previousFailedSnapshot));
        var inProgress = Map.of(policyId, List.of(stillRunning));
        ClusterState clusterState = buildClusterState(definedSlmPolicies, registeredSnapshots, inProgress);

        var writeJobTask = SnapshotLifecycleTask.WriteJobStatus.success(policyId, initiatingSnapshot, randomLong(), randomLong());

        ClusterState newClusterState = writeJobTask.execute(clusterState);

        // previous failure is now recorded in stats and metadata
        SnapshotLifecycleMetadata newSlmMetadata = newClusterState.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats newStats = newSlmMetadata.getStats();
        SnapshotLifecycleStats.SnapshotPolicyStats snapshotPolicyStats = newStats.getMetrics().get(policyId);
        assertEquals(1, snapshotPolicyStats.getSnapshotFailedCount());
        assertEquals(1, snapshotPolicyStats.getSnapshotTakenCount());

        SnapshotLifecyclePolicyMetadata newSlmPolicyMetadata = newSlmMetadata.getSnapshotConfigurations().get(policyId);
        assertEquals(previousFailedSnapshot.getName(), newSlmPolicyMetadata.getLastFailure().getSnapshotName());
        assertEquals(initiatingSnapshot.getName(), newSlmPolicyMetadata.getLastSuccess().getSnapshotName());
        assertEquals(0, newSlmPolicyMetadata.getInvocationsSinceLastSuccess());

        // failed snapshot no longer in registeredSnapshot set
        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata().custom(RegisteredPolicySnapshots.TYPE);
        List<SnapshotId> newRegisteredSnapIds = newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId);
        assertEquals(List.of(stillRunning), newRegisteredSnapIds);
    }

    public void testInferFailureInitiatedByFailure() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnapshot = randSnapshotId();
        final SnapshotId previousFailedSnapshot = randSnapshotId();
        final SnapshotId stillRunning = randSnapshotId();

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(stillRunning, previousFailedSnapshot));
        var inProgress = Map.of(policyId, List.of(stillRunning));
        ClusterState clusterState = buildClusterState(definedSlmPolicies, registeredSnapshots, inProgress);

        var writeJobTask = SnapshotLifecycleTask.WriteJobStatus.failure(policyId, initiatingSnapshot, randomLong(), new RuntimeException());

        ClusterState newClusterState = writeJobTask.execute(clusterState);

        // previous failure is now recorded in stats and metadata
        SnapshotLifecycleMetadata newSlmMetadata = newClusterState.metadata().custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats newStats = newSlmMetadata.getStats();
        SnapshotLifecycleStats.SnapshotPolicyStats snapshotPolicyStats = newStats.getMetrics().get(policyId);
        assertEquals(2, snapshotPolicyStats.getSnapshotFailedCount());
        assertEquals(0, snapshotPolicyStats.getSnapshotTakenCount());

        SnapshotLifecyclePolicyMetadata newSlmPolicyMetadata = newSlmMetadata.getSnapshotConfigurations().get(policyId);
        assertEquals(initiatingSnapshot.getName(), newSlmPolicyMetadata.getLastFailure().getSnapshotName());
        assertNull(newSlmPolicyMetadata.getLastSuccess());
        assertEquals(2, newSlmPolicyMetadata.getInvocationsSinceLastSuccess());

        // failed snapshot no longer in registeredSnapshot set
        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata().custom(RegisteredPolicySnapshots.TYPE);
        List<SnapshotId> newRegisteredSnapIds = newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId);
        assertEquals(List.of(stillRunning), newRegisteredSnapIds);
    }

    public void testGetCurrentlyRunningSnapshots() {
        final SnapshotId snapshot1 = randSnapshotId();
        final SnapshotId snapshot2 = randSnapshotId();
        final SnapshotId snapshot3 = randSnapshotId();
        final SnapshotId snapshot4 = randSnapshotId();

        final String repo1 = randomAlphaOfLength(10);
        final String repo2 = randomAlphaOfLength(10);

        final var snapshotsInProgress = SnapshotsInProgress.EMPTY.withUpdatedEntriesForRepo(
            repo1,
            List.of(
                makeSnapshotInProgress(repo1, "some-policy", snapshot1),
                makeSnapshotInProgress(repo1, "some-policy", snapshot2),
                makeSnapshotInProgress(repo1, "other-policy", snapshot3)
            )
        ).withUpdatedEntriesForRepo(repo2, List.of(makeSnapshotInProgress(repo2, "other-policy", snapshot4)));

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress)
            .build();

        Set<SnapshotId> currentlyRunning = SnapshotLifecycleTask.currentlyRunningSnapshots(clusterState);
        assertEquals(currentlyRunning, Set.of(snapshot1, snapshot2, snapshot3, snapshot4));
    }

    private static SnapshotId randSnapshotId() {
        return new SnapshotId(randomAlphaOfLength(10), randomUUID());
    }

    private static ClusterState buildClusterState(
        List<String> slmPolicies,
        Map<String, List<SnapshotId>> registeredSnaps,
        Map<String, List<SnapshotId>> inProgress
    ) {
        final String repo = randomAlphaOfLength(10);
        List<SnapshotsInProgress.Entry> inProgressEntries = new ArrayList<>();
        for (String policy : inProgress.keySet()) {
            for (SnapshotId snap : inProgress.get(policy)) {
                inProgressEntries.add(makeSnapshotInProgress(repo, policy, snap));
            }
        }

        final List<RegisteredPolicySnapshots.PolicySnapshot> policySnapshots = new ArrayList<>();
        for (Map.Entry<String, List<SnapshotId>> policySnaps : registeredSnaps.entrySet()) {
            for (SnapshotId snapshotId : policySnaps.getValue()) {
                policySnapshots.add(new RegisteredPolicySnapshots.PolicySnapshot(policySnaps.getKey(), snapshotId));
            }
        }

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withUpdatedEntriesForRepo(repo, inProgressEntries))
            .metadata(
                Metadata.builder()
                    .putCustom(SnapshotLifecycleMetadata.TYPE, makeSnapMeta(slmPolicies))
                    .putCustom(RegisteredPolicySnapshots.TYPE, new RegisteredPolicySnapshots(policySnapshots))
            )
            .build();

        return clusterState;
    }

    private static SnapshotLifecycleMetadata makeSnapMeta(List<String> policies) {
        Map<String, SnapshotLifecyclePolicyMetadata> slmMeta = new HashMap<>();

        for (String policy : policies) {
            SnapshotLifecyclePolicyMetadata slmPolicyMeta = SnapshotLifecyclePolicyMetadata.builder()
                .setModifiedDate(randomLong())
                .setPolicy(new SnapshotLifecyclePolicy(policy, "snap", "", "repo-name", null, null))
                .build();
            slmMeta.put(policy, slmPolicyMeta);
        }

        SnapshotLifecycleStats stats = new SnapshotLifecycleStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            new HashMap<>()
        );
        return new SnapshotLifecycleMetadata(slmMeta, OperationMode.RUNNING, stats);
    }

    private static SnapshotsInProgress.Entry makeSnapshotInProgress(String repo, String policyId, SnapshotId snapshotId) {
        final Map<String, Object> metadata = Map.of(SnapshotsService.POLICY_ID_METADATA_FIELD, policyId);
        return SnapshotsInProgress.Entry.snapshot(
            new Snapshot(repo, snapshotId),
            randomBoolean(),
            randomBoolean(),
            SnapshotsInProgress.State.SUCCESS,
            Map.of(),
            List.of(),
            List.of(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            Map.of(),
            null,
            metadata,
            IndexVersion.current()
        );
    }

    /**
     * A client that delegates to a verifying function for action/request/listener
     */
    public static class VerifyingClient extends NoOpClient {

        private final TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier;

        VerifyingClient(ThreadPool threadPool, TriFunction<ActionType<?>, ActionRequest, ActionListener<?>, ActionResponse> verifier) {
            super(threadPool);
            this.verifier = verifier;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            listener.onResponse((Response) verifier.apply(action, request, listener));
        }
    }

    private SnapshotLifecyclePolicyMetadata makePolicyMeta(final String id) {
        SnapshotLifecyclePolicy policy = SnapshotLifecycleServiceTests.createPolicy(id);
        Map<String, String> headers = new HashMap<>();
        headers.put("X-Opaque-ID", randomAlphaOfLength(4));
        return SnapshotLifecyclePolicyMetadata.builder().setPolicy(policy).setHeaders(headers).setVersion(1).setModifiedDate(1).build();
    }

    public static class VerifyingHistoryStore extends SnapshotHistoryStore {

        private final Consumer<SnapshotHistoryItem> verifier;

        public VerifyingHistoryStore(Client client, ClusterService clusterService, Consumer<SnapshotHistoryItem> verifier) {
            super(client, clusterService);
            this.verifier = verifier;
        }

        @Override
        public void putAsync(SnapshotHistoryItem item) {
            verifier.accept(item);
        }
    }
}
