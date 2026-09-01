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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Booleans;
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
import java.util.HashSet;
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
    private final ProjectId projectId = randomProjectIdOrDefault();

    public void testGetSnapMetadata() {
        final String id = randomAlphaOfLength(4);
        final SnapshotLifecyclePolicyMetadata slpm = makePolicyMeta(id);
        final SnapshotLifecycleMetadata meta = new SnapshotLifecycleMetadata(
            Collections.singletonMap(id, slpm),
            OperationMode.RUNNING,
            new SnapshotLifecycleStats()
        );

        final ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId).putCustom(SnapshotLifecycleMetadata.TYPE, meta).build();
        final Optional<SnapshotLifecyclePolicyMetadata> o = SnapshotLifecycleTask.getSnapPolicyMetadata(
            projectMetadata,
            SnapshotLifecycleService.getJobId(slpm)
        );

        assertTrue("the policy metadata should be retrieved from the cluster state", o.isPresent());
        assertThat(o.get(), equalTo(slpm));

        assertFalse(SnapshotLifecycleTask.getSnapPolicyMetadata(projectMetadata, "bad-jobid").isPresent());
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
            .putProjectMetadata(ProjectMetadata.builder(projectId).putCustom(SnapshotLifecycleMetadata.TYPE, meta).build())
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

            SnapshotLifecycleTask task = new SnapshotLifecycleTask(projectId, client, clusterService, historyStore);

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
            .putProjectMetadata(ProjectMetadata.builder(projectId).putCustom(SnapshotLifecycleMetadata.TYPE, meta).build())
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
                    || Booleans.parseBoolean((String) policy.getConfig().get("include_global_state"));
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

            SnapshotLifecycleTask task = new SnapshotLifecycleTask(projectId, client, clusterService, historyStore);
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
            .putProjectMetadata(ProjectMetadata.builder(projectId).putCustom(SnapshotLifecycleMetadata.TYPE, meta).build())
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
                    || Booleans.parseBoolean((String) policy.getConfig().get("include_global_state"));
                assertThat(req.includeGlobalState(), equalTo(globalState));

                long startTime = randomNonNegativeLong();
                long endTime = randomLongBetween(startTime, Long.MAX_VALUE);
                return new CreateSnapshotResponse(
                    new SnapshotInfo(
                        new Snapshot(projectId, req.repository(), new SnapshotId(req.snapshot(), "uuid")),
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

            SnapshotLifecycleTask task = new SnapshotLifecycleTask(projectId, client, clusterService, historyStore);
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
            ? SnapshotLifecycleTask.WriteJobStatus.success(
                projectId,
                policyId,
                initiatingSnap,
                randomLong(),
                randomLong(),
                SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos.EMPTY
            )
            : SnapshotLifecycleTask.WriteJobStatus.failure(
                projectId,
                policyId,
                initiatingSnap,
                randomLong(),
                SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos.EMPTY,
                new RuntimeException(),
                randomBoolean()
            );

        // deletedPolicy is no longer defined
        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(initiatingSnap), deletedPolicy, List.of(snapForDeletedPolicy));
        // behavior is same whether initiatingSnap still in progress
        var inProgress = Map.of(policyId, randomBoolean() ? List.of(initiatingSnap) : List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);

        ClusterState newClusterState = writeJobStatus.execute(clusterState);
        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata()
            .getProject(projectId)
            .custom(RegisteredPolicySnapshots.TYPE);

        assertEquals(List.of(), newRegisteredPolicySnapshots.getSnapshots());
    }

    public void testOtherDefinedPoliciesUneffected() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnap = randSnapshotId();

        final String otherPolicy = randomAlphaOfLength(10);
        final SnapshotId otherSnapRunning = randSnapshotId();
        final SnapshotId otherSnapNotRunning = randSnapshotId();

        SnapshotLifecycleTask.WriteJobStatus writeJobStatus = randomBoolean()
            ? SnapshotLifecycleTask.WriteJobStatus.success(
                projectId,
                policyId,
                initiatingSnap,
                randomLong(),
                randomLong(),
                SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos.EMPTY
            )
            : SnapshotLifecycleTask.WriteJobStatus.failure(
                projectId,
                policyId,
                initiatingSnap,
                randomLong(),
                SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos.EMPTY,
                new RuntimeException(),
                randomBoolean()
            );

        var definedSlmPolicies = List.of(policyId, otherPolicy);
        var registeredSnapshots = Map.of(policyId, List.of(initiatingSnap), otherPolicy, List.of(otherSnapRunning, otherSnapNotRunning));
        var inProgress = Map.of(policyId, List.<SnapshotId>of(), otherPolicy, List.of(otherSnapRunning));
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);

        ClusterState newClusterState = writeJobStatus.execute(clusterState);
        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata()
            .getProject(projectId)
            .custom(RegisteredPolicySnapshots.TYPE);

        assertEquals(List.of(otherSnapRunning, otherSnapNotRunning), newRegisteredPolicySnapshots.getSnapshotsByPolicy(otherPolicy));
        assertEquals(List.of(), newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId));
    }

    public void testInitiatingSnapRemovedButStillRunningRemains() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnap = randSnapshotId();

        SnapshotLifecycleTask.WriteJobStatus writeJobStatus = randomBoolean()
            ? SnapshotLifecycleTask.WriteJobStatus.success(
                projectId,
                policyId,
                initiatingSnap,
                randomLong(),
                randomLong(),
                SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos.EMPTY
            )
            : SnapshotLifecycleTask.WriteJobStatus.failure(
                projectId,
                policyId,
                initiatingSnap,
                randomLong(),
                SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos.EMPTY,
                new RuntimeException(),
                randomBoolean()
            );

        final SnapshotId stillRunning = randSnapshotId();

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(stillRunning, initiatingSnap));
        // behavior is same whether initiatingSnap still in progress
        var inProgress = Map.of(policyId, randomBoolean() ? List.of(stillRunning, initiatingSnap) : List.of(stillRunning));
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);

        ClusterState newClusterState = writeJobStatus.execute(clusterState);
        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata()
            .getProject(projectId)
            .custom(RegisteredPolicySnapshots.TYPE);

        assertEquals(List.of(stillRunning), newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId));
    }

    public void testCleanUpRegisteredInitiatedBySuccess() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnapshot = randSnapshotId();
        final SnapshotId inferredFailureSnapshot = randSnapshotId();
        // currently running snapshots
        final SnapshotId stillRunning = randSnapshotId();

        final SnapshotInfo snapshotInfoSuccess = randomSnapshotInfoSuccess(projectId);
        final SnapshotInfo snapshotInfoFailure = randomSnapshotInfoFailure(projectId);

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(
            policyId,
            List.of(
                stillRunning,
                inferredFailureSnapshot,
                snapshotInfoSuccess.snapshotId(),
                snapshotInfoFailure.snapshotId(),
                initiatingSnapshot
            )
        );
        var inProgress = Map.of(policyId, List.of(stillRunning));
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);

        var writeJobTask = SnapshotLifecycleTask.WriteJobStatus.success(
            projectId,
            policyId,
            initiatingSnapshot,
            randomLong(),
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(
                Set.of(inferredFailureSnapshot, snapshotInfoSuccess.snapshotId(), snapshotInfoFailure.snapshotId()),
                List.of(snapshotInfoSuccess, snapshotInfoFailure)
            )
        );

        ClusterState newClusterState = writeJobTask.execute(clusterState);

        // snapshotInfoSuccess, initiatingSnapshot
        int expectedSuccessCount = 2;
        // inferredFailureSnapshot, snapshotInfoFailure
        int expectedFailureCount = 2;
        // the last snapshot (initiatingSnapshot) was successful
        int expectedInvocationsSinceLastSuccess = 0;
        // registered snapshots state is now recorded in stats and metadata
        SnapshotLifecycleMetadata newSlmMetadata = newClusterState.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats newStats = newSlmMetadata.getStats();
        SnapshotLifecycleStats.SnapshotPolicyStats snapshotPolicyStats = newStats.getMetrics().get(policyId);
        assertEquals(expectedFailureCount, snapshotPolicyStats.getSnapshotFailedCount());
        assertEquals(expectedSuccessCount, snapshotPolicyStats.getSnapshotTakenCount());

        SnapshotLifecyclePolicyMetadata newSlmPolicyMetadata = newSlmMetadata.getSnapshotConfigurations().get(policyId);
        assertEquals(snapshotInfoFailure.snapshotId().getName(), newSlmPolicyMetadata.getLastFailure().getSnapshotName());
        assertEquals(initiatingSnapshot.getName(), newSlmPolicyMetadata.getLastSuccess().getSnapshotName());
        assertEquals(expectedInvocationsSinceLastSuccess, newSlmPolicyMetadata.getInvocationsSinceLastSuccess());

        // completed snapshot no longer in registeredSnapshot set
        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata()
            .getProject(projectId)
            .custom(RegisteredPolicySnapshots.TYPE);
        List<SnapshotId> newRegisteredSnapIds = newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId);
        assertEquals(List.of(stillRunning), newRegisteredSnapIds);
    }

    public void testCleanUpRegisteredInitiatedByFailure() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnapshot = randSnapshotId();
        final SnapshotId inferredFailureSnapshot = randSnapshotId();
        final SnapshotId stillRunning = randSnapshotId();
        final SnapshotInfo snapshotInfoSuccess = randomSnapshotInfoSuccess(projectId);
        final SnapshotInfo snapshotInfoFailure1 = randomSnapshotInfoFailure(projectId);
        final SnapshotInfo snapshotInfoFailure2 = randomSnapshotInfoFailure(projectId);

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(
            policyId,
            List.of(
                stillRunning,
                inferredFailureSnapshot,
                snapshotInfoSuccess.snapshotId(),
                snapshotInfoFailure1.snapshotId(),
                snapshotInfoFailure2.snapshotId(),
                initiatingSnapshot
            )
        );
        var inProgress = Map.of(policyId, List.of(stillRunning));
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);

        var writeJobTask = SnapshotLifecycleTask.WriteJobStatus.failure(
            projectId,
            policyId,
            initiatingSnapshot,
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(
                Set.of(
                    inferredFailureSnapshot,
                    snapshotInfoSuccess.snapshotId(),
                    snapshotInfoFailure1.snapshotId(),
                    snapshotInfoFailure2.snapshotId()
                ),
                List.of(snapshotInfoSuccess, snapshotInfoFailure1, snapshotInfoFailure2)
            ),
            new RuntimeException(),
            // initiating snapshot is still registered, so this flag must not affect stats
            randomBoolean()
        );

        ClusterState newClusterState = writeJobTask.execute(clusterState);

        // snapshotInfoSuccess
        int expectedSuccessCount = 1;
        // inferredFailureSnapshot, snapshotInfoFailure1, snapshotInfoFailure2, initiatingSnapshot
        int expectedFailureCount = 4;
        // snapshotInfoFailure1, snapshotInfoFailure2, initiatingSnapshot
        int expectedInvocationsSinceLastSuccess = 3;
        // registered snapshots state is now recorded in stats and metadata
        SnapshotLifecycleMetadata newSlmMetadata = newClusterState.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats newStats = newSlmMetadata.getStats();
        SnapshotLifecycleStats.SnapshotPolicyStats snapshotPolicyStats = newStats.getMetrics().get(policyId);
        assertEquals(expectedFailureCount, snapshotPolicyStats.getSnapshotFailedCount());
        assertEquals(expectedSuccessCount, snapshotPolicyStats.getSnapshotTakenCount());

        SnapshotLifecyclePolicyMetadata newSlmPolicyMetadata = newSlmMetadata.getSnapshotConfigurations().get(policyId);
        assertEquals(initiatingSnapshot.getName(), newSlmPolicyMetadata.getLastFailure().getSnapshotName());
        assertEquals(snapshotInfoSuccess.snapshotId().getName(), newSlmPolicyMetadata.getLastSuccess().getSnapshotName());
        assertEquals(expectedInvocationsSinceLastSuccess, newSlmPolicyMetadata.getInvocationsSinceLastSuccess());

        // completed snapshot no longer in registeredSnapshot set
        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata()
            .getProject(projectId)
            .custom(RegisteredPolicySnapshots.TYPE);
        List<SnapshotId> newRegisteredSnapIds = newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId);
        assertEquals(List.of(stillRunning), newRegisteredSnapIds);
    }

    /**
     * Reproduces the race from #155621: snapshot B was still running when snapshot A's cleanup looked up completed registered
     * snapshots (so B's SnapshotInfo was never fetched), but B finished before A's WriteJobStatus cluster-state update ran.
     * B must stay registered rather than being inferred as a failure.
     */
    public void testDoesNotInferFailureForSnapshotThatFinishedAfterLookup() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId snapshotA = randSnapshotId();
        final SnapshotId snapshotB = randSnapshotId();

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(snapshotA, snapshotB));
        // B has finished by the time WriteJobStatus runs (not in SnapshotsInProgress)
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        // Lookup saw A as already completed (queried) while B was still running (not queried). No SnapshotInfo for B.
        var writeJobTask = SnapshotLifecycleTask.WriteJobStatus.success(
            projectId,
            policyId,
            snapshotA,
            randomLong(),
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(Set.of(snapshotA), List.of())
        );

        ClusterState newClusterState = writeJobTask.execute(clusterState);

        SnapshotLifecycleMetadata newSlmMetadata = newClusterState.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats.SnapshotPolicyStats snapshotPolicyStats = newSlmMetadata.getStats().getMetrics().get(policyId);
        assertEquals(1, snapshotPolicyStats.getSnapshotTakenCount());
        assertEquals(0, snapshotPolicyStats.getSnapshotFailedCount());
        assertEquals(snapshotA.getName(), newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastSuccess().getSnapshotName());
        assertNull(newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastFailure());
        assertEquals(0, newSlmMetadata.getSnapshotConfigurations().get(policyId).getInvocationsSinceLastSuccess());
        assertStatsNotMutated(statsBefore);

        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata()
            .getProject(projectId)
            .custom(RegisteredPolicySnapshots.TYPE);
        assertEquals(List.of(snapshotB), newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId));
    }

    /**
     * Same TOCTOU as {@link #testDoesNotInferFailureForSnapshotThatFinishedAfterLookup}, but the initiating WriteJobStatus is a
     * failure: snapshot B finished after lookup and must remain registered rather than being inferred as another failure.
     */
    public void testDoesNotInferFailureForSnapshotThatFinishedAfterLookupOnFailurePath() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId snapshotA = randSnapshotId();
        final SnapshotId snapshotB = randSnapshotId();

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(snapshotA, snapshotB));
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        ClusterState newClusterState = SnapshotLifecycleTask.WriteJobStatus.failure(
            projectId,
            policyId,
            snapshotA,
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(Set.of(snapshotA), List.of()),
            new RuntimeException("initiating snapshot failed"),
            // initiating snapshot was queried as registered, so it cannot also be flagged never-registered
            false
        ).execute(clusterState);

        SnapshotLifecycleMetadata newSlmMetadata = newClusterState.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats.SnapshotPolicyStats snapshotPolicyStats = newSlmMetadata.getStats().getMetrics().get(policyId);
        assertEquals(0, snapshotPolicyStats.getSnapshotTakenCount());
        assertEquals(1, snapshotPolicyStats.getSnapshotFailedCount());
        assertEquals(snapshotA.getName(), newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastFailure().getSnapshotName());
        assertNull(newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastSuccess());
        assertEquals(1, newSlmMetadata.getSnapshotConfigurations().get(policyId).getInvocationsSinceLastSuccess());
        assertStatsNotMutated(statsBefore);

        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata()
            .getProject(projectId)
            .custom(RegisteredPolicySnapshots.TYPE);
        assertEquals(List.of(snapshotB), newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId));
    }

    /**
     * Two snapshots that finish around the same time can each discover the other as completed and fetch SnapshotInfo.
     * Whichever WriteJobStatus runs first records both outcomes and clears the registered set; the second must not
     * double-count stats or overwrite last success/failure, even when it still carries stale SnapshotInfo for the peer.
     */
    public void testDoesNotDoubleCountWhenAnotherCleanupAlreadyRecordedSnapshot() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId snapshotA = randSnapshotId();
        final SnapshotId snapshotB = randSnapshotId();
        final SnapshotInfo snapshotInfoA = snapshotInfoSuccess(projectId, snapshotA);
        final SnapshotInfo snapshotInfoB = snapshotInfoSuccess(projectId, snapshotB);

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(snapshotA, snapshotB));
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        // A's cleanup discovers B already completed and records B from SnapshotInfo, then records A's own success
        ClusterState afterA = SnapshotLifecycleTask.WriteJobStatus.success(
            projectId,
            policyId,
            snapshotA,
            randomLong(),
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(Set.of(snapshotB), List.of(snapshotInfoB))
        ).execute(clusterState);

        SnapshotLifecycleMetadata slmAfterA = afterA.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats.SnapshotPolicyStats statsAfterA = slmAfterA.getStats().getMetrics().get(policyId);
        assertEquals(2, statsAfterA.getSnapshotTakenCount());
        assertEquals(0, statsAfterA.getSnapshotFailedCount());
        assertEquals(snapshotA.getName(), slmAfterA.getSnapshotConfigurations().get(policyId).getLastSuccess().getSnapshotName());
        assertEquals(0, slmAfterA.getSnapshotConfigurations().get(policyId).getInvocationsSinceLastSuccess());
        RegisteredPolicySnapshots registeredAfterA = afterA.metadata().getProject(projectId).custom(RegisteredPolicySnapshots.TYPE);
        assertEquals(List.of(), registeredAfterA.getSnapshotsByPolicy(policyId));
        SnapshotLifecycleStats statsAfterASource = slmAfterA.getStats();
        final long takenAfterA = statsAfterA.getSnapshotTakenCount();
        final long failedAfterA = statsAfterA.getSnapshotFailedCount();

        // B also discovered A at lookup time; A is already gone from the registered set so this must be a no-op for stats
        ClusterState afterB = SnapshotLifecycleTask.WriteJobStatus.success(
            projectId,
            policyId,
            snapshotB,
            randomLong(),
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(Set.of(snapshotA, snapshotB), List.of(snapshotInfoA))
        ).execute(afterA);

        SnapshotLifecycleMetadata slmAfterB = afterB.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats.SnapshotPolicyStats statsAfterB = slmAfterB.getStats().getMetrics().get(policyId);
        assertEquals(2, statsAfterB.getSnapshotTakenCount());
        assertEquals(0, statsAfterB.getSnapshotFailedCount());
        assertEquals(snapshotA.getName(), slmAfterB.getSnapshotConfigurations().get(policyId).getLastSuccess().getSnapshotName());
        assertEquals(0, slmAfterB.getSnapshotConfigurations().get(policyId).getInvocationsSinceLastSuccess());
        assertStatsNotMutated(statsBefore);
        assertStatsNotMutated(statsAfterASource, policyId, takenAfterA, failedAfterA);
    }

    /**
     * CreateSnapshot can fail before the snapshot is added to the registered set (e.g. missing index). Failure stats
     * must still be recorded so SLM does not appear stuck with empty policy metrics, including
     * invocationsSinceLastSuccess.
     */
    public void testRecordsFailureEvenWhenSnapshotNeverRegistered() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnapshot = randSnapshotId();

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.<SnapshotId>of());
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        ClusterState newClusterState = SnapshotLifecycleTask.WriteJobStatus.failure(
            projectId,
            policyId,
            initiatingSnapshot,
            randomLong(),
            SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos.EMPTY,
            new RuntimeException("no such index"),
            true
        ).execute(clusterState);

        SnapshotLifecycleMetadata newSlmMetadata = newClusterState.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats.SnapshotPolicyStats snapshotPolicyStats = newSlmMetadata.getStats().getMetrics().get(policyId);
        assertEquals(0, snapshotPolicyStats.getSnapshotTakenCount());
        assertEquals(1, snapshotPolicyStats.getSnapshotFailedCount());
        assertEquals(
            initiatingSnapshot.getName(),
            newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastFailure().getSnapshotName()
        );
        assertEquals(1, newSlmMetadata.getSnapshotConfigurations().get(policyId).getInvocationsSinceLastSuccess());
        assertStatsNotMutated(statsBefore);
    }

    /**
     * Like {@link #testDoesNotDoubleCountWhenAnotherCleanupAlreadyRecordedSnapshot}, but for failures: A's cleanup can record B
     * from SnapshotInfo as a failure and clear the registered set; B's WriteJobStatus.failure must not double-count even when
     * it still carries stale SnapshotInfo for A (concurrent lookup) and is no longer in the registered set. Pass
     * {@code recordFailureIfUnregistered=false} because the snapshot was registered — a peer cleanup removing it must not be
     * confused with a never-registered CreateSnapshot failure (#136759).
     */
    public void testDoesNotDoubleCountFailureWhenAnotherCleanupAlreadyRecordedSnapshot() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId snapshotA = randSnapshotId();
        final SnapshotId snapshotB = randSnapshotId();
        final SnapshotInfo snapshotInfoA = snapshotInfoSuccess(projectId, snapshotA);
        final SnapshotInfo snapshotInfoB = snapshotInfoFailure(projectId, snapshotB);

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(snapshotA, snapshotB));
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        ClusterState afterA = SnapshotLifecycleTask.WriteJobStatus.success(
            projectId,
            policyId,
            snapshotA,
            randomLong(),
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(Set.of(snapshotB), List.of(snapshotInfoB))
        ).execute(clusterState);

        SnapshotLifecycleMetadata slmAfterA = afterA.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats.SnapshotPolicyStats statsAfterA = slmAfterA.getStats().getMetrics().get(policyId);
        assertEquals(1, statsAfterA.getSnapshotTakenCount());
        assertEquals(1, statsAfterA.getSnapshotFailedCount());
        assertEquals(snapshotA.getName(), slmAfterA.getSnapshotConfigurations().get(policyId).getLastSuccess().getSnapshotName());
        assertEquals(snapshotB.getName(), slmAfterA.getSnapshotConfigurations().get(policyId).getLastFailure().getSnapshotName());
        // A's success resets invocations even though B's failure was recorded in the same cleanup
        assertEquals(0, slmAfterA.getSnapshotConfigurations().get(policyId).getInvocationsSinceLastSuccess());
        assertTrue(policyAlreadyRecordsSnapshot(slmAfterA.getSnapshotConfigurations().get(policyId), snapshotB.getName()));
        SnapshotLifecycleStats statsAfterASource = slmAfterA.getStats();
        final long takenAfterA = statsAfterA.getSnapshotTakenCount();
        final long failedAfterA = statsAfterA.getSnapshotFailedCount();

        ClusterState afterB = SnapshotLifecycleTask.WriteJobStatus.failure(
            projectId,
            policyId,
            snapshotB,
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(Set.of(snapshotA), List.of(snapshotInfoA)),
            new RuntimeException("failed to create snapshot successfully"),
            false
        ).execute(afterA);

        SnapshotLifecycleMetadata slmAfterB = afterB.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats.SnapshotPolicyStats statsAfterB = slmAfterB.getStats().getMetrics().get(policyId);
        assertEquals(1, statsAfterB.getSnapshotTakenCount());
        assertEquals(1, statsAfterB.getSnapshotFailedCount());
        assertEquals(snapshotB.getName(), slmAfterB.getSnapshotConfigurations().get(policyId).getLastFailure().getSnapshotName());
        assertEquals(snapshotA.getName(), slmAfterB.getSnapshotConfigurations().get(policyId).getLastSuccess().getSnapshotName());
        assertEquals(0, slmAfterB.getSnapshotConfigurations().get(policyId).getInvocationsSinceLastSuccess());
        assertStatsNotMutated(statsBefore);
        assertStatsNotMutated(statsAfterASource, policyId, takenAfterA, failedAfterA);
    }

    /**
     * Companion to {@link #testDoesNotDoubleCountFailureWhenAnotherCleanupAlreadyRecordedSnapshot} with the initiating snapshot
     * also failing. A's cleanup records B's failure and then A's own failure record overwrites {@code lastFailure}, so when B's
     * WriteJobStatus runs the policy metadata no longer names B and B's outcome must not be recorded a second time (#136759).
     */
    public void testDoesNotDoubleCountWhenBothConcurrentSnapshotsFailed() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId snapshotA = randSnapshotId();
        final SnapshotId snapshotB = randSnapshotId();

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(snapshotA, snapshotB));
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        // A's cleanup discovers B already completed and failed, records it, then records A's own failure
        ClusterState afterA = SnapshotLifecycleTask.WriteJobStatus.failure(
            projectId,
            policyId,
            snapshotA,
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(
                Set.of(snapshotB),
                List.of(snapshotInfoFailure(projectId, snapshotB))
            ),
            new RuntimeException("snapshot A failed"),
            false
        ).execute(clusterState);

        SnapshotLifecycleMetadata slmAfterA = afterA.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecyclePolicyMetadata policyAfterA = slmAfterA.getSnapshotConfigurations().get(policyId);
        SnapshotLifecycleStats statsAfterASource = slmAfterA.getStats();
        final long takenAfterA = statsAfterASource.getMetrics().get(policyId).getSnapshotTakenCount();
        final long failedAfterA = statsAfterASource.getMetrics().get(policyId).getSnapshotFailedCount();
        assertEquals(0, takenAfterA);
        assertEquals(2, failedAfterA);
        assertEquals(2, policyAfterA.getInvocationsSinceLastSuccess());
        assertEquals(
            List.of(),
            ((RegisteredPolicySnapshots) afterA.metadata().getProject(projectId).custom(RegisteredPolicySnapshots.TYPE))
                .getSnapshotsByPolicy(policyId)
        );

        // A's own failure record overwrote the one A's cleanup wrote for B, so the policy no longer names B.
        // last success/failure cannot be used to skip already-recorded outcomes (#136759).
        assertEquals(snapshotA.getName(), policyAfterA.getLastFailure().getSnapshotName());
        assertFalse(policyAlreadyRecordsSnapshot(policyAfterA, snapshotB.getName()));

        // B also discovered A at lookup time; B was already recorded by A's cleanup and must not be counted again
        ClusterState afterB = SnapshotLifecycleTask.WriteJobStatus.failure(
            projectId,
            policyId,
            snapshotB,
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(
                Set.of(snapshotA),
                List.of(snapshotInfoFailure(projectId, snapshotA))
            ),
            new RuntimeException("snapshot B failed"),
            false
        ).execute(afterA);

        SnapshotLifecycleMetadata slmAfterB = afterB.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecyclePolicyMetadata policyAfterB = slmAfterB.getSnapshotConfigurations().get(policyId);
        assertEquals(0, slmAfterB.getStats().getMetrics().get(policyId).getSnapshotTakenCount());
        assertEquals(2, slmAfterB.getStats().getMetrics().get(policyId).getSnapshotFailedCount());
        assertEquals(2, policyAfterB.getInvocationsSinceLastSuccess());
        assertEquals(snapshotA.getName(), policyAfterB.getLastFailure().getSnapshotName());
        assertEquals(2, policyAfterA.getInvocationsSinceLastSuccess());
        assertEquals(snapshotA.getName(), policyAfterA.getLastFailure().getSnapshotName());
        assertEquals(
            List.of(),
            ((RegisteredPolicySnapshots) afterB.metadata().getProject(projectId).custom(RegisteredPolicySnapshots.TYPE))
                .getSnapshotsByPolicy(policyId)
        );
        assertStatsNotMutated(statsBefore);
        assertStatsNotMutated(statsAfterASource, policyId, takenAfterA, failedAfterA);
    }

    /**
     * The scenario from #136759: three snapshots of the same policy fail at virtually the same time. The first cleanup run
     * records all three, so the two later runs must not increment {@code invocationsSinceLastSuccess} or the failed count
     * again - otherwise three failures are reported as more invocations and the SLM health indicator can turn yellow
     * prematurely. Later runs still carry stale SnapshotInfo from concurrent lookup.
     */
    public void testThreeConcurrentFailuresCountedOnce() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId snapshotA = randSnapshotId();
        final SnapshotId snapshotB = randSnapshotId();
        final SnapshotId snapshotC = randSnapshotId();
        final List<SnapshotId> allSnapshots = List.of(snapshotA, snapshotB, snapshotC);
        final SnapshotId first = randomFrom(allSnapshots);
        final List<SnapshotId> others = new ArrayList<>(allSnapshots);
        others.remove(first);

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, allSnapshots);
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        // First cleanup sees the other two already completed and failed, records both, then records its own failure
        ClusterState state = SnapshotLifecycleTask.WriteJobStatus.failure(
            projectId,
            policyId,
            first,
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(
                Set.copyOf(allSnapshots),
                others.stream().map(id -> snapshotInfoFailure(projectId, id)).toList()
            ),
            new RuntimeException("snapshot failed"),
            false
        ).execute(clusterState);

        SnapshotLifecycleMetadata slmAfterFirst = state.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecyclePolicyMetadata policyAfterFirst = slmAfterFirst.getSnapshotConfigurations().get(policyId);
        SnapshotLifecycleStats statsAfterFirst = slmAfterFirst.getStats();
        final long takenAfterFirst = statsAfterFirst.getMetrics().get(policyId).getSnapshotTakenCount();
        final long failedAfterFirst = statsAfterFirst.getMetrics().get(policyId).getSnapshotFailedCount();
        assertEquals(0, takenAfterFirst);
        assertEquals(3, failedAfterFirst);
        assertEquals(3, policyAfterFirst.getInvocationsSinceLastSuccess());
        assertEquals(first.getName(), policyAfterFirst.getLastFailure().getSnapshotName());
        assertEquals(
            List.of(),
            ((RegisteredPolicySnapshots) state.metadata().getProject(projectId).custom(RegisteredPolicySnapshots.TYPE))
                .getSnapshotsByPolicy(policyId)
        );
        for (SnapshotId other : others) {
            assertFalse(policyAlreadyRecordsSnapshot(policyAfterFirst, other.getName()));
        }

        // Later listeners still carry stale SnapshotInfo for their peers from concurrent lookup
        for (SnapshotId snapshotId : shuffledList(others)) {
            List<SnapshotId> peers = new ArrayList<>(allSnapshots);
            peers.remove(snapshotId);
            state = SnapshotLifecycleTask.WriteJobStatus.failure(
                projectId,
                policyId,
                snapshotId,
                randomLong(),
                new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(
                    Set.copyOf(allSnapshots),
                    peers.stream().map(id -> snapshotInfoFailure(projectId, id)).toList()
                ),
                new RuntimeException("snapshot failed"),
                false
            ).execute(state);
        }

        SnapshotLifecycleMetadata slmMetadata = state.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecyclePolicyMetadata policyMetadata = slmMetadata.getSnapshotConfigurations().get(policyId);
        assertEquals(0, slmMetadata.getStats().getMetrics().get(policyId).getSnapshotTakenCount());
        assertEquals(3, slmMetadata.getStats().getMetrics().get(policyId).getSnapshotFailedCount());
        assertEquals(3, policyMetadata.getInvocationsSinceLastSuccess());
        assertEquals(first.getName(), policyMetadata.getLastFailure().getSnapshotName());
        assertEquals(3, policyAfterFirst.getInvocationsSinceLastSuccess());
        assertEquals(first.getName(), policyAfterFirst.getLastFailure().getSnapshotName());
        assertEquals(
            List.of(),
            ((RegisteredPolicySnapshots) state.metadata().getProject(projectId).custom(RegisteredPolicySnapshots.TYPE))
                .getSnapshotsByPolicy(policyId)
        );
        assertStatsNotMutated(statsBefore);
        assertStatsNotMutated(statsAfterFirst, policyId, takenAfterFirst, failedAfterFirst);
    }

    /**
     * Combines registered-set cleanup with the #155621 TOCTOU: a queried snapshot missing SnapshotInfo is inferred as a
     * failure, while a snapshot that was still running at lookup (not queried) but finished before WriteJobStatus stays
     * registered rather than being inferred.
     */
    public void testInfersFailureOnlyForQueriedSnapshotsWhenAnotherFinishedAfterLookup() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnapshot = randSnapshotId();
        final SnapshotId inferredFailureSnapshot = randSnapshotId();
        final SnapshotId finishedAfterLookup = randSnapshotId();

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(finishedAfterLookup, inferredFailureSnapshot, initiatingSnapshot));
        // finishedAfterLookup is no longer in SnapshotsInProgress, but was not queried (still running at lookup)
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        ClusterState newClusterState = SnapshotLifecycleTask.WriteJobStatus.success(
            projectId,
            policyId,
            initiatingSnapshot,
            randomLong(),
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(Set.of(initiatingSnapshot, inferredFailureSnapshot), List.of())
        ).execute(clusterState);

        SnapshotLifecycleMetadata newSlmMetadata = newClusterState.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats.SnapshotPolicyStats snapshotPolicyStats = newSlmMetadata.getStats().getMetrics().get(policyId);
        assertEquals(1, snapshotPolicyStats.getSnapshotTakenCount());
        assertEquals(1, snapshotPolicyStats.getSnapshotFailedCount());
        assertEquals(
            initiatingSnapshot.getName(),
            newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastSuccess().getSnapshotName()
        );
        assertEquals(
            inferredFailureSnapshot.getName(),
            newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastFailure().getSnapshotName()
        );
        assertEquals(0, newSlmMetadata.getSnapshotConfigurations().get(policyId).getInvocationsSinceLastSuccess());
        assertStatsNotMutated(statsBefore);

        RegisteredPolicySnapshots newRegisteredPolicySnapshots = newClusterState.metadata()
            .getProject(projectId)
            .custom(RegisteredPolicySnapshots.TYPE);
        assertEquals(List.of(finishedAfterLookup), newRegisteredPolicySnapshots.getSnapshotsByPolicy(policyId));
    }

    /**
     * CreateSnapshot can fail before registration while other registered snapshots still need cleanup. Own failure stats
     * are recorded ({@code recordFailureIfUnregistered=true}) and peer SnapshotInfo is still applied.
     */
    public void testNeverRegisteredFailureStillCleansUpOtherRegisteredSnapshots() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnapshot = randSnapshotId();
        final SnapshotId staleSuccess = randSnapshotId();
        final SnapshotInfo staleSuccessInfo = snapshotInfoSuccess(projectId, staleSuccess);

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(staleSuccess));
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        ClusterState newClusterState = SnapshotLifecycleTask.WriteJobStatus.failure(
            projectId,
            policyId,
            initiatingSnapshot,
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(Set.of(staleSuccess), List.of(staleSuccessInfo)),
            new RuntimeException("no such index"),
            true
        ).execute(clusterState);

        SnapshotLifecycleMetadata newSlmMetadata = newClusterState.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats.SnapshotPolicyStats snapshotPolicyStats = newSlmMetadata.getStats().getMetrics().get(policyId);
        assertEquals(1, snapshotPolicyStats.getSnapshotTakenCount());
        assertEquals(1, snapshotPolicyStats.getSnapshotFailedCount());
        assertEquals(staleSuccess.getName(), newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastSuccess().getSnapshotName());
        assertEquals(
            initiatingSnapshot.getName(),
            newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastFailure().getSnapshotName()
        );
        assertEquals(1, newSlmMetadata.getSnapshotConfigurations().get(policyId).getInvocationsSinceLastSuccess());
        assertEquals(
            List.of(),
            ((RegisteredPolicySnapshots) newClusterState.metadata().getProject(projectId).custom(RegisteredPolicySnapshots.TYPE))
                .getSnapshotsByPolicy(policyId)
        );
        assertStatsNotMutated(statsBefore);
    }

    /**
     * GetSnapshots can fail independently of CreateSnapshot. A never-registered initiating failure must still be
     * recorded, but other registered snapshots that were not queried must stay registered rather than being inferred
     * as failures (same TOCTOU rule as #155621).
     */
    public void testNeverRegisteredFailureWithLookupFailureLeavesOtherSnapshotsRegistered() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId initiatingSnapshot = randSnapshotId();
        final SnapshotId staleRegistered = randSnapshotId();

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, List.of(staleRegistered));
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        ClusterState newClusterState = SnapshotLifecycleTask.WriteJobStatus.failure(
            projectId,
            policyId,
            initiatingSnapshot,
            randomLong(),
            SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos.EMPTY,
            new RuntimeException("no such index"),
            true
        ).execute(clusterState);

        SnapshotLifecycleMetadata newSlmMetadata = newClusterState.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecycleStats.SnapshotPolicyStats snapshotPolicyStats = newSlmMetadata.getStats().getMetrics().get(policyId);
        assertEquals(0, snapshotPolicyStats.getSnapshotTakenCount());
        assertEquals(1, snapshotPolicyStats.getSnapshotFailedCount());
        assertEquals(
            initiatingSnapshot.getName(),
            newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastFailure().getSnapshotName()
        );
        assertNull(newSlmMetadata.getSnapshotConfigurations().get(policyId).getLastSuccess());
        assertEquals(1, newSlmMetadata.getSnapshotConfigurations().get(policyId).getInvocationsSinceLastSuccess());
        assertEquals(
            List.of(staleRegistered),
            ((RegisteredPolicySnapshots) newClusterState.metadata().getProject(projectId).custom(RegisteredPolicySnapshots.TYPE))
                .getSnapshotsByPolicy(policyId)
        );
        assertStatsNotMutated(statsBefore);
    }

    /**
     * Success-path counterpart of {@link #testThreeConcurrentFailuresCountedOnce} (#136759): the first cleanup
     * records all three successes; later runs must not increment the taken count or overwrite last success from stale
     * SnapshotInfo.
     */
    public void testThreeConcurrentSuccessesCountedOnce() throws Exception {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId snapshotA = randSnapshotId();
        final SnapshotId snapshotB = randSnapshotId();
        final SnapshotId snapshotC = randSnapshotId();
        final List<SnapshotId> allSnapshots = List.of(snapshotA, snapshotB, snapshotC);
        final SnapshotId first = randomFrom(allSnapshots);
        final List<SnapshotId> others = new ArrayList<>(allSnapshots);
        others.remove(first);

        var definedSlmPolicies = List.of(policyId);
        var registeredSnapshots = Map.of(policyId, allSnapshots);
        var inProgress = Map.of(policyId, List.<SnapshotId>of());
        ClusterState clusterState = buildClusterState(projectId, definedSlmPolicies, registeredSnapshots, inProgress);
        SnapshotLifecycleStats statsBefore = slmStats(clusterState);

        ClusterState state = SnapshotLifecycleTask.WriteJobStatus.success(
            projectId,
            policyId,
            first,
            randomLong(),
            randomLong(),
            new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(
                Set.copyOf(allSnapshots),
                others.stream().map(id -> snapshotInfoSuccess(projectId, id)).toList()
            )
        ).execute(clusterState);

        SnapshotLifecycleMetadata slmAfterFirst = state.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecyclePolicyMetadata policyAfterFirst = slmAfterFirst.getSnapshotConfigurations().get(policyId);
        SnapshotLifecycleStats statsAfterFirst = slmAfterFirst.getStats();
        final long takenAfterFirst = statsAfterFirst.getMetrics().get(policyId).getSnapshotTakenCount();
        final long failedAfterFirst = statsAfterFirst.getMetrics().get(policyId).getSnapshotFailedCount();
        assertEquals(3, takenAfterFirst);
        assertEquals(0, failedAfterFirst);
        assertEquals(0, policyAfterFirst.getInvocationsSinceLastSuccess());
        assertEquals(first.getName(), policyAfterFirst.getLastSuccess().getSnapshotName());
        assertEquals(
            List.of(),
            ((RegisteredPolicySnapshots) state.metadata().getProject(projectId).custom(RegisteredPolicySnapshots.TYPE))
                .getSnapshotsByPolicy(policyId)
        );
        for (SnapshotId other : others) {
            assertFalse(policyAlreadyRecordsSnapshot(policyAfterFirst, other.getName()));
        }

        for (SnapshotId snapshotId : shuffledList(others)) {
            List<SnapshotId> peers = new ArrayList<>(allSnapshots);
            peers.remove(snapshotId);
            state = SnapshotLifecycleTask.WriteJobStatus.success(
                projectId,
                policyId,
                snapshotId,
                randomLong(),
                randomLong(),
                new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(
                    Set.copyOf(allSnapshots),
                    peers.stream().map(id -> snapshotInfoSuccess(projectId, id)).toList()
                )
            ).execute(state);
        }

        SnapshotLifecycleMetadata slmMetadata = state.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE);
        SnapshotLifecyclePolicyMetadata policyMetadata = slmMetadata.getSnapshotConfigurations().get(policyId);
        assertEquals(3, slmMetadata.getStats().getMetrics().get(policyId).getSnapshotTakenCount());
        assertEquals(0, slmMetadata.getStats().getMetrics().get(policyId).getSnapshotFailedCount());
        assertEquals(0, policyMetadata.getInvocationsSinceLastSuccess());
        assertEquals(first.getName(), policyMetadata.getLastSuccess().getSnapshotName());
        assertEquals(
            List.of(),
            ((RegisteredPolicySnapshots) state.metadata().getProject(projectId).custom(RegisteredPolicySnapshots.TYPE))
                .getSnapshotsByPolicy(policyId)
        );
        assertEquals(3, statsAfterFirst.getMetrics().get(policyId).getSnapshotTakenCount());
        assertEquals(0, policyAfterFirst.getInvocationsSinceLastSuccess());
        assertEquals(first.getName(), policyAfterFirst.getLastSuccess().getSnapshotName());
        assertStatsNotMutated(statsBefore);
        assertStatsNotMutated(statsAfterFirst, policyId, takenAfterFirst, failedAfterFirst);
    }

    /**
     * {@code recordFailureIfUnregistered=true} means CreateSnapshot failed before registration, so lookup cannot have
     * queried this snapshot id. The execute-path still ignores that combination if assertions are disabled.
     */
    public void testNeverRegisteredFlagRejectedWhenSnapshotWasQueried() {
        assumeTrue("assertions enabled", Assertions.ENABLED);
        final SnapshotId snapshotId = randSnapshotId();
        AssertionError error = expectThrows(
            AssertionError.class,
            () -> SnapshotLifecycleTask.WriteJobStatus.failure(
                projectId,
                randomAlphaOfLength(10),
                snapshotId,
                randomLong(),
                new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(Set.of(snapshotId), List.of()),
                new RuntimeException("failed"),
                true
            )
        );
        assertThat(error.getMessage(), containsString(snapshotId.toString()));
    }

    public void testCompletedRegisteredSnapshotInfosRejectsInfoOutsideQueriedSet() {
        assumeTrue("assertions enabled", Assertions.ENABLED);
        final SnapshotId queried = randSnapshotId();
        final SnapshotId notQueried = randSnapshotId();
        final SnapshotInfo info = snapshotInfoSuccess(projectId, notQueried);
        AssertionError ex = expectThrows(
            AssertionError.class,
            () -> new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(Set.of(queried), List.of(info))
        );
        assertThat(ex.getMessage(), containsString(notQueried.toString()));
    }

    public void testCompletedRegisteredSnapshotInfosDefensivelyCopiesInputs() {
        final SnapshotId snapshotId = randSnapshotId();
        final SnapshotInfo info = snapshotInfoSuccess(projectId, snapshotId);
        Set<SnapshotId> queried = new HashSet<>(Set.of(snapshotId));
        List<SnapshotInfo> infos = new ArrayList<>(List.of(info));
        var completed = new SnapshotLifecycleTask.CompletedRegisteredSnapshotInfos(queried, infos);
        queried.clear();
        infos.clear();
        assertEquals(Set.of(snapshotId), completed.queriedSnapshotIds());
        assertEquals(List.of(info), completed.snapshotInfos());
    }

    public void testFindCompletedRegisteredSnapshotIdsExcludesRunningSnapshots() {
        final String policyId = randomAlphaOfLength(10);
        final SnapshotId completed = randSnapshotId();
        final SnapshotId running = randSnapshotId();
        ClusterState clusterState = buildClusterState(
            projectId,
            List.of(policyId),
            Map.of(policyId, List.of(completed, running)),
            Map.of(policyId, List.of(running))
        );
        assertEquals(
            List.of(completed),
            SnapshotLifecycleTask.findCompletedRegisteredSnapshotIds(clusterState.projectState(projectId), policyId)
        );
    }

    public void testGetCurrentlyRunningSnapshots() {
        final SnapshotId snapshot1 = randSnapshotId();
        final SnapshotId snapshot2 = randSnapshotId();
        final SnapshotId snapshot3 = randSnapshotId();
        final SnapshotId snapshot4 = randSnapshotId();

        final String repo1 = randomAlphaOfLength(10);
        final String repo2 = randomAlphaOfLength(10);

        final var snapshotsInProgress = SnapshotsInProgress.EMPTY.createCopyWithUpdatedEntriesForRepo(
            projectId,
            repo1,
            List.of(
                makeSnapshotInProgress(projectId, repo1, "some-policy", snapshot1),
                makeSnapshotInProgress(projectId, repo1, "some-policy", snapshot2),
                makeSnapshotInProgress(projectId, repo1, "other-policy", snapshot3)
            )
        )
            .createCopyWithUpdatedEntriesForRepo(
                projectId,
                repo2,
                List.of(makeSnapshotInProgress(projectId, repo2, "other-policy", snapshot4))
            );

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress)
            .build();

        Set<SnapshotId> currentlyRunning = SnapshotLifecycleTask.currentlyRunningSnapshots(clusterState);
        assertEquals(currentlyRunning, Set.of(snapshot1, snapshot2, snapshot3, snapshot4));
    }

    private static SnapshotId randSnapshotId() {
        return new SnapshotId(randomAlphaOfLength(10), randomUUID());
    }

    private SnapshotLifecycleStats slmStats(ClusterState clusterState) {
        return ((SnapshotLifecycleMetadata) clusterState.metadata().getProject(projectId).custom(SnapshotLifecycleMetadata.TYPE))
            .getStats();
    }

    /**
     * WriteJobStatus must copy-on-write stats (see {@code SLMStatsImmutableIT}): prior cluster-state stats instances and the
     * {@link SnapshotLifecycleMetadata#EMPTY} singleton must remain untouched. Pin primitive counts rather than a shallow
     * map copy of policy stats objects, which would not detect in-place mutation.
     */
    private static void assertStatsNotMutated(SnapshotLifecycleStats emptyStatsSource) {
        assertThat(emptyStatsSource.getMetrics(), equalTo(Map.of()));
        assertThat(SnapshotLifecycleMetadata.EMPTY.getStats().getMetrics(), equalTo(Map.of()));
    }

    private static void assertStatsNotMutated(SnapshotLifecycleStats statsSource, String policyId, long taken, long failed) {
        SnapshotLifecycleStats.SnapshotPolicyStats policyStats = statsSource.getMetrics().get(policyId);
        assertEquals(taken, policyStats.getSnapshotTakenCount());
        assertEquals(failed, policyStats.getSnapshotFailedCount());
        assertThat(SnapshotLifecycleMetadata.EMPTY.getStats().getMetrics(), equalTo(Map.of()));
    }

    /**
     * Whether {@code policyMetadata} currently names {@code snapshotName} as last success or last failure.
     * Concurrent cleanups overwrite these fields, so they must not be used to skip already-recorded outcomes.
     */
    private static boolean policyAlreadyRecordsSnapshot(SnapshotLifecyclePolicyMetadata policyMetadata, String snapshotName) {
        var lastFailure = policyMetadata.getLastFailure();
        var lastSuccess = policyMetadata.getLastSuccess();
        return (lastFailure != null && snapshotName.equals(lastFailure.getSnapshotName()))
            || (lastSuccess != null && snapshotName.equals(lastSuccess.getSnapshotName()));
    }

    private static ClusterState buildClusterState(
        ProjectId projectId,
        List<String> slmPolicies,
        Map<String, List<SnapshotId>> registeredSnaps,
        Map<String, List<SnapshotId>> inProgress
    ) {
        final String repo = randomAlphaOfLength(10);
        List<SnapshotsInProgress.Entry> inProgressEntries = new ArrayList<>();
        for (String policy : inProgress.keySet()) {
            for (SnapshotId snap : inProgress.get(policy)) {
                inProgressEntries.add(makeSnapshotInProgress(projectId, repo, policy, snap));
            }
        }

        final List<RegisteredPolicySnapshots.PolicySnapshot> policySnapshots = new ArrayList<>();
        for (Map.Entry<String, List<SnapshotId>> policySnaps : registeredSnaps.entrySet()) {
            for (SnapshotId snapshotId : policySnaps.getValue()) {
                policySnapshots.add(new RegisteredPolicySnapshots.PolicySnapshot(policySnaps.getKey(), snapshotId));
            }
        }

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .putProjectMetadata(
                ProjectMetadata.builder(projectId)
                    .putCustom(SnapshotLifecycleMetadata.TYPE, makeSnapMeta(slmPolicies))
                    .putCustom(RegisteredPolicySnapshots.TYPE, new RegisteredPolicySnapshots(policySnapshots))
                    .build()
            )
            .putCustom(
                SnapshotsInProgress.TYPE,
                SnapshotsInProgress.EMPTY.createCopyWithUpdatedEntriesForRepo(projectId, repo, inProgressEntries)
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

    private static SnapshotsInProgress.Entry makeSnapshotInProgress(
        ProjectId projectId,
        String repo,
        String policyId,
        SnapshotId snapshotId
    ) {
        final Map<String, Object> metadata = Map.of(SnapshotsService.POLICY_ID_METADATA_FIELD, policyId);
        return SnapshotsInProgress.Entry.snapshot(
            new Snapshot(projectId, repo, snapshotId),
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

    private static SnapshotInfo randomSnapshotInfoSuccess(ProjectId projectId) {
        return snapshotInfoSuccess(projectId, randSnapshotId());
    }

    private static SnapshotInfo snapshotInfoSuccess(ProjectId projectId, SnapshotId snapshotId) {
        long startTime = randomNonNegativeLong();
        long endTime = randomLongBetween(startTime, Long.MAX_VALUE);
        return new SnapshotInfo(
            new Snapshot(projectId, "repo", snapshotId),
            List.of("index1", "index2"),
            List.of(),
            List.of(),
            null,
            endTime,
            2,
            List.of(),
            randomBoolean(),
            Map.of(),
            startTime,
            Map.of()
        );
    }

    private static SnapshotInfo randomSnapshotInfoFailure(ProjectId projectId) {
        return snapshotInfoFailure(projectId, randSnapshotId());
    }

    private static SnapshotInfo snapshotInfoFailure(ProjectId projectId, SnapshotId snapshotId) {
        long startTime = randomNonNegativeLong();
        long endTime = randomLongBetween(startTime, Long.MAX_VALUE);
        return new SnapshotInfo(
            new Snapshot(projectId, "repo", snapshotId),
            List.of("index1", "index2"),
            List.of(),
            List.of(),
            "failed snapshot",
            endTime,
            2,
            List.of(new SnapshotShardFailure("nodeId", new ShardId("index", "uuid", 0), "forced failure")),
            randomBoolean(),
            Map.of(),
            startTime,
            Map.of()
        );
    }
}
