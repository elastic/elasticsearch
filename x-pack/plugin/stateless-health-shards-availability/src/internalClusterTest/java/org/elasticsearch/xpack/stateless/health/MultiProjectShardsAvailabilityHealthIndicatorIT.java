/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.health;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectStateRegistry;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.health.node.ProjectIndexName;
import org.elasticsearch.multiproject.TestOnlyMultiProjectPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.ACTION_ENABLE_INDEX_ROUTING_ALLOCATION;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.PRIMARY_UNASSIGNED_IMPACT_ID;
import static org.elasticsearch.cluster.routing.allocation.shards.ShardsAvailabilityHealthIndicatorService.REPLICA_UNASSIGNED_IMPACT_ID;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.health.StatelessShardsAvailabilityHealthIndicatorService.ALL_REPLICAS_UNASSIGNED_IMPACT_ID;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * Multi-project integration tests for the {@code shards_availability} indicator, focused on multiple projects being unhealthy at once.
 * For multi-project green tests, see {@code 30_shards_availability}
 */
public class MultiProjectShardsAvailabilityHealthIndicatorIT extends AbstractStatelessPluginIntegTestCase {

    private static final String SHARED_INDEX = "shared_index";

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    @Override
    protected boolean addMockFsRepository() {
        // Keep object-store type predictable when creating per-project stores.
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new HashSet<>(super.nodePlugins());
        plugins.add(TestOnlyMultiProjectPlugin.class);
        plugins.add(StatelessShardsHealthPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(TestOnlyMultiProjectPlugin.MULTI_PROJECT_ENABLED.getKey(), true);
    }

    public void testRedPrimariesInTwoProjectsAreAggregated() throws Exception {
        startMasterAndIndexNode();
        zeroBuffers();

        ProjectId projectA = createProject();
        ProjectId projectB = createProject();
        ProjectId projectC = createProject();

        try {
            createUnavailablePrimary(projectA, SHARED_INDEX);
            createUnavailablePrimary(projectB, SHARED_INDEX);
            createGreenIndex(projectC, SHARED_INDEX);

            HealthIndicatorResult result = waitForStatusAndGet(HealthStatus.RED);
            assertThat(result.symptom(), containsString("2 unavailable primary shards"));

            var details = ((SimpleHealthIndicatorDetails) result.details()).details();
            String unavailablePrimaries = (String) details.get("indices_with_unavailable_primaries");
            assertThat(unavailablePrimaries, containsString(projectIndex(projectA, SHARED_INDEX)));
            assertThat(unavailablePrimaries, containsString(projectIndex(projectB, SHARED_INDEX)));
            assertThat(unavailablePrimaries.contains(projectIndex(projectC, SHARED_INDEX)), equalTo(false));

            List<String> enableAllocationIndices = affectedIndices(result, ACTION_ENABLE_INDEX_ROUTING_ALLOCATION.id());
            assertThat(
                enableAllocationIndices,
                containsInAnyOrder(projectIndex(projectA, SHARED_INDEX), projectIndex(projectB, SHARED_INDEX))
            );
            assertThat(enableAllocationIndices, hasSize(2));
        } finally {
            deleteIndex(projectA, SHARED_INDEX);
            deleteIndex(projectB, SHARED_INDEX);
            deleteIndex(projectC, SHARED_INDEX);
        }
    }

    public void testMixedPrimaryAndReplicaFailuresAcrossProjects() throws Exception {
        startMasterAndIndexNode();
        zeroBuffers();

        ProjectId primaryFailureProject = createProject();
        ProjectId replicaFailureProject = createProject();
        final String primaryFailIndex = "primary_fail_index";
        final String replicaFailIndex = "replica_fail_index";

        try {
            createUnavailablePrimary(primaryFailureProject, primaryFailIndex);
            createIndexWithUnassignedReplicas(replicaFailureProject, replicaFailIndex);

            HealthIndicatorResult result = waitForStatusAndGet(HealthStatus.RED);
            assertThat(result.symptom(), containsString("unavailable primary shard"));
            assertThat(result.symptom(), containsString("unavailable replica shard"));

            var details = ((SimpleHealthIndicatorDetails) result.details()).details();
            assertThat(
                (String) details.get("indices_with_unavailable_primaries"),
                equalTo(projectIndex(primaryFailureProject, primaryFailIndex))
            );
            assertThat(
                (String) details.get("indices_with_unavailable_replicas"),
                equalTo(projectIndex(replicaFailureProject, replicaFailIndex))
            );

            List<String> impactIds = result.impacts().stream().map(HealthIndicatorImpact::id).toList();
            assertThat(impactIds, hasItem(PRIMARY_UNASSIGNED_IMPACT_ID));
            assertThat(impactIds, hasItem(ALL_REPLICAS_UNASSIGNED_IMPACT_ID));
            assertThat(impactIds.contains(REPLICA_UNASSIGNED_IMPACT_ID), equalTo(false));

            List<String> allAffectedIndices = result.diagnosisList()
                .stream()
                .flatMap(d -> d.affectedResources().stream())
                .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
                .flatMap(r -> r.getValues().stream())
                .distinct()
                .toList();
            assertThat(
                allAffectedIndices,
                containsInAnyOrder(
                    projectIndex(primaryFailureProject, primaryFailIndex),
                    projectIndex(replicaFailureProject, replicaFailIndex)
                )
            );
        } finally {
            deleteIndex(primaryFailureProject, primaryFailIndex);
            deleteIndex(replicaFailureProject, replicaFailIndex);
        }
    }

    /** The same diagnosis from two projects is merged into one entry whose affected resources list both project-qualified indices. */
    public void testSameDiagnosisMergedAcrossProjects() throws Exception {
        startMasterAndIndexNode();
        zeroBuffers();

        ProjectId projectA = createProject();
        ProjectId projectB = createProject();

        try {
            createUnavailablePrimary(projectA, SHARED_INDEX);
            createUnavailablePrimary(projectB, SHARED_INDEX);

            HealthIndicatorResult result = waitForStatusAndGet(HealthStatus.RED);

            List<Diagnosis> enableAllocationDiagnoses = result.diagnosisList()
                .stream()
                .filter(d -> d.definition().id().equals(ACTION_ENABLE_INDEX_ROUTING_ALLOCATION.id()))
                .toList();
            assertThat(enableAllocationDiagnoses, hasSize(1));
            assertThat(
                affectedIndices(result, ACTION_ENABLE_INDEX_ROUTING_ALLOCATION.id()),
                containsInAnyOrder(projectIndex(projectA, SHARED_INDEX), projectIndex(projectB, SHARED_INDEX))
            );
        } finally {
            deleteIndex(projectA, SHARED_INDEX);
            deleteIndex(projectB, SHARED_INDEX);
        }
    }

    /**
     * Default grace windows (primary_unassigned_buffer_time ~5s, replica_unassigned_buffer_time ~25s) treat freshly
     * unassigned/initializing shards as provisionally unavailable and can keep the indicator GREEN. These tests need
     * unavailable shards to count as real failures immediately so status, details, and diagnoses are deterministic
     * without waiting out the grace period.
     */
    private void zeroBuffers() {
        updateClusterSettings(
            Settings.builder()
                .put("health.shards_availability.replica_unassigned_buffer_time", "0s")
                .put("health.shards_availability.primary_unassigned_buffer_time", "0s")
        );
    }

    private void createUnavailablePrimary(ProjectId projectId, String indexName) {
        assertAcked(
            client().projectClient(projectId)
                .admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(
                    indexSettings(1, 0).put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none").build()
                )
                .setWaitForActiveShards(ActiveShardCount.NONE)
        );
    }

    private void createGreenIndex(ProjectId projectId, String indexName) {
        assertAcked(
            client().projectClient(projectId)
                .admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(indexSettings(1, 0))
                .setWaitForActiveShards(ActiveShardCount.ALL)
        );
        ensureGreen(client().projectClient(projectId), indexName);
    }

    private void createIndexWithUnassignedReplicas(ProjectId projectId, String indexName) throws Exception {
        // Primaries can start on the index node; replicas stay unassigned without search nodes.
        assertAcked(
            client().projectClient(projectId)
                .admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(indexSettings(1, 1))
                .setWaitForActiveShards(ActiveShardCount.NONE)
                .setTimeout(TimeValue.timeValueSeconds(30))
        );
        assertBusy(() -> {
            var health = client().projectClient(projectId).admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT, indexName).get();
            assertThat(health.getActivePrimaryShards(), equalTo(1));
            assertThat(health.getUnassignedShards(), equalTo(1));
        });
    }

    private void deleteIndex(ProjectId projectId, String indexName) {
        try {
            assertAcked(client().projectClient(projectId).admin().indices().prepareDelete(indexName));
        } catch (Exception e) {
            // Best-effort cleanup so suite wipe does not hit cross-project shard locks.
            logger.warn("failed to delete index [{}] in project [{}]", indexName, projectId, e);
        }
    }

    private ProjectId createProject() throws Exception {
        ProjectId projectId = randomUniqueProjectId();
        Settings projectSettings = Settings.builder()
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.FS)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), "project_" + projectId.id())
            .put(ObjectStoreService.BASE_PATH_SETTING.getKey(), "base_path")
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "default")
            .build();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask("put-project-" + projectId, new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState)
                        .putProjectMetadata(ProjectMetadata.builder(projectId))
                        .putCustom(
                            ProjectStateRegistry.TYPE,
                            ProjectStateRegistry.builder(currentState).putProjectSettings(projectId, projectSettings).build()
                        )
                        .build();
                }

                @Override
                public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failure.set(e);
                    latch.countDown();
                }
            });
        assertTrue("timed out creating project " + projectId, latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw failure.get();
        }

        assertBusy(() -> assertNotNull(getCurrentMasterObjectStoreService().getProjectBlobStore(projectId)));
        return projectId;
    }

    private HealthIndicatorResult waitForStatusAndGet(HealthStatus color) throws Exception {
        String indicator = "shards_availability";
        HealthIndicatorResult[] result = new HealthIndicatorResult[1];
        assertBusy(() -> {
            GetHealthAction.Request request = new GetHealthAction.Request(indicator, true, 10);
            var health = client().execute(GetHealthAction.INSTANCE, request).get();
            assertThat(health.findIndicator(indicator).status(), equalTo(color));
            result[0] = health.findIndicator(indicator);
        });
        return result[0];
    }

    private static String projectIndex(ProjectId projectId, String indexName) {
        return new ProjectIndexName(projectId, indexName).toString(true);
    }

    private static List<String> affectedIndices(HealthIndicatorResult hir, String diagnosisId) {
        return hir.diagnosisList()
            .stream()
            .filter(d -> d.definition().id().equals(diagnosisId))
            .flatMap(d -> d.affectedResources().stream())
            .filter(r -> r.getType() == Diagnosis.Resource.Type.INDEX)
            .flatMap(r -> r.getValues().stream())
            .toList();
    }

    private void ensureGreen(Client projectClient, String index) {
        var health = projectClient.admin()
            .cluster()
            .prepareHealth(TEST_REQUEST_TIMEOUT, index)
            .setWaitForGreenStatus()
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();
        assertThat(health.getStatus(), equalTo(ClusterHealthStatus.GREEN));
    }
}
