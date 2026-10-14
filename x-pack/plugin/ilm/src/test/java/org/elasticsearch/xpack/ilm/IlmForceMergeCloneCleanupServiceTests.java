/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.SearchableSnapshotAction;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class IlmForceMergeCloneCleanupServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private ProjectId projectId;

    private CopyOnWriteArrayList<DeleteIndexRequest> capturedDeleteIndexRequests;
    private AtomicReference<AcknowledgedResponse> mockDeleteIndexResponse;
    private AtomicReference<Exception> mockDeleteIndexFailure;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        Set<Setting<?>> allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(IlmForceMergeCloneCleanupService.POLL_INTERVAL_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, allSettings);
        clusterService = createClusterService(threadPool, clusterSettings);
        projectId = randomProjectIdOrDefault();

        capturedDeleteIndexRequests = new CopyOnWriteArrayList<>();
        mockDeleteIndexResponse = new AtomicReference<>(AcknowledgedResponse.of(true));
        mockDeleteIndexFailure = new AtomicReference<>();
    }

    @After
    public void cleanup() {
        clusterService.close();
        terminate(threadPool);
    }

    private NoOpClient createCapturingTestClient() {
        return new NoOpClient(threadPool, TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof DeleteIndexRequest req) {
                    capturedDeleteIndexRequests.add(req);
                    if (mockDeleteIndexFailure.get() != null) {
                        listener.onFailure(mockDeleteIndexFailure.get());
                    } else if (mockDeleteIndexResponse.get() != null) {
                        listener.onResponse((Response) mockDeleteIndexResponse.get());
                    }
                } else {
                    fail("Unexpected request type: " + request.getClass());
                }
            }
        };
    }

    private IlmForceMergeCloneCleanupService createService() {
        return new IlmForceMergeCloneCleanupService(clusterService, createCapturingTestClient(), TimeValue.timeValueDays(1).millis());
    }

    /**
     * Builds an fm-clone index bearing the ILM marker pointing at {@code sourceUuid}.
     */
    private IndexMetadata createMarkedFmClone(String name, String sourceUuid) {
        return IndexMetadata.builder(name)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_FORCE_MERGE_CLONE_SOURCE_UUID, sourceUuid).build())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    /** Builds a source index (no marker). */
    private IndexMetadata createSourceIndex(String name) {
        return IndexMetadata.builder(name).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0).build();
    }

    /** Builds a source index whose execution state tracks the given clone name. */
    private IndexMetadata createSourceIndexWithTrackedClone(String name, String cloneName) {
        LifecycleExecutionState state = LifecycleExecutionState.builder().setForceMergeCloneIndexName(cloneName).build();
        return IndexMetadata.builder(name)
            .settings(settings(IndexVersion.current()))
            .putCustom(ILM_CUSTOM_METADATA_KEY, state.asMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    /** Builds a searchable-snapshot-backed index that also carries the marker (backstop scenario). */
    private IndexMetadata createSearchableSnapshotWithMarker(String name, String sourceUuid) {
        return IndexMetadata.builder(name)
            .settings(
                settings(IndexVersion.current()).put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), "snapshot")
                    .put(LifecycleSettings.LIFECYCLE_FORCE_MERGE_CLONE_SOURCE_UUID, sourceUuid)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private void setClusterState(List<IndexMetadata> indices) {
        setClusterState(indices, OperationMode.RUNNING);
    }

    private void setClusterState(List<IndexMetadata> indices, OperationMode ilmMode) {
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        for (IndexMetadata idx : indices) {
            projectBuilder.put(idx, false);
        }
        if (ilmMode != OperationMode.RUNNING) {
            projectBuilder.putCustom(LifecycleOperationMetadata.TYPE, new LifecycleOperationMetadata(ilmMode, OperationMode.RUNNING));
        }
        Metadata metadata = Metadata.builder().put(projectBuilder).build();
        setState(clusterService, ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build());
    }

    /** Publishes a state where the local node is master and the given indices are present. */
    private void becomeMasterWithIndices(List<IndexMetadata> indices) {
        ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(projectId);
        for (IndexMetadata idx : indices) {
            projectBuilder.put(idx, false);
        }
        Metadata metadata = Metadata.builder().put(projectBuilder).build();
        // Build a fresh state (no routing table) so the metadata's single project stays consistent, but carry over the
        // discovery nodes from the current state and elect the local node as master to trigger the scheduler.
        DiscoveryNodes nodes = clusterService.state().nodes();
        setState(
            clusterService,
            ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).nodes(nodes.withMasterNodeId(nodes.getLocalNodeId())).build()
        );
    }

    // ── core orphan-predicate tests ───────────────────────────────────────────

    public void testOrphanedClone_deleted() {
        IndexMetadata source = createSourceIndex("my-source");
        IndexMetadata orphan = createMarkedFmClone(
            SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "abc-my-source",
            source.getIndexUUID()
        );
        setClusterState(List.of(source, orphan));

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests.size(), is(1));
        assertThat(List.of(capturedDeleteIndexRequests.get(0).indices()), containsInAnyOrder(orphan.getIndex().getName()));
    }

    public void testMultipleOrphanedClones_allDeleted() {
        IndexMetadata source = createSourceIndex("my-source");
        String uuid = source.getIndexUUID();
        IndexMetadata orphan1 = createMarkedFmClone(SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "aaa-my-source", uuid);
        IndexMetadata orphan2 = createMarkedFmClone(SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "bbb-my-source", uuid);
        setClusterState(List.of(source, orphan1, orphan2));

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests.size(), is(1));
        assertThat(
            List.of(capturedDeleteIndexRequests.get(0).indices()),
            containsInAnyOrder(orphan1.getIndex().getName(), orphan2.getIndex().getName())
        );
    }

    public void testCloneTrackedBySourceIndex_notDeleted() {
        String cloneName = SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "abc-my-source";
        IndexMetadata source = createSourceIndexWithTrackedClone("my-source", cloneName);
        IndexMetadata clone = createMarkedFmClone(cloneName, source.getIndexUUID());
        setClusterState(List.of(source, clone));

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests, empty());
    }

    public void testCloneTrackedByRestoredIndex_notDeleted() {
        // CopyExecutionStateStep copies the full execution state, including the clone pointer, onto the restored index.
        String cloneName = SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "abc-my-source";
        IndexMetadata source = createSourceIndex("my-source");
        IndexMetadata clone = createMarkedFmClone(cloneName, source.getIndexUUID());
        // The restored index tracks the clone name in its execution state (copied by CopyExecutionStateStep)
        LifecycleExecutionState restoredState = LifecycleExecutionState.builder().setForceMergeCloneIndexName(cloneName).build();
        IndexMetadata restored = IndexMetadata.builder("restored-my-source")
            .settings(settings(IndexVersion.current()))
            .putCustom(ILM_CUSTOM_METADATA_KEY, restoredState.asMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        setClusterState(List.of(source, clone, restored));

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests, empty());
    }

    public void testUnmarkedFmClone_notDeleted() {
        // A legacy clone (created before this change) has no marker and must not be deleted.
        IndexMetadata source = createSourceIndex("my-source");
        IndexMetadata unmarked = IndexMetadata.builder(SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "abc-my-source")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        setClusterState(List.of(source, unmarked));

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests, empty());
    }

    public void testMarkerWithAnyUuid_deleted() {
        // The UUID in the marker proves ILM provenance; it need not match any existing source index.
        // A clone whose source was deleted (or recreated with a new UUID) is still an orphan.
        IndexMetadata source = createSourceIndex("my-source");
        IndexMetadata orphan = createMarkedFmClone(
            SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "abc-my-source",
            randomAlphaOfLength(22)
        );
        setClusterState(List.of(source, orphan));

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests.size(), is(1));
        assertThat(List.of(capturedDeleteIndexRequests.get(0).indices()), containsInAnyOrder(orphan.getIndex().getName()));
    }

    public void testSearchableSnapshotWithMarker_notDeleted() {
        // A mounted index that somehow inherited the marker (e.g. snapshot taken before MountSnapshotStep strips it).
        IndexMetadata source = createSourceIndex("my-source");
        IndexMetadata mounted = createSearchableSnapshotWithMarker("restored-my-source", source.getIndexUUID());
        setClusterState(List.of(source, mounted));

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests, empty());
    }

    public void testIlmStopped_nothingDeleted() {
        IndexMetadata source = createSourceIndex("my-source");
        IndexMetadata orphan = createMarkedFmClone(
            SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "abc-my-source",
            source.getIndexUUID()
        );
        setClusterState(List.of(source, orphan), OperationMode.STOPPED);

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests, empty());
    }

    public void testIlmStopping_nothingDeleted() {
        // ILM transitions through STOPPING before STOPPED while in-flight steps drain; the service must
        // stay quiet during that window too.
        IndexMetadata source = createSourceIndex("my-source");
        IndexMetadata orphan = createMarkedFmClone(
            SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "abc-my-source",
            source.getIndexUUID()
        );
        setClusterState(List.of(source, orphan), OperationMode.STOPPING);

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests, empty());
    }

    public void testDeleteFailure_doesNotPropagate() {
        IndexMetadata source = createSourceIndex("my-source");
        IndexMetadata orphan = createMarkedFmClone(
            SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "abc-my-source",
            source.getIndexUUID()
        );
        setClusterState(List.of(source, orphan));
        mockDeleteIndexFailure.set(new RuntimeException("simulated delete failure"));

        try (IlmForceMergeCloneCleanupService service = createService()) {
            // Must complete without throwing
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests.size(), is(1));
    }

    // ── multi-project test ────────────────────────────────────────────────────

    public void testMultipleProjects_eachProjectCleaned() {
        ProjectId projectId2 = randomValueOtherThan(projectId, ESTestCase::randomProjectIdOrDefault);

        IndexMetadata source1 = createSourceIndex("source-1");
        IndexMetadata orphan1 = createMarkedFmClone(
            SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "aaa-source-1",
            source1.getIndexUUID()
        );

        IndexMetadata source2 = createSourceIndex("source-2");
        IndexMetadata orphan2 = createMarkedFmClone(
            SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "bbb-source-2",
            source2.getIndexUUID()
        );

        ProjectMetadata.Builder project1Builder = ProjectMetadata.builder(projectId).put(source1, false).put(orphan1, false);
        ProjectMetadata.Builder project2Builder = ProjectMetadata.builder(projectId2).put(source2, false).put(orphan2, false);
        Metadata metadata = Metadata.builder().put(project1Builder).put(project2Builder).build();
        setState(clusterService, ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).build());

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests.size(), is(2));
        List<String> allDeletedIndices = capturedDeleteIndexRequests.stream().flatMap(req -> Arrays.stream(req.indices())).toList();
        assertThat(allDeletedIndices, containsInAnyOrder(orphan1.getIndex().getName(), orphan2.getIndex().getName()));
    }

    public void testMarkedNonClonePrefix_notDeleted() {
        // A marked, non-searchable-snapshot index whose name lacks the fm-clone- prefix must be
        // excluded by the prefix filter, independent of the searchable-snapshot filter.
        IndexMetadata source = createSourceIndex("my-source");
        IndexMetadata marked = createMarkedFmClone("regular-index", source.getIndexUUID());
        setClusterState(List.of(source, marked));

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.cleanUpOrphanedForceMergeClones();
        }

        assertThat(capturedDeleteIndexRequests, empty());
    }

    // ── scheduler lifecycle tests ─────────────────────────────────────────────

    public void testSchedulerStartsOnMasterGainedAndStopsOnMasterLost() throws Exception {
        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.init();
            assertThat(service.isSchedulerRunning(), is(false));

            // simulate becoming master
            setState(
                clusterService,
                ClusterState.builder(clusterService.state())
                    .nodes(clusterService.state().nodes().withMasterNodeId(clusterService.localNode().getId()))
                    .build()
            );
            assertBusy(() -> assertThat(service.isSchedulerRunning(), is(true)));

            // simulate losing mastership
            setState(
                clusterService,
                ClusterState.builder(clusterService.state()).nodes(clusterService.state().nodes().withMasterNodeId(null)).build()
            );
            assertBusy(() -> assertThat(service.isSchedulerRunning(), is(false)));
        }
    }

    public void testClose_terminatesScheduler() throws Exception {
        IlmForceMergeCloneCleanupService service = createService();
        try {
            service.init();
            // Force master state so the scheduler starts
            setState(
                clusterService,
                ClusterState.builder(clusterService.state())
                    .nodes(clusterService.state().nodes().withMasterNodeId(clusterService.localNode().getId()))
                    .build()
            );
            assertBusy(() -> assertThat(service.isSchedulerRunning(), is(true)));

            service.close();
            assertThat(service.isSchedulerRunning(), is(false));
        } finally {
            // close() is idempotent; ensures the scheduler thread never leaks if an assertion above fails.
            service.close();
        }
    }

    public void testSchedulerDoesNotStartWhileStateNotRecovered() throws Exception {
        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.init();

            // Master elected but the cluster state has not recovered yet: the scheduler must stay down.
            setState(
                clusterService,
                ClusterState.builder(clusterService.state())
                    .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
                    .nodes(clusterService.state().nodes().withMasterNodeId(clusterService.localNode().getId()))
                    .build()
            );
            assertThat(service.isSchedulerRunning(), is(false));

            // Once the recovery block clears, the scheduler starts.
            setState(clusterService, ClusterState.builder(clusterService.state()).blocks(ClusterBlocks.builder().build()).build());
            assertBusy(() -> assertThat(service.isSchedulerRunning(), is(true)));
        }
    }

    public void testDynamicPollIntervalUpdateReschedulesAndRunsPromptly() throws Exception {
        IndexMetadata source = createSourceIndex("my-source");
        IndexMetadata orphan = createMarkedFmClone(
            SearchableSnapshotAction.FORCE_MERGE_CLONE_INDEX_PREFIX + "abc-my-source",
            source.getIndexUUID()
        );

        try (IlmForceMergeCloneCleanupService service = createService()) {
            service.init();
            // Scheduler starts on master election but with a 1-day initial delay, so no sweep fires yet.
            becomeMasterWithIndices(List.of(source, orphan));
            assertBusy(() -> assertThat(service.isSchedulerRunning(), is(true)));
            assertThat(capturedDeleteIndexRequests, empty());

            // Lowering the interval dynamically restarts the scheduler with a zero initial delay, so the
            // pending orphan is swept promptly rather than after the original delay.
            clusterService.getClusterSettings()
                .applySettings(Settings.builder().put(IlmForceMergeCloneCleanupService.POLL_INTERVAL_SETTING.getKey(), "1s").build());

            // The mock client does not mutate cluster state, so the 1s scheduler keeps sweeping; assert at
            // least one prompt sweep happened rather than an exact count.
            assertBusy(() -> assertThat(capturedDeleteIndexRequests.size(), greaterThanOrEqualTo(1)));
            assertThat(List.of(capturedDeleteIndexRequests.get(0).indices()), containsInAnyOrder(orphan.getIndex().getName()));
        }
    }
}
