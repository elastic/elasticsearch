/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;

public class DLMFrozenTransitionServiceTests extends ESTestCase {

    /**
     * Minimal test double implementing {@link DLMFrozenTransitionRunnable} that blocks until released,
     * allowing tests to observe the transition as running and to record which indices were submitted.
     */
    static class TestDLMFrozenTransitionRunnable implements DLMFrozenTransitionRunnable {
        private final String indexName;
        private final CountDownLatch blockUntil;
        private final CountDownLatch started;

        TestDLMFrozenTransitionRunnable(String indexName, CountDownLatch blockUntil) {
            this(indexName, blockUntil, new CountDownLatch(0));
        }

        TestDLMFrozenTransitionRunnable(String indexName, CountDownLatch blockUntil, CountDownLatch started) {
            this.indexName = indexName;
            this.blockUntil = blockUntil;
            this.started = started;
        }

        @Override
        public String getIndexName() {
            return indexName;
        }

        @Override
        public ProjectId getProjectId() {
            return ProjectId.DEFAULT;
        }

        @Override
        public void run() {
            started.countDown();
            try {
                blockUntil.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private TestThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void setupTest() {
        Set<org.elasticsearch.common.settings.Setting<?>> settingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(DLMFrozenTransitionService.POLL_INTERVAL_SETTING);
        settingSet.add(DLMFrozenTransitionService.MAX_CONCURRENCY_SETTING);
        settingSet.add(DLMFrozenTransitionService.MAX_QUEUE_SIZE);
        threadPool = new TestThreadPool(getTestName());
        // Set max_queue_size equal to max_concurrency so that capacity tests remain valid: once maxConcurrency * 2
        // tasks have been submitted, hasCapacity() returns false.
        int maxConcurrency = DLMFrozenTransitionService.MAX_CONCURRENCY_SETTING.getDefault(Settings.EMPTY);
        Settings settings = Settings.builder().put("dlm.frozen_transition.max_queue_size", maxConcurrency).build();
        clusterService = createClusterService(
            threadPool,
            DiscoveryNodeUtils.create("node", "node"),
            settings,
            new ClusterSettings(settings, settingSet)
        );
    }

    @After
    public void tearDown() throws Exception {
        clusterService.close();
        terminate(threadPool);
        super.tearDown();
    }

    private DLMFrozenTransitionService createService() {
        return new DLMFrozenTransitionService(
            clusterService,
            (indexName, pid) -> new TestDLMFrozenTransitionRunnable(indexName, new CountDownLatch(0))
        );
    }

    public void testClusterChangedIgnoredWhenStateNotRecovered() throws Exception {
        var service = createService();
        try {
            ClusterState stateWithBlock = ClusterState.builder(clusterService.state())
                .blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK))
                .build();
            ClusterState previousState = ClusterState.builder(new ClusterName("test")).build();

            ClusterChangedEvent event = new ClusterChangedEvent("test", stateWithBlock, previousState);
            service.clusterChanged(event);

            assertFalse(service.isSchedulerThreadRunning());
            assertFalse(service.isTransitionExecutorRunning());
        } finally {
            service.close();
        }
    }

    public void testBecomingStartsThenLosingMasterStopsThreadPools() throws Exception {
        var service = createService();
        try {
            assertFalse(service.isSchedulerThreadRunning());
            assertFalse(service.isTransitionExecutorRunning());

            service.clusterChanged(createMasterEvent(true));
            assertTrue(service.isSchedulerThreadRunning());
            assertTrue(service.isTransitionExecutorRunning());

            service.clusterChanged(createMasterEvent(false));
            assertFalse(service.isSchedulerThreadRunning());
            assertFalse(service.isTransitionExecutorRunning());

            // Becoming master again should re-create the pools
            service.clusterChanged(createMasterEvent(true));
            assertTrue(service.isSchedulerThreadRunning());
            assertTrue(service.isTransitionExecutorRunning());
        } finally {
            service.close();
        }
    }

    public void testRepeatedMasterEventsAreIdempotent() throws Exception {
        var service = createService();
        try {
            service.clusterChanged(createMasterEvent(true));
            var transitionExecutor = service.getTransitionExecutor();

            // Repeated master events should not recreate the pools
            service.clusterChanged(createMasterEvent(true));
            assertTrue(service.isSchedulerThreadRunning());
            assertSame(transitionExecutor, service.getTransitionExecutor());
        } finally {
            service.close();
        }
    }

    public void testRepeatedNonMasterEventsAreIdempotent() throws Exception {
        var service = createService();
        try {
            service.clusterChanged(createMasterEvent(false));
            assertFalse(service.isSchedulerThreadRunning());
            assertFalse(service.isTransitionExecutorRunning());

            service.clusterChanged(createMasterEvent(false));
            assertFalse(service.isSchedulerThreadRunning());
            assertFalse(service.isTransitionExecutorRunning());
        } finally {
            service.close();
        }
    }

    public void testCloseWhileMaster() throws Exception {
        var service = createService();
        service.clusterChanged(createMasterEvent(true));
        assertTrue(service.isSchedulerThreadRunning());
        assertTrue(service.isTransitionExecutorRunning());

        service.close();
        assertTrue(service.isClosing());
        assertFalse(service.isSchedulerThreadRunning());
        assertFalse(service.isTransitionExecutorRunning());

        // After close, becoming master should not start thread pools
        service.clusterChanged(createMasterEvent(true));
        assertFalse(service.isSchedulerThreadRunning());
        assertFalse(service.isTransitionExecutorRunning());
    }

    public void testCloseWhenNeverMaster() throws Exception {
        var service = createService();
        assertFalse(service.isSchedulerThreadRunning());
        assertFalse(service.isTransitionExecutorRunning());

        service.close();
        assertTrue(service.isClosing());
        assertFalse(service.isSchedulerThreadRunning());
        assertFalse(service.isTransitionExecutorRunning());
    }

    public void testCheckForFrozenIndicesSubmitsOnlyMarkedIndices() throws Exception {
        CountDownLatch blockUntil = new CountDownLatch(1);
        CountDownLatch tasksStarted = new CountDownLatch(2);
        List<String> submittedIndices = new CopyOnWriteArrayList<>();
        var service = new DLMFrozenTransitionService(clusterService, (indexName, pid) -> {
            submittedIndices.add(indexName);
            return new TestDLMFrozenTransitionRunnable(indexName, blockUntil, tasksStarted);
        });
        try {
            ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());

            IndexMetadata markedIndex1 = createMarkedIndex("frozen-ds-1");
            addDataStream(projectBuilder, "frozen-ds-1", markedIndex1);

            IndexMetadata markedIndex2 = createMarkedIndex("frozen-ds-2");
            addDataStream(projectBuilder, "frozen-ds-2", markedIndex2);

            IndexMetadata unmarkedIndex = createUnmarkedIndex("normal-ds");
            addDataStream(projectBuilder, "normal-ds", unmarkedIndex);

            setProjectState(projectBuilder);
            service.clusterChanged(createMasterEvent(true));

            // Wait for both marked indices to be submitted and started
            safeAwait(tasksStarted);

            // A second poll should not re-submit the already-running transitions
            service.checkForFrozenIndices();

            assertEquals("Only marked indices should be submitted", 2, submittedIndices.size());
            assertTrue(submittedIndices.contains(markedIndex1.getIndex().getName()));
            assertTrue(submittedIndices.contains(markedIndex2.getIndex().getName()));
            assertFalse(submittedIndices.contains(unmarkedIndex.getIndex().getName()));
        } finally {
            blockUntil.countDown();
            service.close();
        }
    }

    public void testAlreadyRunningIndexIsNotSubmittedAgain() throws Exception {
        CountDownLatch blockUntil = new CountDownLatch(1);
        CountDownLatch taskStarted = new CountDownLatch(1);
        List<String> submittedIndices = new CopyOnWriteArrayList<>();
        var service = new DLMFrozenTransitionService(clusterService, (indexName, pid) -> {
            submittedIndices.add(indexName);
            return new TestDLMFrozenTransitionRunnable(indexName, blockUntil, taskStarted);
        });
        try {
            IndexMetadata markedIndex = createMarkedIndex("frozen-ds");
            ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
            addDataStream(projectBuilder, "frozen-ds", markedIndex);

            setProjectState(projectBuilder);
            service.clusterChanged(createMasterEvent(true));

            // Wait for the task to actually start running on the executor
            safeAwait(taskStarted);
            assertEquals(1, submittedIndices.size());

            // Second poll should skip the already-running index
            service.checkForFrozenIndices();
            assertEquals("Running index should not be submitted again", 1, submittedIndices.size());
        } finally {
            blockUntil.countDown();
            service.close();
        }
    }

    public void testCheckForFrozenIndicesReturnsEarlyWhenCapacityExhausted() throws Exception {
        int maxConcurrency = DLMFrozenTransitionService.MAX_CONCURRENCY_SETTING.get(clusterService.getSettings());
        int maxQueue = DLMFrozenTransitionService.MAX_QUEUE_SIZE.get(clusterService.getSettings());

        int maxJobs = maxConcurrency + maxQueue;

        CountDownLatch blockUntil = new CountDownLatch(1);
        CountDownLatch allSubmitted = new CountDownLatch(maxJobs);
        List<String> submittedIndices = new CopyOnWriteArrayList<>();
        var service = new DLMFrozenTransitionService(clusterService, (indexName, pid) -> {
            submittedIndices.add(indexName);
            allSubmitted.countDown();
            return new TestDLMFrozenTransitionRunnable(indexName, blockUntil);
        });
        try {
            // Start with exactly maxJobs marked indices so the initial poll fills capacity without rejection
            ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
            for (int i = 1; i <= maxJobs; i++) {
                String dsName = "frozen-ds-" + i;
                addDataStream(projectBuilder, dsName, createMarkedIndex(dsName));
            }

            setProjectState(projectBuilder);
            service.clusterChanged(createMasterEvent(true));

            // Wait until all maxJobs tasks have been accepted by the executor (capacity now exhausted)
            safeAwait(allSubmitted);
            // The factory latch fires before executor.submit() adds the task to submittedTransitions,
            // so we must also wait for the executor to reflect full capacity exhaustion.
            assertBusy(() -> assertFalse(service.getTransitionExecutor().hasCapacity()));
            assertEquals(maxJobs, submittedIndices.size());

            // Add more marked indices to the cluster state
            ProjectMetadata existingProject = clusterService.state().metadata().projects().values().iterator().next();
            ProjectMetadata.Builder updatedBuilder = ProjectMetadata.builder(existingProject);
            addDataStream(updatedBuilder, "frozen-ds-extra", createMarkedIndex("frozen-ds-extra"));

            setProjectState(updatedBuilder);

            // Manually trigger poll — running indices are skipped, new index hits capacity check and returns early
            service.checkForFrozenIndices();
            assertEquals("No additional indices should be submitted when capacity is exhausted", maxJobs, submittedIndices.size());
        } finally {
            blockUntil.countDown();
            service.close();
        }
    }

    /**
     * A task that has been submitted but is waiting in the executor queue (single thread occupied, second index
     * queued but not yet started) must be treated as "running" by the de-duplication guard in
     * {@code checkForFrozenIndices}, so a second poll does not submit it a second time.
     */
    public void testAlreadyQueuedIndexIsNotResubmitted() throws Exception {
        Set<org.elasticsearch.common.settings.Setting<?>> allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(DLMFrozenTransitionService.POLL_INTERVAL_SETTING);
        allSettings.add(DLMFrozenTransitionService.MAX_CONCURRENCY_SETTING);
        allSettings.add(DLMFrozenTransitionService.MAX_QUEUE_SIZE);
        Settings singleThreadSettings = Settings.builder()
            .put("dlm.frozen_transition.max_concurrency", 1)
            .put("dlm.frozen_transition.max_queue_size", 5)
            .build();
        ClusterService localClusterService = createClusterService(
            threadPool,
            DiscoveryNodeUtils.create("local-node", "local-node"),
            singleThreadSettings,
            new ClusterSettings(singleThreadSettings, allSettings)
        );
        try {
            CountDownLatch blockUntil = new CountDownLatch(1);
            List<String> submittedIndices = new CopyOnWriteArrayList<>();

            IndexMetadata firstIndex = createMarkedIndex("first-ds");
            IndexMetadata secondIndex = createMarkedIndex("second-ds");
            ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
            addDataStream(projectBuilder, "first-ds", firstIndex);
            addDataStream(projectBuilder, "second-ds", secondIndex);
            setState(localClusterService, ClusterState.builder(localClusterService.state()).putProjectMetadata(projectBuilder).build());

            var service = new DLMFrozenTransitionService(localClusterService, (indexName, pid) -> {
                submittedIndices.add(indexName);
                return new TestDLMFrozenTransitionRunnable(indexName, blockUntil);
            });
            try {
                service.clusterChanged(createMasterEventFor(localClusterService, true));

                // Wait until both indices have been accepted by the executor — one is running on the single
                // thread, the other is waiting in the queue. We poll submittedTransitions directly so that
                // the assertion is independent of which index happened to be scheduled first.
                assertBusy(() -> {
                    DLMFrozenTransitionExecutor exec = service.getTransitionExecutor();
                    assertNotNull(exec);
                    assertTrue(exec.transitionSubmitted(firstIndex.getIndex().getName()));
                    assertTrue(exec.transitionSubmitted(secondIndex.getIndex().getName()));
                });
                assertEquals(2, submittedIndices.size());

                // A second poll must skip both: one is running, the other is queued but not yet started.
                service.checkForFrozenIndices();
                assertEquals("Queued index must not be submitted a second time", 2, submittedIndices.size());
            } finally {
                blockUntil.countDown();
                service.close();
            }
        } finally {
            localClusterService.close();
        }
    }

    private IndexMetadata createMarkedIndex(String dataStreamName) {
        return IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putCustom(
                DataStreamsPlugin.LIFECYCLE_CUSTOM_INDEX_METADATA_KEY,
                Map.of(DataStreamLifecycleService.FROZEN_CANDIDATE_REPOSITORY_METADATA_KEY, "my-repo")
            )
            .build();
    }

    private IndexMetadata createUnmarkedIndex(String dataStreamName) {
        return IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
    }

    private void addDataStream(ProjectMetadata.Builder projectBuilder, String name, IndexMetadata index) {
        projectBuilder.put(index, true);
        projectBuilder.put(DataStream.builder(name, List.of(index.getIndex())).setGeneration(1).build());
    }

    private void setProjectState(ProjectMetadata.Builder projectBuilder) {
        setState(clusterService, ClusterState.builder(clusterService.state()).putProjectMetadata(projectBuilder).build());
    }

    private ClusterChangedEvent createMasterEvent(boolean isMaster) {
        return createMasterEventFor(clusterService, isMaster);
    }

    private ClusterChangedEvent createMasterEventFor(ClusterService cs, boolean isMaster) {
        var localNode = DiscoveryNodeUtils.create("local-node", "local-node");
        var otherNode = DiscoveryNodeUtils.create("other-node", "other-node");

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId("local-node");

        if (isMaster) {
            nodesBuilder.masterNodeId("local-node");
        } else {
            nodesBuilder.masterNodeId("other-node");
        }

        ClusterState newState = ClusterState.builder(cs.state()).nodes(nodesBuilder).blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK).build();

        ClusterState previousState = ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(localNode)
                    .add(otherNode)
                    .localNodeId("local-node")
                    .masterNodeId(isMaster ? "other-node" : "local-node")
            )
            .build();

        return new ClusterChangedEvent("test", newState, previousState);
    }
}
