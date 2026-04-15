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

public class DlmFrozenTransitionServiceTests extends ESTestCase {

    /**
     * Minimal test double implementing {@link DlmFrozenTransitionRunnable} that blocks until released,
     * allowing tests to observe the transition as running and to record which indices were submitted.
     */
    static class TestDlmFrozenTransitionRunnable implements DlmFrozenTransitionRunnable {
        private final String indexName;
        private final CountDownLatch blockUntil;
        private final CountDownLatch started;

        TestDlmFrozenTransitionRunnable(String indexName, CountDownLatch blockUntil) {
            this(indexName, blockUntil, new CountDownLatch(0));
        }

        TestDlmFrozenTransitionRunnable(String indexName, CountDownLatch blockUntil, CountDownLatch started) {
            this.indexName = indexName;
            this.blockUntil = blockUntil;
            this.started = started;
        }

        @Override
        public String getIndexName() {
            return indexName;
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
        settingSet.add(DlmFrozenTransitionService.POLL_INTERVAL_SETTING);
        settingSet.add(DlmFrozenTransitionService.MAX_CONCURRENCY_SETTING);
        threadPool = new TestThreadPool(getTestName());
        clusterService = createClusterService(
            threadPool,
            DiscoveryNodeUtils.create("node", "node"),
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, settingSet)
        );
    }

    @After
    public void tearDown() throws Exception {
        clusterService.close();
        terminate(threadPool);
        super.tearDown();
    }

    private DlmFrozenTransitionService createService() {
        return new DlmFrozenTransitionService(
            clusterService,
            (indexName, pid) -> new TestDlmFrozenTransitionRunnable(indexName, new CountDownLatch(0))
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
        var service = new DlmFrozenTransitionService(clusterService, (indexName, pid) -> {
            submittedIndices.add(indexName);
            return new TestDlmFrozenTransitionRunnable(indexName, blockUntil, tasksStarted);
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
        var service = new DlmFrozenTransitionService(clusterService, (indexName, pid) -> {
            submittedIndices.add(indexName);
            return new TestDlmFrozenTransitionRunnable(indexName, blockUntil, taskStarted);
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
        int maxConcurrency = DlmFrozenTransitionService.MAX_CONCURRENCY_SETTING.getDefault(Settings.EMPTY);
        CountDownLatch blockUntil = new CountDownLatch(1);
        CountDownLatch tasksStarted = new CountDownLatch(maxConcurrency);
        List<String> submittedIndices = new CopyOnWriteArrayList<>();
        var service = new DlmFrozenTransitionService(clusterService, (indexName, pid) -> {
            submittedIndices.add(indexName);
            return new TestDlmFrozenTransitionRunnable(indexName, blockUntil, tasksStarted);
        });
        try {
            // Start with exactly maxConcurrency marked indices so the initial poll fills capacity without rejection
            ProjectMetadata.Builder projectBuilder = ProjectMetadata.builder(randomProjectIdOrDefault());
            for (int i = 1; i <= maxConcurrency; i++) {
                String dsName = "frozen-ds-" + i;
                addDataStream(projectBuilder, dsName, createMarkedIndex(dsName));
            }

            setProjectState(projectBuilder);
            service.clusterChanged(createMasterEvent(true));

            // Wait for all tasks to start (capacity now exhausted)
            safeAwait(tasksStarted);
            assertEquals(maxConcurrency, submittedIndices.size());

            // Add more marked indices to the cluster state
            ProjectMetadata existingProject = clusterService.state().metadata().projects().values().iterator().next();
            ProjectMetadata.Builder updatedBuilder = ProjectMetadata.builder(existingProject);
            addDataStream(updatedBuilder, "frozen-ds-extra", createMarkedIndex("frozen-ds-extra"));

            setProjectState(updatedBuilder);

            // Manually trigger poll — running indices are skipped, new index hits capacity check and returns early
            service.checkForFrozenIndices();
            assertEquals("No additional indices should be submitted when capacity is exhausted", maxConcurrency, submittedIndices.size());
        } finally {
            blockUntil.countDown();
            service.close();
        }
    }

    public void testPollIntervalMinimum() {
        Settings tooLow = Settings.builder().put("dlm.frozen_transition.poll_interval", "30s").build();
        expectThrows(IllegalArgumentException.class, () -> DlmFrozenTransitionService.POLL_INTERVAL_SETTING.get(tooLow));
    }

    public void testMaxConcurrencyMinimum() {
        Settings tooLow = Settings.builder().put("dlm.frozen_transition.max_concurrency", 0).build();
        expectThrows(IllegalArgumentException.class, () -> DlmFrozenTransitionService.MAX_CONCURRENCY_SETTING.get(tooLow));
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
        var localNode = DiscoveryNodeUtils.create("local-node", "local-node");
        var otherNode = DiscoveryNodeUtils.create("other-node", "other-node");

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId("local-node");

        if (isMaster) {
            nodesBuilder.masterNodeId("local-node");
        } else {
            nodesBuilder.masterNodeId("other-node");
        }

        ClusterState newState = ClusterState.builder(clusterService.state())
            .nodes(nodesBuilder)
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();

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
