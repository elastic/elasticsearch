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
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;

public class DLMFrozenTransitionPluginTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ClusterService clusterService;
    private DLMFrozenTransitionExecutor transitionExecutor;

    @Before
    public void setupTest() {
        threadPool = new TestThreadPool(
            getTestName(),
            new FixedExecutorBuilder(
                Settings.EMPTY,
                DLMFrozenTransitionPlugin.EXECUTOR_NAME,
                2,
                2,
                "dlm.frozen.transition.thread_pool",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
        clusterService = createClusterService(threadPool);
        transitionExecutor = new DLMFrozenTransitionExecutor(
            clusterService,
            4,
            DLMFrozenTransitionSettings.create(clusterService),
            new DataStreamLifecycleErrorStore(System::currentTimeMillis),
            threadPool.executor(DLMFrozenTransitionPlugin.EXECUTOR_NAME)
        );
    }

    @After
    public void tearDown() throws Exception {
        clusterService.close();
        terminate(threadPool);
        super.tearDown();
    }

    /**
     * Verifies that {@link DLMFrozenTransitionPlugin#close()} shuts down the thread pools of all
     * managed services.
     */
    public void testCloseShutsDownAllManagedServices() throws IOException {
        var transitionService = new DLMFrozenTransitionService(
            clusterService,
            (indexName, pid) -> new DLMFrozenTransitionServiceTests.TestDLMFrozenTransitionRunnable(
                indexName,
                new java.util.concurrent.CountDownLatch(0)
            ),
            transitionExecutor
        );
        var cleanupService = new DLMFrozenCleanupService(clusterService, new NoOpClient(threadPool));
        var plugin = new DLMFrozenTransitionPlugin(List.of(transitionService, cleanupService));

        // Simulate becoming master so both services start their thread pools
        var localNode = DiscoveryNodeUtils.create("node", "node");
        ClusterState masterState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .build();
        ClusterState previousState = ClusterState.builder(new ClusterName("test")).build();
        ClusterChangedEvent masterEvent = new ClusterChangedEvent("test", masterState, previousState);

        transitionService.clusterChanged(masterEvent);
        cleanupService.clusterChanged(masterEvent);

        assertTrue("transition scheduler should be running", transitionService.isSchedulerThreadRunning());
        assertTrue("cleanup scheduler should be running", cleanupService.isSchedulerThreadRunning());

        plugin.close();

        assertFalse("transition scheduler should be stopped after close", transitionService.isSchedulerThreadRunning());
        assertFalse("cleanup scheduler should be stopped after close", cleanupService.isSchedulerThreadRunning());
    }
}
