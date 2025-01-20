/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class InternalClusterInfoServiceSchedulingTests extends ESTestCase {

    public void testScheduling() {
        final DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("test");
        final DiscoveryNodes noMaster = DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).build();
        final DiscoveryNodes localMaster = noMaster.withMasterNodeId(discoveryNode.getId());

        final Settings.Builder settingsBuilder = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), discoveryNode.getName());
        if (randomBoolean()) {
            settingsBuilder.put(INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), randomIntBetween(10000, 60000) + "ms");
        }
        final Settings settings = settingsBuilder.build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();

        final ClusterApplierService clusterApplierService = new ClusterApplierService("test", settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };

        final MasterService masterService = new FakeThreadPoolMasterService("test", threadPool, r -> {
            fail("master service should not run any tasks");
        });

        final ClusterService clusterService = new ClusterService(settings, clusterSettings, masterService, clusterApplierService);

        final FakeClusterInfoServiceClient client = new FakeClusterInfoServiceClient(threadPool);
        final InternalClusterInfoService clusterInfoService = new InternalClusterInfoService(settings, clusterService, threadPool, client);
        clusterService.addListener(clusterInfoService);
        clusterInfoService.addListener(ignored -> {});

        clusterService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        clusterApplierService.setInitialState(ClusterState.builder(new ClusterName("cluster")).nodes(noMaster).build());
        masterService.setClusterStatePublisher((clusterChangedEvent, publishListener, ackListener) -> fail("should not publish"));
        masterService.setClusterStateSupplier(clusterApplierService::state);
        clusterService.start();

        final AtomicBoolean becameMaster1 = new AtomicBoolean();
        clusterApplierService.onNewClusterState(
            "become master 1",
            () -> ClusterState.builder(new ClusterName("cluster")).nodes(localMaster).build(),
            setFlagOnSuccess(becameMaster1)
        );
        runUntilFlag(deterministicTaskQueue, becameMaster1);

        final AtomicBoolean failMaster1 = new AtomicBoolean();
        clusterApplierService.onNewClusterState(
            "fail master 1",
            () -> ClusterState.builder(new ClusterName("cluster")).nodes(noMaster).build(),
            setFlagOnSuccess(failMaster1)
        );
        runUntilFlag(deterministicTaskQueue, failMaster1);

        final AtomicBoolean becameMaster2 = new AtomicBoolean();
        clusterApplierService.onNewClusterState(
            "become master 2",
            () -> ClusterState.builder(new ClusterName("cluster")).nodes(localMaster).build(),
            setFlagOnSuccess(becameMaster2)
        );
        runUntilFlag(deterministicTaskQueue, becameMaster2);
        deterministicTaskQueue.runAllRunnableTasks();

        for (int i = 0; i < 3; i++) {
            final int initialRequestCount = client.requestCount;
            final long duration = INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings).millis();
            runFor(deterministicTaskQueue, duration);
            deterministicTaskQueue.runAllRunnableTasks();
            assertThat(client.requestCount, equalTo(initialRequestCount + 2)); // should have run two client requests per interval
        }

        final AtomicBoolean failMaster2 = new AtomicBoolean();
        clusterApplierService.onNewClusterState(
            "fail master 2",
            () -> ClusterState.builder(new ClusterName("cluster")).nodes(noMaster).build(),
            setFlagOnSuccess(failMaster2)
        );
        runUntilFlag(deterministicTaskQueue, failMaster2);

        runFor(deterministicTaskQueue, INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings).millis());
        deterministicTaskQueue.runAllRunnableTasks();
        assertFalse(deterministicTaskQueue.hasRunnableTasks());
        assertFalse(deterministicTaskQueue.hasDeferredTasks());
    }

    private static void runFor(DeterministicTaskQueue deterministicTaskQueue, long duration) {
        final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + duration;
        while (deterministicTaskQueue.getCurrentTimeMillis() < endTime
            && (deterministicTaskQueue.hasRunnableTasks() || deterministicTaskQueue.hasDeferredTasks())) {
            if (deterministicTaskQueue.hasDeferredTasks() && randomBoolean()) {
                deterministicTaskQueue.advanceTime();
            } else if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            }
        }
    }

    private static void runUntilFlag(DeterministicTaskQueue deterministicTaskQueue, AtomicBoolean flag) {
        while (flag.get() == false) {
            if (deterministicTaskQueue.hasDeferredTasks() && randomBoolean()) {
                deterministicTaskQueue.advanceTime();
            } else if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            }
        }
    }

    private static ActionListener<Void> setFlagOnSuccess(AtomicBoolean flag) {
        return ActionTestUtils.assertNoFailureListener(ignored -> assertTrue(flag.compareAndSet(false, true)));
    }

    private static class FakeClusterInfoServiceClient extends NoOpClient {

        int requestCount;

        FakeClusterInfoServiceClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof NodesStatsRequest || request instanceof IndicesStatsRequest) {
                requestCount++;
                // ClusterInfoService handles ClusterBlockExceptions quietly, so we invent such an exception to avoid excess logging
                listener.onFailure(new ClusterBlockException(Set.of(NoMasterBlockService.NO_MASTER_BLOCK_ALL)));
            } else {
                fail("unexpected action: " + action.name());
            }
        }
    }

}
