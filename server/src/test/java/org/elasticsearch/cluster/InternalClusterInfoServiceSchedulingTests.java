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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.ClusterInfoServiceUtils.refresh;
import static org.elasticsearch.cluster.InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

public class InternalClusterInfoServiceSchedulingTests extends ESTestCase {

    public void testScheduling() {
        final DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("test");
        final DiscoveryNodes noMaster = DiscoveryNodes.builder().add(discoveryNode).localNodeId(discoveryNode.getId()).build();
        final DiscoveryNodes localMaster = noMaster.withMasterNodeId(discoveryNode.getId());
        final DiscoveryNode joiner = DiscoveryNodeUtils.create("joiner");
        final DiscoveryNodes withJoiner = DiscoveryNodes.builder(localMaster).add(joiner).build();

        final Settings.Builder settingsBuilder = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), discoveryNode.getName())
            .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), true)
            .put(ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL.getKey(), TimeValue.ZERO)
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                randomBoolean()
                    ? WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
                    : WriteLoadConstraintSettings.WriteLoadDeciderStatus.LOW_THRESHOLD_ONLY
            );
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
        final EstimatedHeapUsageCollector mockEstimatedHeapUsageCollector = spy(new StubEstimatedEstimatedHeapUsageCollector());
        final Map<ShardId, BoostedAndUnboostedCacheSizes> shardCacheSizes = Map.of(
            new ShardId("index", "uuid", 0),
            new BoostedAndUnboostedCacheSizes(10L, 20L)
        );
        final Map<String, CurrentCacheUsage> nodeCacheUsage = Map.of(discoveryNode.getId(), new CurrentCacheUsage(100L, 30L));
        final CacheUsageAndCommitmentCollector mockCacheUsageAndCommitmentCollector = mock(CacheUsageAndCommitmentCollector.class);
        doAnswer(invocation -> {
            final ActionListener<Map<ShardId, BoostedAndUnboostedCacheSizes>> listener = invocation.getArgument(1);
            listener.onResponse(shardCacheSizes);
            return null;
        }).when(mockCacheUsageAndCommitmentCollector).collectShardCacheSizes(any(), any());
        doAnswer(invocation -> {
            final ActionListener<Map<String, CurrentCacheUsage>> listener = invocation.getArgument(1);
            listener.onResponse(nodeCacheUsage);
            return null;
        }).when(mockCacheUsageAndCommitmentCollector).collectNodeCacheUsage(any(), any());
        final NodeUsageStatsForThreadPoolsCollector nodeUsageStatsForThreadPoolsCollector = spy(
            new NodeUsageStatsForThreadPoolsCollector()
        );
        final WriteLoadConstraintSettings writeLoadConstraintSettings = new WriteLoadConstraintSettings(
            clusterService.getClusterSettings()
        );
        final InternalClusterInfoService clusterInfoService = new InternalClusterInfoService(
            settings,
            writeLoadConstraintSettings,
            clusterService,
            threadPool,
            client,
            mockEstimatedHeapUsageCollector,
            mockCacheUsageAndCommitmentCollector,
            nodeUsageStatsForThreadPoolsCollector
        );
        final WriteLoadConstraintMonitor usageMonitor = spy(
            new WriteLoadConstraintMonitor(
                writeLoadConstraintSettings,
                threadPool.relativeTimeInMillisSupplier(),
                clusterService::state,
                new RerouteService() {
                    @Override
                    public void reroute(String reason, Priority priority, ActionListener<Void> listener) {}
                },
                MeterRegistry.NOOP
            )
        );
        clusterInfoService.addListener(usageMonitor::onNewInfo);
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

        // A node joins the cluster
        {
            Mockito.clearInvocations(
                mockEstimatedHeapUsageCollector,
                mockCacheUsageAndCommitmentCollector,
                nodeUsageStatsForThreadPoolsCollector
            );
            final int initialRequestCount = client.requestCount;
            final AtomicBoolean nodeJoined = new AtomicBoolean();
            clusterApplierService.onNewClusterState(
                "node joins",
                () -> ClusterState.builder(new ClusterName("cluster")).nodes(withJoiner).build(),
                setFlagOnSuccess(nodeJoined)
            );
            // Don't use runUntilFlag because we don't want the scheduled task to run
            deterministicTaskQueue.runAllRunnableTasks();
            assertTrue(nodeJoined.get());
            // Addition of node should have triggered refresh
            // should have run two client requests: nodes stats request and indices stats request
            assertThat(client.requestCount, equalTo(initialRequestCount + 2));
            verify(mockEstimatedHeapUsageCollector).collectClusterHeapUsage(any()); // Should have polled for heap usage
            verify(mockEstimatedHeapUsageCollector).collectShardHeapUsage(any());
            verify(mockCacheUsageAndCommitmentCollector).collectShardCacheSizes(any(), any());
            verify(mockCacheUsageAndCommitmentCollector).collectNodeCacheUsage(any(), any());
            verify(nodeUsageStatsForThreadPoolsCollector).collectUsageStats(any(), any(), any());
            assertThat(clusterInfoService.getClusterInfo().getShardCacheSizes(), equalTo(shardCacheSizes));
            assertThat(clusterInfoService.getClusterInfo().getNodeCacheUsage(), equalTo(nodeCacheUsage));
        }

        // ... then leaves
        {
            Mockito.clearInvocations(
                mockEstimatedHeapUsageCollector,
                mockCacheUsageAndCommitmentCollector,
                nodeUsageStatsForThreadPoolsCollector
            );
            final int initialRequestCount = client.requestCount;
            final AtomicBoolean nodeLeft = new AtomicBoolean();
            clusterApplierService.onNewClusterState(
                "node leaves",
                () -> ClusterState.builder(new ClusterName("cluster")).nodes(localMaster).build(),
                setFlagOnSuccess(nodeLeft)
            );
            // Don't use runUntilFlag because we don't want the scheduled task to run
            deterministicTaskQueue.runAllRunnableTasks();
            assertTrue(nodeLeft.get());
            // departing nodes don't trigger refreshes
            assertThat(client.requestCount, equalTo(initialRequestCount));
            verifyNoInteractions(mockEstimatedHeapUsageCollector);
            verifyNoInteractions(mockCacheUsageAndCommitmentCollector);
            verifyNoInteractions(nodeUsageStatsForThreadPoolsCollector);
        }

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
            Mockito.clearInvocations(mockEstimatedHeapUsageCollector);
            Mockito.clearInvocations(mockCacheUsageAndCommitmentCollector);
            Mockito.clearInvocations(nodeUsageStatsForThreadPoolsCollector);
            final int initialRequestCount = client.requestCount;
            final long duration = INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.get(settings).millis();
            runFor(deterministicTaskQueue, duration);
            deterministicTaskQueue.runAllRunnableTasks();
            assertThat(client.requestCount, equalTo(initialRequestCount + 2)); // should have run two client requests per interval
            verify(mockEstimatedHeapUsageCollector).collectClusterHeapUsage(any()); // Should poll for heap usage once per interval
            verify(mockEstimatedHeapUsageCollector).collectShardHeapUsage(any());
            verify(mockCacheUsageAndCommitmentCollector).collectShardCacheSizes(any(), any());
            verify(mockCacheUsageAndCommitmentCollector).collectNodeCacheUsage(any(), any());
            verify(nodeUsageStatsForThreadPoolsCollector).collectUsageStats(any(), any(), any());
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

    public void testEmptyCacheUsageCollector() {
        final PlainActionFuture<Map<ShardId, BoostedAndUnboostedCacheSizes>> shardCacheSizesFuture = new PlainActionFuture<>();
        CacheUsageAndCommitmentCollector.EMPTY.collectShardCacheSizes(ClusterState.EMPTY_STATE, shardCacheSizesFuture);
        assertThat(shardCacheSizesFuture.actionGet(), equalTo(Map.of()));

        final PlainActionFuture<Map<String, CurrentCacheUsage>> nodeCacheUsageFuture = new PlainActionFuture<>();
        CacheUsageAndCommitmentCollector.EMPTY.collectNodeCacheUsage(ClusterState.EMPTY_STATE, nodeCacheUsageFuture);
        assertThat(nodeCacheUsageFuture.actionGet(), equalTo(Map.of()));
    }

    public void testCacheUsageAndCommitmentCollectorFailureFallbacks() {
        final Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.DISABLED
            )
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        final ThreadPool threadPool = deterministicTaskQueue.getThreadPool();

        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, clusterSettings)) {
            final FakeClusterInfoServiceClient client = new FakeClusterInfoServiceClient(threadPool);
            final Map<ShardId, BoostedAndUnboostedCacheSizes> shardCacheSizes = Map.of(
                new ShardId("index", "uuid", 0),
                new BoostedAndUnboostedCacheSizes(10L, 20L)
            );
            final Map<String, CurrentCacheUsage> nodeCacheUsage = Map.of("node-id", new CurrentCacheUsage(100L, 30L));
            final AtomicBoolean failShardCacheSizes = new AtomicBoolean();
            final AtomicBoolean failNodeCacheUsage = new AtomicBoolean();
            final CacheUsageAndCommitmentCollector cacheUsageAndCommitmentCollector = mock(CacheUsageAndCommitmentCollector.class);
            doAnswer(invocation -> {
                final ActionListener<Map<ShardId, BoostedAndUnboostedCacheSizes>> listener = invocation.getArgument(1);
                if (failShardCacheSizes.get()) {
                    listener.onFailure(new IllegalStateException("simulated shard cache sizes failure"));
                } else {
                    listener.onResponse(shardCacheSizes);
                }
                return null;
            }).when(cacheUsageAndCommitmentCollector).collectShardCacheSizes(any(), any());
            doAnswer(invocation -> {
                final ActionListener<Map<String, CurrentCacheUsage>> listener = invocation.getArgument(1);
                if (failNodeCacheUsage.get()) {
                    listener.onFailure(new IllegalStateException("simulated node cache usage failure"));
                } else {
                    listener.onResponse(nodeCacheUsage);
                }
                return null;
            }).when(cacheUsageAndCommitmentCollector).collectNodeCacheUsage(any(), any());

            final InternalClusterInfoService clusterInfoService = new InternalClusterInfoService(
                settings,
                new WriteLoadConstraintSettings(clusterService.getClusterSettings()),
                clusterService,
                threadPool,
                client,
                EstimatedHeapUsageCollector.EMPTY,
                cacheUsageAndCommitmentCollector,
                NodeUsageStatsForThreadPoolsCollector.EMPTY
            );
            clusterInfoService.addListener(ignored -> {});

            failShardCacheSizes.set(true);
            ClusterInfo clusterInfo = refresh(clusterInfoService);
            verify(cacheUsageAndCommitmentCollector).collectShardCacheSizes(any(), any());
            verify(cacheUsageAndCommitmentCollector).collectNodeCacheUsage(any(), any());
            assertThat(clusterInfo.getShardCacheSizes(), equalTo(Map.of()));
            assertThat(clusterInfo.getNodeCacheUsage(), equalTo(nodeCacheUsage));

            Mockito.clearInvocations(cacheUsageAndCommitmentCollector);
            failShardCacheSizes.set(false);
            failNodeCacheUsage.set(true);
            clusterInfo = refresh(clusterInfoService);
            verify(cacheUsageAndCommitmentCollector).collectShardCacheSizes(any(), any());
            verify(cacheUsageAndCommitmentCollector).collectNodeCacheUsage(any(), any());
            assertThat(clusterInfo.getShardCacheSizes(), equalTo(shardCacheSizes));
            assertThat(clusterInfo.getNodeCacheUsage(), equalTo(Map.of()));
        }
    }

    private static class StubEstimatedEstimatedHeapUsageCollector implements EstimatedHeapUsageCollector {

        @Override
        public void collectClusterHeapUsage(ActionListener<Map<String, Long>> listener) {
            listener.onResponse(Map.of());
        }

        @Override
        public void collectShardHeapUsage(ActionListener<ShardHeapUsageEstimates> listener) {
            listener.onResponse(ShardHeapUsageEstimates.empty());
        }
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
