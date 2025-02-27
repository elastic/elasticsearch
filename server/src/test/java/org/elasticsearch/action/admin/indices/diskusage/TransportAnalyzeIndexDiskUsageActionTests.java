/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.diskusage;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportAnalyzeIndexDiskUsageActionTests extends ESTestCase {

    private static ThreadPool threadPool;

    @Before
    public void setUpThreadPool() throws Exception {
        threadPool = new TestThreadPool("test");
    }

    @After
    public void shutdownThreadPool() throws Exception {
        TestThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testSimpleLimitRequests() throws Exception {
        DiscoveryNodes nodes = newNodes(between(1, 16));
        int numberOfShards = randomIntBetween(1, 100);
        Map<DiscoveryNode, Queue<ShardRouting>> nodeToShards = new HashMap<>();
        Map<ShardId, List<ShardRouting>> groupShardRoutings = new HashMap<>();
        for (int i = 0; i < numberOfShards; i++) {
            ShardId shardId = new ShardId("test_index", "n/a", i);
            DiscoveryNode node = randomFrom(nodes.getAllNodes());
            ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, node.getId(), randomBoolean(), ShardRoutingState.STARTED);
            groupShardRoutings.put(shardId, List.of(shardRouting));
            nodeToShards.computeIfAbsent(node, k -> new LinkedList<>()).add(shardRouting);
        }
        TestTransportService transportService = new TestTransportService(threadPool, r -> {});
        ClusterService clusterService = mockClusterService(ClusterState.builder(ClusterState.EMPTY_STATE).nodes(nodes).build());
        TransportAnalyzeIndexDiskUsageAction transportAction = createTransportAction(clusterService, transportService, groupShardRoutings);
        int maxConcurrentRequests = randomIntBetween(1, 5);
        PlainActionFuture<AnalyzeIndexDiskUsageResponse> future = new PlainActionFuture<>();
        Task task = new Task(randomLong(), "transport", "action", "", null, emptyMap());
        TransportAnalyzeIndexDiskUsageAction.LimitingRequestPerNodeBroadcastAction broadcastAction =
            transportAction.new LimitingRequestPerNodeBroadcastAction(task, randomDiskUsageRequest(), future, maxConcurrentRequests);
        broadcastAction.start();

        Map<DiscoveryNode, Integer> expectedRequestCounts = new HashMap<>();
        for (Map.Entry<DiscoveryNode, Queue<ShardRouting>> e : nodeToShards.entrySet()) {
            Queue<ShardRouting> shards = e.getValue();
            int sentRequests = Math.min(shards.size(), maxConcurrentRequests);
            expectedRequestCounts.put(e.getKey(), sentRequests);
            for (int i = 0; i < sentRequests; i++) {
                shards.remove();
            }
        }
        assertThat(transportService.getRequestsSentPerNode(), equalTo(expectedRequestCounts));
        expectedRequestCounts.clear();
        final AtomicLong totalIndexSizeInBytes = new AtomicLong();
        final List<CapturingRequest> pendingRequests = new ArrayList<>(transportService.getCapturedRequests(true));
        while (pendingRequests.isEmpty() == false) {
            expectedRequestCounts.clear();
            List<CapturingRequest> toReply = randomSubsetOf(pendingRequests);
            for (CapturingRequest r : toReply) {
                long shardSize = between(1, Integer.MAX_VALUE);
                totalIndexSizeInBytes.addAndGet(shardSize);
                r.sendRandomResponse(shardSize, randomBoolean());
                pendingRequests.remove(r);
                if (nodeToShards.get(r.node).poll() != null) {
                    expectedRequestCounts.compute(r.node, (k, v) -> v == null ? 1 : v + 1);
                }
            }
            assertBusy(() -> assertThat(transportService.getRequestsSentPerNode(), equalTo(expectedRequestCounts)));
            pendingRequests.addAll(transportService.getCapturedRequests(true));
        }
        AnalyzeIndexDiskUsageResponse response = future.actionGet();
        assertThat(response.getTotalShards(), equalTo(numberOfShards));
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(response.getSuccessfulShards(), equalTo(numberOfShards));
        assertThat(response.getStats().get("test_index").getIndexSizeInBytes(), equalTo(totalIndexSizeInBytes.get()));
    }

    public void testRandomLimitConcurrentRequests() throws Exception {
        DiscoveryNodes nodes = newNodes(between(1, 20));
        int numberOfShards = randomIntBetween(1, 1000);
        Map<ShardId, List<ShardRouting>> shardToRoutings = new HashMap<>();
        for (int i = 0; i < numberOfShards; i++) {
            ShardId shardId = new ShardId("test_index", "n/a", i);
            List<ShardRouting> shardRoutings = randomSubsetOf(between(1, nodes.size()), nodes.getAllNodes()).stream()
                .map(node -> TestShardRouting.newShardRouting(shardId, node.getId(), randomBoolean(), ShardRoutingState.STARTED))
                .toList();
            shardToRoutings.put(shardId, shardRoutings);
        }
        Set<ShardId> failedShards = new HashSet<>(randomSubsetOf(between(0, (numberOfShards + 4) / 5), shardToRoutings.keySet()));
        int maxConcurrentRequests = randomIntBetween(1, 16);
        PlainActionFuture<AnalyzeIndexDiskUsageResponse> requestFuture = new PlainActionFuture<>();
        Queue<CapturingRequest> pendingRequests = ConcurrentCollections.newQueue();
        Semaphore availableRequests = new Semaphore(0);
        AtomicBoolean stopped = new AtomicBoolean();
        TestTransportService transportService = new TestTransportService(threadPool, r -> {
            pendingRequests.add(r);
            availableRequests.release();
        });
        final AtomicLong totalIndexSize = new AtomicLong();
        final Thread handlingThread = new Thread(() -> {
            Map<ShardId, Integer> shardIdToRounds = ConcurrentCollections.newConcurrentMap();
            while (stopped.get() == false && requestFuture.isDone() == false) {
                try {
                    if (availableRequests.tryAcquire(10, TimeUnit.MILLISECONDS) == false) {
                        continue;
                    }
                    if (randomBoolean()) {
                        // make sure we never have more max_concurrent_requests outstanding requests on each node
                        Map<DiscoveryNode, Integer> perNode = new HashMap<>();
                        for (CapturingRequest r : pendingRequests) {
                            int count = perNode.compute(r.node, (k, v) -> v == null ? 1 : v + 1);
                            assertThat(count, lessThanOrEqualTo(maxConcurrentRequests));
                        }
                    }
                    final List<CapturingRequest> readyRequests = randomSubsetOf(between(1, pendingRequests.size()), pendingRequests);
                    pendingRequests.removeAll(readyRequests);
                    availableRequests.acquireUninterruptibly(readyRequests.size() - 1);
                    for (CapturingRequest r : readyRequests) {
                        ShardId shardId = r.request.shardId();
                        int round = shardIdToRounds.compute(shardId, (k, curr) -> curr == null ? 1 : curr + 1);
                        int maxRound = shardToRoutings.get(shardId).size();
                        if (failedShards.contains(shardId) || (round < maxRound && randomBoolean())) {
                            r.sendRandomFailure(randomBoolean());
                        } else {
                            long shardSize = between(1, Integer.MAX_VALUE);
                            totalIndexSize.addAndGet(shardSize);
                            r.sendRandomResponse(shardSize, randomBoolean());
                        }
                    }
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
            }
        });
        handlingThread.start();
        ClusterService clusterService = mockClusterService(ClusterState.builder(ClusterState.EMPTY_STATE).nodes(nodes).build());
        TransportAnalyzeIndexDiskUsageAction transportAction = createTransportAction(clusterService, transportService, shardToRoutings);
        Task task = new Task(randomLong(), "transport", "action", "", null, emptyMap());
        TransportAnalyzeIndexDiskUsageAction.LimitingRequestPerNodeBroadcastAction broadcastAction =
            transportAction.new LimitingRequestPerNodeBroadcastAction(task, randomDiskUsageRequest(), requestFuture, maxConcurrentRequests);
        broadcastAction.start();
        try {
            AnalyzeIndexDiskUsageResponse response = requestFuture.actionGet(TimeValue.timeValueSeconds(30));
            assertThat(response.getTotalShards(), equalTo(numberOfShards));
            assertThat(response.getFailedShards(), equalTo(failedShards.size()));
            assertThat(response.getSuccessfulShards(), equalTo(numberOfShards - failedShards.size()));
            if (numberOfShards == failedShards.size()) {
                assertTrue(response.getStats().isEmpty());
                assertThat(totalIndexSize.get(), equalTo(0L));
            } else {
                assertThat(response.getStats().get("test_index").getIndexSizeInBytes(), equalTo(totalIndexSize.get()));
            }
        } finally {
            stopped.set(true);
            handlingThread.join();
        }
    }

    /**
     * Make sure that we don't hit StackOverflow if responses are replied on the same thread.
     */
    public void testManyShards() {
        DiscoveryNodes discoNodes = newNodes(10);
        int numberOfShards = randomIntBetween(200, 10000);
        Map<ShardId, List<ShardRouting>> shardToRoutings = new HashMap<>();
        for (int i = 0; i < numberOfShards; i++) {
            ShardId shardId = new ShardId("test_index", "n/a", i);
            List<ShardRouting> shardRoutings = randomSubsetOf(between(1, discoNodes.size()), discoNodes.getAllNodes()).stream()
                .map(node -> TestShardRouting.newShardRouting(shardId, node.getId(), randomBoolean(), ShardRoutingState.STARTED))
                .toList();
            shardToRoutings.put(shardId, shardRoutings);
        }
        Set<ShardId> successfulShards = new HashSet<>(randomSubsetOf(between(0, (numberOfShards + 4) / 5), shardToRoutings.keySet()));
        final AtomicLong totalIndexSize = new AtomicLong();
        boolean maybeFork = randomBoolean();
        Map<ShardId, Integer> shardIdToRounds = ConcurrentCollections.newConcurrentMap();
        TestTransportService transportService = new TestTransportService(threadPool, r -> {
            ShardId shardId = r.request.shardId();
            int round = shardIdToRounds.compute(shardId, (k, curr) -> curr == null ? 1 : curr + 1);
            int maxRound = shardToRoutings.get(shardId).size();
            if (successfulShards.contains(shardId) == false || (round < maxRound && randomBoolean())) {
                r.sendRandomFailure(maybeFork);
            } else {
                long shardSize = between(0, Integer.MAX_VALUE);
                totalIndexSize.addAndGet(shardSize);
                r.sendRandomResponse(shardSize, maybeFork);
            }
        });
        ClusterService clusterService = mockClusterService(ClusterState.builder(ClusterState.EMPTY_STATE).nodes(discoNodes).build());
        TransportAnalyzeIndexDiskUsageAction transportAction = createTransportAction(clusterService, transportService, shardToRoutings);
        int maxConcurrentRequests = randomIntBetween(1, 16);
        PlainActionFuture<AnalyzeIndexDiskUsageResponse> future = new PlainActionFuture<>();
        Task task = new Task(randomLong(), "transport", "action", "", null, emptyMap());
        TransportAnalyzeIndexDiskUsageAction.LimitingRequestPerNodeBroadcastAction broadcastAction =
            transportAction.new LimitingRequestPerNodeBroadcastAction(task, randomDiskUsageRequest(), future, maxConcurrentRequests);
        broadcastAction.start();
        AnalyzeIndexDiskUsageResponse resp = future.actionGet();
        assertThat(resp.getTotalShards(), equalTo(numberOfShards));
        assertThat(resp.getSuccessfulShards(), equalTo(successfulShards.size()));
        if (successfulShards.isEmpty()) {
            assertTrue(resp.getStats().isEmpty());
        } else {
            assertThat(resp.getStats().get("test_index").getIndexSizeInBytes(), equalTo(totalIndexSize.get()));
        }
    }

    private static DiscoveryNodes newNodes(int numNodes) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < numNodes; i++) {
            nodes.add(DiscoveryNodeUtils.builder("node_" + i).roles(emptySet()).build());
        }
        return nodes.localNodeId("node_0").build();

    }

    private static AnalyzeIndexDiskUsageRequest randomDiskUsageRequest(String... indices) {
        return new AnalyzeIndexDiskUsageRequest(indices, BroadcastRequest.DEFAULT_INDICES_OPTIONS, randomBoolean());
    }

    private TransportAnalyzeIndexDiskUsageAction createTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        Map<ShardId, List<ShardRouting>> targetShards
    ) {
        return new TransportAnalyzeIndexDiskUsageAction(
            clusterService,
            transportService,
            mock(IndicesService.class),
            new ActionFilters(new HashSet<>()),
            TestProjectResolvers.DEFAULT_PROJECT_ONLY,
            new IndexNameExpressionResolver(
                new ThreadContext(Settings.EMPTY),
                EmptySystemIndices.INSTANCE,
                TestProjectResolvers.DEFAULT_PROJECT_ONLY
            ) {
                @Override
                public String[] concreteIndexNames(ProjectMetadata project, IndicesRequest request) {
                    return request.indices();
                }
            }
        ) {
            @Override
            protected List<ShardIterator> shards(
                ClusterState clusterState,
                AnalyzeIndexDiskUsageRequest request,
                String[] concreteIndices
            ) {
                final List<ShardIterator> shardIterators = new ArrayList<>(targetShards.size());
                for (Map.Entry<ShardId, List<ShardRouting>> e : targetShards.entrySet()) {
                    shardIterators.add(new ShardIterator(e.getKey(), e.getValue()));
                }
                return shardIterators;
            }
        };
    }

    private ClusterService mockClusterService(ClusterState clusterState) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.localNode()).thenReturn(clusterState.nodes().getLocalNode());
        return clusterService;
    }

    private record CapturingRequest(
        DiscoveryNode node,
        AnalyzeDiskUsageShardRequest request,
        TransportResponseHandler<AnalyzeDiskUsageShardResponse> handler
    ) {

        void sendRandomResponse(long sizeInBytes, boolean maybeFork) {
            AnalyzeDiskUsageShardResponse shardResponse = new AnalyzeDiskUsageShardResponse(
                request.shardId(),
                new IndexDiskUsageStats(sizeInBytes)
            );
            if (maybeFork && randomBoolean()) {
                threadPool.generic().execute(() -> handler.handleResponse(shardResponse));
            } else {
                handler.handleResponse(shardResponse);
            }
        }

        void sendRandomFailure(boolean maybeFork) {
            TransportException e = new TransportException(new NodeDisconnectedException(node, "disconnected"));
            if (maybeFork && randomBoolean()) {
                threadPool.generic().execute(() -> handler.handleException(e));
            } else {
                handler.handleException(e);
            }
        }
    }

    static class TestTransportService extends TransportService {
        private final Queue<CapturingRequest> capturedRequests = ConcurrentCollections.newQueue();
        private final Consumer<CapturingRequest> onRequestSent;

        TestTransportService(ThreadPool threadPool, Consumer<CapturingRequest> onRequestSent) {
            super(
                Settings.EMPTY,
                new MockTransport(),
                threadPool,
                TransportService.NOOP_TRANSPORT_INTERCEPTOR,
                addr -> DiscoveryNodeUtils.builder("node_0").roles(emptySet()).build(),
                null,
                Collections.emptySet()
            );
            this.onRequestSent = onRequestSent;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T extends TransportResponse> void sendRequest(
            DiscoveryNode node,
            String action,
            TransportRequest request,
            TransportResponseHandler<T> handler
        ) {
            CapturingRequest capturingRequest = new CapturingRequest(
                node,
                (AnalyzeDiskUsageShardRequest) request,
                (TransportResponseHandler<AnalyzeDiskUsageShardResponse>) handler
            );
            capturedRequests.add(capturingRequest);
            onRequestSent.accept(capturingRequest);
        }

        List<CapturingRequest> getCapturedRequests(boolean clear) {
            final List<CapturingRequest> requests = new ArrayList<>(capturedRequests);
            if (clear) {
                capturedRequests.clear();
            }
            return requests;
        }

        Map<DiscoveryNode, Integer> getRequestsSentPerNode() {
            Map<DiscoveryNode, Integer> sentRequests = new HashMap<>();
            for (CapturingRequest r : getCapturedRequests(false)) {
                sentRequests.compute(r.node, (k, v) -> v == null ? 1 : v + 1);
            }
            return sentRequests;
        }
    }
}
