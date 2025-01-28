/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataNodeRequestSenderTests extends ComputeTestCase {

    private TestThreadPool threadPool;
    private Executor executor = null;
    private static final String ESQL_TEST_EXECUTOR = "esql_test_executor";

    private final DiscoveryNode node1 = DiscoveryNodeUtils.create("node-1");
    private final DiscoveryNode node2 = DiscoveryNodeUtils.create("node-2");
    private final DiscoveryNode node3 = DiscoveryNodeUtils.create("node-3");
    private final DiscoveryNode node4 = DiscoveryNodeUtils.create("node-4");
    private final DiscoveryNode node5 = DiscoveryNodeUtils.create("node-5");
    private final ShardId shard1 = new ShardId("index", "n/a", 1);
    private final ShardId shard2 = new ShardId("index", "n/a", 2);
    private final ShardId shard3 = new ShardId("index", "n/a", 3);
    private final ShardId shard4 = new ShardId("index", "n/a", 4);
    private final ShardId shard5 = new ShardId("index", "n/a", 5);

    @Before
    public void setThreadPool() {
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        threadPool = new TestThreadPool(
            "test",
            new FixedExecutorBuilder(Settings.EMPTY, ESQL_TEST_EXECUTOR, numThreads, 1024, "esql", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        executor = threadPool.executor(ESQL_TEST_EXECUTOR);
    }

    @After
    public void shutdownThreadPool() throws Exception {
        terminate(threadPool);
    }

    public void testEmpty() {
        var future = sendRequests(List.of(), (node, shardIds, aliasFilters, listener) -> fail("expect no data-node request is sent"));
        var resp = safeGet(future);
        assertThat(resp.totalShards, equalTo(0));
    }

    public void testOnePass() {
        var targetShards = List.of(
            targetShard(shard1, node1),
            targetShard(shard2, node2, node4),
            targetShard(shard3, node1, node2),
            targetShard(shard4, node2, node3)
        );
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        var future = sendRequests(targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            var resp = new DataNodeComputeResponse(List.of(), Map.of());
            runWithDelay(() -> listener.onResponse(resp));
        });
        safeGet(future);
        assertThat(sent.size(), equalTo(2));
        assertThat(groupRequests(sent, 2), equalTo(Map.of(node1, List.of(shard1, shard3), node2, List.of(shard2, shard4))));
    }

    public void testMissingShards() {
        var targetShards = List.of(targetShard(shard1, node1), targetShard(shard3), targetShard(shard4, node2, node3));
        var future = sendRequests(targetShards, (node, shardIds, aliasFilters, listener) -> {
            fail("expect no data-node request is sent when target shards are missing");
        });
        var error = expectThrows(ShardNotFoundException.class, future::actionGet);
        assertThat(error.getMessage(), containsString("no shard copies found"));
    }

    public void testRetryThenSuccess() {
        var targetShards = List.of(
            targetShard(shard1, node1),
            targetShard(shard2, node4, node2),
            targetShard(shard3, node2, node3),
            targetShard(shard4, node2, node3),
            targetShard(shard5, node1, node3, node2)
        );
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        var future = sendRequests(targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            Map<ShardId, Exception> failures = new HashMap<>();
            if (node.equals(node1) && shardIds.contains(shard5)) {
                failures.put(shard5, new IOException("test"));
            }
            if (node.equals(node4) && shardIds.contains(shard2)) {
                failures.put(shard2, new IOException("test"));
            }
            runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(List.of(), failures)));
        });
        try {
            future.actionGet(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertThat(sent, hasSize(5));
        var firstRound = groupRequests(sent, 3);
        assertThat(firstRound, equalTo(Map.of(node1, List.of(shard1, shard5), node4, List.of(shard2), node2, List.of(shard3, shard4))));
        var secondRound = groupRequests(sent, 2);
        assertThat(secondRound, equalTo(Map.of(node2, List.of(shard2), node3, List.of(shard5))));
    }

    public void testRetryButFail() {
        var targetShards = List.of(
            targetShard(shard1, node1),
            targetShard(shard2, node4, node2),
            targetShard(shard3, node2, node3),
            targetShard(shard4, node2, node3),
            targetShard(shard5, node1, node3, node2)
        );
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        var future = sendRequests(targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            Map<ShardId, Exception> failures = new HashMap<>();
            if (shardIds.contains(shard5)) {
                failures.put(shard5, new IOException("test failure for shard5"));
            }
            runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(List.of(), failures)));
        });
        var error = expectThrows(Exception.class, future::actionGet);
        assertNotNull(ExceptionsHelper.unwrap(error, IOException.class));
        // {node-1, node-2, node-4}, {node-3}, {node-2}
        assertThat(sent.size(), equalTo(5));
        var firstRound = groupRequests(sent, 3);
        assertThat(firstRound, equalTo(Map.of(node1, List.of(shard1, shard5), node2, List.of(shard3, shard4), node4, List.of(shard2))));
        NodeRequest fourth = sent.remove();
        assertThat(fourth.node(), equalTo(node3));
        assertThat(fourth.shardIds(), equalTo(List.of(shard5)));
        NodeRequest fifth = sent.remove();
        assertThat(fifth.node(), equalTo(node2));
        assertThat(fifth.shardIds(), equalTo(List.of(shard5)));
    }

    public void testDoNotRetryOnRequestLevelFailure() {
        var targetShards = List.of(targetShard(shard1, node1), targetShard(shard2, node2), targetShard(shard3, node1));
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        AtomicBoolean failed = new AtomicBoolean();
        var future = sendRequests(targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            if (node1.equals(node) && failed.compareAndSet(false, true)) {
                runWithDelay(() -> listener.onFailure(new IOException("test request level failure"), true));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of())));
            }
        });
        Exception exception = expectThrows(Exception.class, future::actionGet);
        assertNotNull(ExceptionsHelper.unwrap(exception, IOException.class));
        // one round: {node-1, node-2}
        assertThat(sent.size(), equalTo(2));
        var firstRound = groupRequests(sent, 2);
        assertThat(firstRound, equalTo(Map.of(node1, List.of(shard1, shard3), node2, List.of(shard2))));
    }

    static DataNodeRequestSender.TargetShard targetShard(ShardId shardId, DiscoveryNode... nodes) {
        return new DataNodeRequestSender.TargetShard(shardId, new ArrayList<>(Arrays.asList(nodes)), null);
    }

    static Map<DiscoveryNode, List<ShardId>> groupRequests(Queue<NodeRequest> sent, int limit) {
        Map<DiscoveryNode, List<ShardId>> map = new HashMap<>();
        for (int i = 0; i < limit; i++) {
            NodeRequest r = sent.remove();
            assertNull(map.put(r.node(), r.shardIds().stream().sorted().toList()));
        }
        return map;
    }

    void runWithDelay(Runnable runnable) {
        if (randomBoolean()) {
            threadPool.schedule(runnable, TimeValue.timeValueNanos(between(0, 5000)), executor);
        } else {
            executor.execute(runnable);
        }
    }

    PlainActionFuture<ComputeResponse> sendRequests(List<DataNodeRequestSender.TargetShard> shards, Sender sender) {
        PlainActionFuture<ComputeResponse> future = new PlainActionFuture<>();
        TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(threadPool);
        CancellableTask task = new CancellableTask(
            randomNonNegativeLong(),
            "type",
            "action",
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
        DataNodeRequestSender requestSender = new DataNodeRequestSender(transportService, executor, task) {
            @Override
            void searchShards(
                Task parentTask,
                String clusterAlias,
                QueryBuilder filter,
                Set<String> concreteIndices,
                OriginalIndices originalIndices,
                ActionListener<TargetShards> listener
            ) {
                var targetShards = new TargetShards(
                    shards.stream().collect(Collectors.toMap(TargetShard::shardId, Function.identity())),
                    shards.size(),
                    0
                );
                assertSame(parentTask, task);
                runWithDelay(() -> listener.onResponse(targetShards));
            }

            @Override
            protected void sendRequest(
                DiscoveryNode node,
                List<ShardId> shardIds,
                Map<Index, AliasFilter> aliasFilters,
                NodeListener listener
            ) {
                sender.sendRequestToOneNode(node, shardIds, aliasFilters, listener);
            }
        };
        requestSender.startComputeOnDataNodes(
            "",
            Set.of(randomAlphaOfLength(10)),
            new OriginalIndices(new String[0], SearchRequest.DEFAULT_INDICES_OPTIONS),
            null,
            () -> {},
            future
        );
        return future;
    }

    interface Sender {
        void sendRequestToOneNode(
            DiscoveryNode node,
            List<ShardId> shardIds,
            Map<Index, AliasFilter> aliasFilters,
            DataNodeRequestSender.NodeListener listener
        );
    }
}
