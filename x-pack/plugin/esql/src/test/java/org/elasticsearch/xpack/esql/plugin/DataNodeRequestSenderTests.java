/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.breaker.CircuitBreaker.Durability;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeListener;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeRequest;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.not;
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
    public void shutdownThreadPool() {
        terminate(threadPool);
    }

    public void testEmpty() {
        var future = sendRequests(
            randomBoolean(),
            -1,
            List.of(),
            (node, shardIds, aliasFilters, listener) -> fail("expect no data-node request is sent")
        );
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
        var future = sendRequests(randomBoolean(), -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of())));
        });
        safeGet(future);
        assertThat(sent.size(), equalTo(2));
        assertThat(groupRequests(sent, 2), equalTo(Map.of(node1, List.of(shard1, shard3), node2, List.of(shard2, shard4))));
    }

    public void testMissingShards() {
        {
            var targetShards = List.of(targetShard(shard1, node1), targetShard(shard3), targetShard(shard4, node2, node3));
            var future = sendRequests(false, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
                fail("expect no data-node request is sent when target shards are missing");
            });
            expectThrows(NoShardAvailableActionException.class, containsString("no shard copies found"), future::actionGet);
        }
        {
            var targetShards = List.of(targetShard(shard1, node1), targetShard(shard3), targetShard(shard4, node2, node3));
            var future = sendRequests(true, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
                assertThat(shard3, not(in(shardIds)));
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of())));
            });
            ComputeResponse resp = safeGet(future);
            assertThat(resp.totalShards, equalTo(3));
            assertThat(resp.failedShards, equalTo(1));
            assertThat(resp.successfulShards, equalTo(2));
            assertThat(resp.failures, not(empty()));
            assertNotNull(resp.failures.get(0).shard());
            assertThat(resp.failures.get(0).shard().getShardId(), equalTo(shard3));
            assertThat(resp.failures.get(0).reason(), containsString("no shard copies found"));
        }
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
        var future = sendRequests(randomBoolean(), -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
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
        var future = sendRequests(false, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
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
        var future = sendRequests(false, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
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

    public void testAllowPartialResults() {
        var targetShards = List.of(targetShard(shard1, node1), targetShard(shard2, node2), targetShard(shard3, node1, node2));
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        AtomicBoolean failed = new AtomicBoolean();
        var future = sendRequests(true, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            if (node1.equals(node) && failed.compareAndSet(false, true)) {
                runWithDelay(() -> listener.onFailure(new IOException("test request level failure"), true));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of())));
            }
        });
        ComputeResponse resp = safeGet(future);
        // one round: {node-1, node-2}
        assertThat(sent.size(), equalTo(2));
        var firstRound = groupRequests(sent, 2);
        assertThat(firstRound, equalTo(Map.of(node1, List.of(shard1, shard3), node2, List.of(shard2))));
        assertThat(resp.totalShards, equalTo(3));
        assertThat(resp.failedShards, equalTo(2));
        assertThat(resp.successfulShards, equalTo(1));
    }

    public void testNonFatalErrorIsRetriedOnAnotherShard() {
        var targetShards = List.of(targetShard(shard1, node1, node2));
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var response = safeGet(sendRequests(false, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            if (Objects.equals(node1, node)) {
                runWithDelay(() -> listener.onFailure(new RuntimeException("test request level non fatal failure"), false));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of())));
            }
        }));
        assertThat(response.totalShards, equalTo(1));
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
        assertThat(sent.size(), equalTo(2));
    }

    public void testNonFatalFailedOnAllNodes() {
        var targetShards = List.of(targetShard(shard1, node1, node2));
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var future = sendRequests(false, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            runWithDelay(() -> listener.onFailure(new RuntimeException("test request level non fatal failure"), false));
        });
        expectThrows(RuntimeException.class, equalTo("test request level non fatal failure"), future::actionGet);
        assertThat(sent.size(), equalTo(2));
    }

    public void testDoNotRetryCircuitBreakerException() {
        var targetShards = List.of(targetShard(shard1, node1, node2));
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var future = sendRequests(false, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            runWithDelay(() -> listener.onFailure(new CircuitBreakingException("cbe", randomFrom(Durability.values())), false));
        });
        expectThrows(CircuitBreakingException.class, equalTo("cbe"), future::actionGet);
        assertThat(sent.size(), equalTo(1));
    }

    public void testLimitConcurrentNodes() {
        var targetShards = List.of(
            targetShard(shard1, node1),
            targetShard(shard2, node2),
            targetShard(shard3, node3),
            targetShard(shard4, node4),
            targetShard(shard5, node5)
        );

        var concurrency = randomIntBetween(1, 2);
        AtomicInteger maxConcurrentRequests = new AtomicInteger(0);
        AtomicInteger concurrentRequests = new AtomicInteger(0);
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var response = safeGet(sendRequests(randomBoolean(), concurrency, targetShards, (node, shardIds, aliasFilters, listener) -> {
            concurrentRequests.incrementAndGet();

            while (true) {
                var priorMax = maxConcurrentRequests.get();
                var newMax = Math.max(priorMax, concurrentRequests.get());
                if (newMax <= priorMax || maxConcurrentRequests.compareAndSet(priorMax, newMax)) {
                    break;
                }
            }

            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            runWithDelay(() -> {
                concurrentRequests.decrementAndGet();
                listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of()));
            });
        }));
        assertThat(sent.size(), equalTo(5));
        assertThat(maxConcurrentRequests.get(), equalTo(concurrency));
        assertThat(response.totalShards, equalTo(5));
        assertThat(response.successfulShards, equalTo(5));
        assertThat(response.failedShards, equalTo(0));
    }

    public void testSkipNodes() {
        var targetShards = List.of(
            targetShard(shard1, node1),
            targetShard(shard2, node2),
            targetShard(shard3, node3),
            targetShard(shard4, node4),
            targetShard(shard5, node5)
        );

        AtomicInteger processed = new AtomicInteger(0);
        var response = safeGet(sendRequests(randomBoolean(), 1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            runWithDelay(() -> {
                if (processed.incrementAndGet() == 1) {
                    listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of()));
                } else {
                    listener.onSkip();
                }
            });
        }));
        assertThat(response.totalShards, equalTo(5));
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.skippedShards, equalTo(4));
        assertThat(response.failedShards, equalTo(0));
    }

    public void testSkipRemovesPriorNonFatalErrors() {
        var targetShards = List.of(targetShard(shard1, node1, node2), targetShard(shard2, node3));

        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var response = safeGet(sendRequests(randomBoolean(), 1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(new NodeRequest(node, shardIds, aliasFilters));
            runWithDelay(() -> {
                if (Objects.equals(node.getId(), node1.getId()) && shardIds.equals(List.of(shard1))) {
                    listener.onFailure(new RuntimeException("test request level non fatal failure"), false);
                } else if (Objects.equals(node.getId(), node3.getId()) && shardIds.equals(List.of(shard2))) {
                    listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of()));
                } else if (Objects.equals(node.getId(), node2.getId()) && shardIds.equals(List.of(shard1))) {
                    listener.onSkip();
                }
            });
        }));
        assertThat(sent.size(), equalTo(3));
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.skippedShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
    }

    public void testRetryMovedShard() {
        var attempt = new AtomicInteger(0);
        var response = safeGet(
            sendRequests(randomBoolean(), -1, List.of(targetShard(shard1, node1)), shardIds -> switch (attempt.incrementAndGet()) {
                case 1 -> Map.of(shard1, List.of(node2));
                case 2 -> Map.of(shard1, List.of(node3));
                default -> Map.of(shard1, List.of(node4));
            },
                (node, shardIds, aliasFilters, listener) -> runWithDelay(
                    () -> listener.onResponse(
                        Objects.equals(node, node4)
                            ? new DataNodeComputeResponse(List.of(), Map.of())
                            : new DataNodeComputeResponse(List.of(), Map.of(shard1, new ShardNotFoundException(shard1)))
                    )
                )
            )
        );
        assertThat(response.totalShards, equalTo(1));
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.skippedShards, equalTo(0));
        assertThat(response.failedShards, equalTo(0));
        assertThat(attempt.get(), equalTo(3));
    }

    public void testRetryMultipleMovedShards() {
        var attempt = new AtomicInteger(0);
        var response = safeGet(
            sendRequests(
                randomBoolean(),
                -1,
                List.of(targetShard(shard1, node1), targetShard(shard2, node2), targetShard(shard3, node3)),
                shardIds -> shardIds.stream().collect(toMap(Function.identity(), shardId -> List.of(randomFrom(node1, node2, node3)))),
                (node, shardIds, aliasFilters, listener) -> runWithDelay(
                    () -> listener.onResponse(
                        attempt.incrementAndGet() <= 6
                            ? new DataNodeComputeResponse(
                                List.of(),
                                shardIds.stream().collect(toMap(Function.identity(), ShardNotFoundException::new))
                            )
                            : new DataNodeComputeResponse(List.of(), Map.of())
                    )
                )
            )
        );
        assertThat(response.totalShards, equalTo(3));
        assertThat(response.successfulShards, equalTo(3));
        assertThat(response.skippedShards, equalTo(0));
        assertThat(response.failedShards, equalTo(0));
    }

    public void testDoesNotRetryMovedShardIndefinitely() {
        var attempt = new AtomicInteger(0);
        var response = safeGet(sendRequests(true, -1, List.of(targetShard(shard1, node1)), shardIds -> {
            attempt.incrementAndGet();
            return Map.of(shard1, List.of(node2));
        },
            (node, shardIds, aliasFilters, listener) -> runWithDelay(
                () -> listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of(shard1, new ShardNotFoundException(shard1))))
            )
        ));
        assertThat(response.totalShards, equalTo(1));
        assertThat(response.successfulShards, equalTo(0));
        assertThat(response.skippedShards, equalTo(0));
        assertThat(response.failedShards, equalTo(1));
        assertThat(attempt.get(), equalTo(10));
    }

    public void testRetryOnlyMovedShards() {
        var attempt = new AtomicInteger(0);
        var resolvedShards = Collections.synchronizedSet(new HashSet<>());
        var response = safeGet(
            sendRequests(randomBoolean(), -1, List.of(targetShard(shard1, node1, node3), targetShard(shard2, node2)), shardIds -> {
                attempt.incrementAndGet();
                resolvedShards.addAll(shardIds);
                return Map.of(shard2, List.of(node4));
            }, (node, shardIds, aliasFilters, listener) -> runWithDelay(() -> {
                if (Objects.equals(node, node1)) {
                    // search is going to be retried from replica on node3 without shard resolution
                    listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of(shard1, new ShardNotFoundException(shard1))));
                } else if (Objects.equals(node, node2)) {
                    // search is going to be retried after resolving new shard node since there are no replicas
                    listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of(shard2, new ShardNotFoundException(shard2))));
                } else {
                    listener.onResponse(new DataNodeComputeResponse(List.of(), Map.of()));
                }
            }))
        );
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.successfulShards, equalTo(2));
        assertThat(response.skippedShards, equalTo(0));
        assertThat(response.failedShards, equalTo(0));
        assertThat(attempt.get(), equalTo(1));
        assertThat("Must retry only affected shards", resolvedShards, contains(shard2));
    }

    public void testRetryUnassignedShardWithoutPartialResults() {
        var attempt = new AtomicInteger(0);
        var future = sendRequests(false, -1, List.of(targetShard(shard1, node1), targetShard(shard2, node2)), shardIds -> {
            attempt.incrementAndGet();
            return Map.of(shard1, List.of());
        },
            (node, shardIds, aliasFilters, listener) -> runWithDelay(
                () -> listener.onResponse(
                    Objects.equals(shardIds, List.of(shard2))
                        ? new DataNodeComputeResponse(List.of(), Map.of())
                        : new DataNodeComputeResponse(List.of(), Map.of(shard1, new ShardNotFoundException(shard1)))
                )
            )

        );
        expectThrows(NoShardAvailableActionException.class, containsString("no such shard"), future::actionGet);
        assertThat(attempt.get(), equalTo(1));
    }

    public void testRetryUnassignedShardWithPartialResults() {
        var attempt = new AtomicInteger(0);
        var response = safeGet(sendRequests(true, -1, List.of(targetShard(shard1, node1), targetShard(shard2, node2)), shardIds -> {
            attempt.incrementAndGet();
            return Map.of(shard1, List.of());
        },
            (node, shardIds, aliasFilters, listener) -> runWithDelay(
                () -> listener.onResponse(
                    Objects.equals(shardIds, List.of(shard2))
                        ? new DataNodeComputeResponse(List.of(), Map.of())
                        : new DataNodeComputeResponse(List.of(), Map.of(shard1, new ShardNotFoundException(shard1)))
                )
            )
        ));
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.skippedShards, equalTo(0));
        assertThat(response.failedShards, equalTo(1));
        assertThat(attempt.get(), equalTo(1));
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

    PlainActionFuture<ComputeResponse> sendRequests(
        boolean allowPartialResults,
        int concurrentRequests,
        List<DataNodeRequestSender.TargetShard> shards,
        Sender sender
    ) {
        return sendRequests(allowPartialResults, concurrentRequests, shards, shardIds -> {
            throw new AssertionError("No shard resolution is expected here");
        }, sender);
    }

    PlainActionFuture<ComputeResponse> sendRequests(
        boolean allowPartialResults,
        int concurrentRequests,
        List<DataNodeRequestSender.TargetShard> shards,
        Resolver resolver,
        Sender sender
    ) {
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
        DataNodeRequestSender requestSender = new DataNodeRequestSender(
            null,
            transportService,
            executor,
            task,
            new OriginalIndices(new String[0], SearchRequest.DEFAULT_INDICES_OPTIONS),
            null,
            "",
            allowPartialResults,
            concurrentRequests,
            10
        ) {
            @Override
            void searchShards(Set<String> concreteIndices, ActionListener<TargetShards> listener) {
                runWithDelay(
                    () -> listener.onResponse(
                        new TargetShards(shards.stream().collect(toMap(TargetShard::shardId, Function.identity())), shards.size(), 0)
                    )
                );
            }

            @Override
            Map<ShardId, List<DiscoveryNode>> resolveShards(Set<ShardId> shardIds) {
                return resolver.resolve(shardIds);
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
        requestSender.startComputeOnDataNodes(Set.of(randomAlphaOfLength(10)), () -> {}, future);
        return future;
    }

    interface Resolver {
        Map<ShardId, List<DiscoveryNode>> resolve(Set<ShardId> shardIds);
    }

    interface Sender {
        void sendRequestToOneNode(DiscoveryNode node, List<ShardId> shardIds, Map<Index, AliasFilter> aliasFilters, NodeListener listener);
    }
}
