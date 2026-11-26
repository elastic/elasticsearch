/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.breaker.CircuitBreaker.Durability;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.SliceSelection;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.scheduling.SearchLoadMetrics;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeFailure;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeListener;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_COLD_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_WARM_NODE_ROLE;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeRequest;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class DataNodeRequestSenderTests extends ComputeTestCase {

    /** The resolution attempts of a sender, unless a test sets them. */
    private static final int DEFAULT_RESOLUTION_ATTEMPTS = 10;

    private ThreadPool threadPool;
    private Executor executor = null;

    private final DiscoveryNode node1 = DiscoveryNodeUtils.builder("node-1").roles(Set.of(DATA_HOT_NODE_ROLE)).build();
    private final DiscoveryNode node2 = DiscoveryNodeUtils.builder("node-2").roles(Set.of(DATA_HOT_NODE_ROLE)).build();
    private final DiscoveryNode node3 = DiscoveryNodeUtils.builder("node-3").roles(Set.of(DATA_HOT_NODE_ROLE)).build();
    private final DiscoveryNode node4 = DiscoveryNodeUtils.builder("node-4").roles(Set.of(DATA_HOT_NODE_ROLE)).build();
    private final DiscoveryNode node5 = DiscoveryNodeUtils.builder("node-5").roles(Set.of(DATA_HOT_NODE_ROLE)).build();
    private final DataNodeRequest.Shard shard1 = new DataNodeRequest.Shard(new ShardId("index", "n/a", 1), SplitShardCountSummary.UNSET);
    private final DataNodeRequest.Shard shard2 = new DataNodeRequest.Shard(new ShardId("index", "n/a", 2), SplitShardCountSummary.UNSET);
    private final DataNodeRequest.Shard shard3 = new DataNodeRequest.Shard(
        new ShardId("index", "n/a", 3),
        SplitShardCountSummary.fromInt(3)
    );
    private final DataNodeRequest.Shard shard4 = new DataNodeRequest.Shard(new ShardId("index", "n/a", 4), SplitShardCountSummary.UNSET);
    private final DataNodeRequest.Shard shard5 = new DataNodeRequest.Shard(new ShardId("index", "n/a", 5), SplitShardCountSummary.UNSET);

    @Before
    public void setThreadPool() {
        int numThreads = randomBoolean() ? 1 : between(2, 16);
        var settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "TransportActionTests").build();
        threadPool = new ThreadPool(
            new ThreadContext(settings),
            settings,
            MeterRegistry.NOOP,
            (s, allocatedProcessors) -> Map.of(),
            new FixedExecutorBuilder(Settings.EMPTY, ThreadPool.Names.SEARCH, numThreads, 1024, "", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        executor = threadPool.executor(ThreadPool.Names.SEARCH);
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
        var future = sendRequests(randomBoolean(), -1, targetShards, (node, shards, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shards));
            runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
        });
        safeGet(future);
        assertThat(sent.size(), equalTo(2));
        assertThat(sent, containsInAnyOrder(nodeRequest(node1, shard1, shard3), nodeRequest(node2, shard2, shard4)));
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
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            });
            ComputeResponse resp = safeGet(future);
            assertThat(resp.totalShards, equalTo(3));
            assertThat(resp.failedShards, equalTo(1));
            assertThat(resp.successfulShards, equalTo(2));
            assertThat(resp.failures, not(empty()));
            assertNotNull(resp.failures.get(0).shard());
            assertThat(resp.failures.get(0).shard().getShardId(), equalTo(shard3.shardId()));
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
            sent.add(nodeRequest(node, shardIds));
            Map<ShardId, Exception> failures = new HashMap<>();
            if (node.equals(node1) && shardIds.contains(shard5)) {
                failures.put(shard5.shardId(), new IOException("test"));
            }
            if (node.equals(node4) && shardIds.contains(shard2)) {
                failures.put(shard2.shardId(), new IOException("test"));
            }
            runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, failures)));
        });
        try {
            future.actionGet(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        assertThat(sent, hasSize(5));
        assertThat(
            take(sent, 3),
            containsInAnyOrder(nodeRequest(node1, shard1, shard5), nodeRequest(node4, shard2), nodeRequest(node2, shard3, shard4))
        );
        assertThat(take(sent, 2), containsInAnyOrder(nodeRequest(node2, shard2), nodeRequest(node3, shard5)));
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
            sent.add(nodeRequest(node, shardIds));
            Map<ShardId, Exception> failures = new HashMap<>();
            if (shardIds.contains(shard5)) {
                failures.put(shard5.shardId(), new IOException("test failure for shard5"));
            }
            runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, failures)));
        });
        var error = expectThrows(Exception.class, future::actionGet);
        assertNotNull(ExceptionsHelper.unwrap(error, IOException.class));
        // {node-1, node-2, node-4}, {node-3}, {node-2}
        assertThat(sent.size(), equalTo(5));
        assertThat(
            take(sent, 3),
            containsInAnyOrder(nodeRequest(node1, shard1, shard5), nodeRequest(node2, shard3, shard4), nodeRequest(node4, shard2))
        );
        assertThat(take(sent, 1), containsInAnyOrder(nodeRequest(node3, shard5)));
        assertThat(take(sent, 1), containsInAnyOrder(nodeRequest(node2, shard5)));
    }

    public void testDoNotRetryOnRequestLevelFailure() {
        var targetShards = List.of(targetShard(shard1, node1), targetShard(shard2, node2), targetShard(shard3, node1));
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        AtomicBoolean failed = new AtomicBoolean();
        var future = sendRequests(false, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            if (node1.equals(node) && failed.compareAndSet(false, true)) {
                runWithDelay(() -> listener.onFailure(new IOException("test request level failure"), true));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            }
        });
        Exception exception = expectThrows(Exception.class, future::actionGet);
        assertNotNull(ExceptionsHelper.unwrap(exception, IOException.class));
        // one round: {node-1, node-2}
        assertThat(sent.size(), equalTo(2));
        assertThat(sent, containsInAnyOrder(nodeRequest(node1, shard1, shard3), nodeRequest(node2, shard2)));
    }

    public void testAllowPartialResults() {
        var targetShards = List.of(targetShard(shard1, node1), targetShard(shard2, node2), targetShard(shard3, node1, node2));
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        AtomicBoolean failed = new AtomicBoolean();
        var future = sendRequests(true, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            if (node1.equals(node) && failed.compareAndSet(false, true)) {
                runWithDelay(() -> listener.onFailure(new IOException("test request level failure"), true));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            }
        });
        var response = safeGet(future);
        assertThat(response.totalShards, equalTo(3));
        assertThat(response.failedShards, equalTo(2));
        assertThat(response.successfulShards, equalTo(1));
        // one round: {node-1, node-2}
        assertThat(sent.size(), equalTo(2));
        assertThat(sent, containsInAnyOrder(nodeRequest(node1, shard1, shard3), nodeRequest(node2, shard2)));
    }

    public void testNonFatalErrorIsRetriedOnAnotherShard() {
        var targetShards = List.of(targetShard(shard1, node1, node2));
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var response = safeGet(sendRequests(false, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            if (Objects.equals(node1, node)) {
                runWithDelay(() -> listener.onFailure(new RuntimeException("test request level non fatal failure"), false));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
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
            sent.add(nodeRequest(node, shardIds));
            runWithDelay(() -> listener.onFailure(new RuntimeException("test request level non fatal failure"), false));
        });
        expectThrows(RuntimeException.class, equalTo("test request level non fatal failure"), future::actionGet);
        assertThat(sent.size(), equalTo(2));
    }

    public void testDoNotRetryCircuitBreakerException() {
        var targetShards = List.of(targetShard(shard1, node1, node2));
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var future = sendRequests(false, -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            runWithDelay(() -> listener.onFailure(new CircuitBreakingException("cbe", randomFrom(Durability.values())), false));
        });
        expectThrows(CircuitBreakingException.class, equalTo("cbe"), future::actionGet);
        assertThat(sent.size(), equalTo(1));
    }

    /**
     * A request that failed does not take back what the requests that succeeded did: the failure carries no completion info, so the
     * usage is reported to the consumer given to the sender, which is how the query is metered with it.
     */
    public void testSuccessfulWorkerUsageSurvivesSiblingFailure() {
        var targetShards = List.of(targetShard(shard1, node1), targetShard(shard2, node2));
        long cpuNanos = between(1, 1_000_000);
        DriverCompletionInfo succeeded = new DriverCompletionInfo(
            0,
            0,
            0,
            0,
            0,
            0,
            cpuNanos,
            0,
            SearchLoadMetrics.EMPTY,
            List.of(),
            List.of(),
            Map.of(),
            false,
            false,
            Set.of()
        );
        // the sibling fails after the worker that succeeded has answered, whichever of them is asked first
        CompletableFuture<Void> workerAnswered = new CompletableFuture<>();
        AtomicReference<DriverCompletionInfo> reported = new AtomicReference<>();
        var future = sendRequests(
            false,
            -1,
            targetShards,
            shardIds -> { throw new AssertionError("No shard resolution is expected here"); },
            (node, shardIds, aliasFilters, listener) -> {
                if (node1.equals(node)) {
                    runWithDelay(() -> {
                        listener.onResponse(new DataNodeComputeResponse(succeeded, Map.of()));
                        workerAnswered.complete(null);
                    });
                } else {
                    workerAnswered.thenRun(
                        () -> listener.onFailure(new CircuitBreakingException("cbe", randomFrom(Durability.values())), false)
                    );
                }
            },
            usage -> assertTrue("reported once", reported.compareAndSet(null, usage))
        );
        expectThrows(CircuitBreakingException.class, equalTo("cbe"), future::actionGet);
        assertNotNull("the usage is reported", reported.get());
        assertThat(reported.get().cpuNanos(), equalTo(cpuNanos));
    }

    public void testLimitConcurrentNodes() {
        final int shards = 10;
        var targetShards = new ArrayList<DataNodeRequestSender.TargetShard>(shards);
        for (int i = 0; i < shards; i++) {
            targetShards.add(
                targetShard(
                    new DataNodeRequest.Shard(new ShardId("index", "n/a", i), SplitShardCountSummary.UNSET),
                    DiscoveryNodeUtils.builder("node-" + i).roles(Set.of(DATA_HOT_NODE_ROLE)).build()
                )
            );
        }

        // this will force requests to pile up
        boolean forceConcurrent = randomBoolean();

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

            sent.add(nodeRequest(node, shardIds));
            runWithDelay(() -> {
                if (forceConcurrent) {
                    int maxRetries = 0;
                    // This forces the requests to pile up to test the concurrency limit is respected
                    while (maxConcurrentRequests.get() < concurrency && maxRetries++ < 100) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                concurrentRequests.decrementAndGet();
                listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of()));
            });
        }));
        assertThat(sent.size(), equalTo(shards));
        if (forceConcurrent) {
            // Since we forced the concurrency, we should hit the exact concurrency limit here
            assertThat(maxConcurrentRequests.get(), equalTo(concurrency));
        } else {
            // Since the requests are not forced to run concurrently, we might not hit the concurrency limit
            // But we never have to exceed it
            assertThat(maxConcurrentRequests.get(), is(lessThanOrEqualTo(concurrency)));
        }
        assertThat(response.totalShards, equalTo(shards));
        assertThat(response.successfulShards, equalTo(shards));
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
                    listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of()));
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
            sent.add(nodeRequest(node, shardIds));
            runWithDelay(() -> {
                if (Objects.equals(node.getId(), node1.getId()) && shardIds.equals(List.of(shard1))) {
                    listener.onFailure(new RuntimeException("test request level non fatal failure"), false);
                } else if (Objects.equals(node.getId(), node3.getId()) && shardIds.equals(List.of(shard2))) {
                    listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of()));
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

    public void testQueryHotShardsFirst() {
        var warnNode = DiscoveryNodeUtils.builder("node-2").roles(Set.of(DATA_WARM_NODE_ROLE)).build();
        var coldNode = DiscoveryNodeUtils.builder("node-3").roles(Set.of(DATA_COLD_NODE_ROLE)).build();
        var frozenNode = DiscoveryNodeUtils.builder("node-4").roles(Set.of(DATA_FROZEN_NODE_ROLE)).build();
        var targetShards = shuffledList(
            List.of(
                targetShard(shard1, node1),
                targetShard(shard2, warnNode),
                targetShard(shard3, coldNode),
                targetShard(shard4, frozenNode)
            )
        );
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        safeGet(sendRequests(randomBoolean(), -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
        }));
        assertThat(
            sent,
            contains(
                nodeRequest(node1, shard1),
                nodeRequest(warnNode, shard2),
                nodeRequest(coldNode, shard3),
                nodeRequest(frozenNode, shard4)
            )
        );
    }

    public void testQueryHotShardsFirstWhenIlmMovesShard() {
        var warmNode2 = DiscoveryNodeUtils.builder("node-2").roles(Set.of(DATA_WARM_NODE_ROLE)).build();
        var targetShards = shuffledList(
            List.of(targetShard(shard1, node1), targetShard(shard2, shuffledList(List.of(node2, warmNode2)).toArray(DiscoveryNode[]::new)))
        );
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        safeGet(sendRequests(randomBoolean(), -1, targetShards, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
        }));
        assertThat(take(sent, 1), contains(nodeRequest(node1, shard1)));
        assertThat(take(sent, 1), anyOf(contains(nodeRequest(node2, shard2)), contains(nodeRequest(warmNode2, shard2))));
    }

    public void testRetryMovedShard() {
        var attempt = new AtomicInteger(0);
        var response = safeGet(
            sendRequests(randomBoolean(), -1, List.of(targetShard(shard1, node1)), shardIds -> switch (attempt.incrementAndGet()) {
                case 1 -> Map.of(shard1.shardId(), List.of(node2));
                case 2 -> Map.of(shard1.shardId(), List.of(node3));
                default -> Map.of(shard1.shardId(), List.of(node4));
            },
                (node, shardIds, aliasFilters, listener) -> runWithDelay(
                    () -> listener.onResponse(
                        Objects.equals(node, node4)
                            ? new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())
                            : new DataNodeComputeResponse(
                                DriverCompletionInfo.EMPTY,
                                Map.of(shard1.shardId(), new ShardNotFoundException(shard1.shardId()))
                            )
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
                (node, shards, aliasFilters, listener) -> runWithDelay(
                    () -> listener.onResponse(
                        attempt.incrementAndGet() <= 6
                            ? new DataNodeComputeResponse(
                                DriverCompletionInfo.EMPTY,
                                shards.stream()
                                    .map(DataNodeRequest.Shard::shardId)
                                    .collect(toMap(Function.identity(), ShardNotFoundException::new))
                            )
                            : new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())
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
            return Map.of(shard1.shardId(), List.of(node2));
        },
            (node, shardIds, aliasFilters, listener) -> runWithDelay(
                () -> listener.onResponse(
                    new DataNodeComputeResponse(
                        DriverCompletionInfo.EMPTY,
                        Map.of(shard1.shardId(), new ShardNotFoundException(shard1.shardId()))
                    )
                )
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
                return Map.of(shard2.shardId(), List.of(node4));
            }, (node, shardIds, aliasFilters, listener) -> runWithDelay(() -> {
                if (Objects.equals(node, node1)) {
                    // search is going to be retried from replica on node3 without shard resolution
                    listener.onResponse(
                        new DataNodeComputeResponse(
                            DriverCompletionInfo.EMPTY,
                            Map.of(shard1.shardId(), new ShardNotFoundException(shard1.shardId()))
                        )
                    );
                } else if (Objects.equals(node, node2)) {
                    // search is going to be retried after resolving new shard node since there are no replicas
                    listener.onResponse(
                        new DataNodeComputeResponse(
                            DriverCompletionInfo.EMPTY,
                            Map.of(shard2.shardId(), new ShardNotFoundException(shard2.shardId()))
                        )
                    );
                } else {
                    listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of()));
                }
            }))
        );
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.successfulShards, equalTo(2));
        assertThat(response.skippedShards, equalTo(0));
        assertThat(response.failedShards, equalTo(0));
        assertThat(attempt.get(), equalTo(1));
        assertThat("Must retry only affected shards", resolvedShards, contains(shard2.shardId()));
    }

    public void testRetryUnassignedShardWithoutPartialResults() {
        var attempt = new AtomicInteger(0);
        var future = sendRequests(false, -1, List.of(targetShard(shard1, node1), targetShard(shard2, node2)), shardIds -> {
            attempt.incrementAndGet();
            return Map.of(shard1.shardId(), List.of());
        },
            (node, shardIds, aliasFilters, listener) -> runWithDelay(
                () -> listener.onResponse(
                    Objects.equals(shardIds, List.of(shard2))
                        ? new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())
                        : new DataNodeComputeResponse(
                            DriverCompletionInfo.EMPTY,
                            Map.of(shard1.shardId(), new ShardNotFoundException(shard1.shardId()))
                        )
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
            return Map.of(shard1.shardId(), List.of());
        },
            (node, shardIds, aliasFilters, listener) -> runWithDelay(
                () -> listener.onResponse(
                    Objects.equals(shardIds, List.of(shard2))
                        ? new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())
                        : new DataNodeComputeResponse(
                            DriverCompletionInfo.EMPTY,
                            Map.of(shard1.shardId(), new ShardNotFoundException(shard1.shardId()))
                        )
                )
            )
        ));
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.skippedShards, equalTo(0));
        assertThat(response.failedShards, equalTo(1));
        assertThat(attempt.get(), equalTo(1));
    }

    public void testNodeFailureClassification() {
        assertThat("refused by the target", DataNodeRequestSender.classifyNodeFailure(refusal(node1)), equalTo(NodeFailure.REFUSED));
        assertThat(
            DataNodeRequestSender.classifyNodeFailure(new NodeDisconnectedException(node1, ComputeService.DATA_ACTION_NAME)),
            equalTo(NodeFailure.UNREACHABLE)
        );
        assertThat(
            DataNodeRequestSender.classifyNodeFailure(new NodeNotConnectedException(node1, "not connected")),
            equalTo(NodeFailure.UNREACHABLE)
        );
        assertThat(
            DataNodeRequestSender.classifyNodeFailure(
                new SendRequestTransportException(node1, "action", new NodeNotConnectedException(node1, "not connected"))
            ),
            equalTo(NodeFailure.UNREACHABLE)
        );
        assertThat(
            "a coordinator that is closing itself says nothing about the target",
            DataNodeRequestSender.classifyNodeFailure(localNodeClosed(node1)),
            equalTo(NodeFailure.OTHER)
        );
        assertThat(
            "already unwrapped",
            DataNodeRequestSender.classifyNodeFailure(new NodeClosedException(node1)),
            equalTo(NodeFailure.OTHER)
        );
        assertThat(
            "a connection failure of the target to another node",
            DataNodeRequestSender.classifyNodeFailure(
                new RemoteTransportException("remote", new NodeDisconnectedException(node2, ComputeService.DATA_ACTION_NAME))
            ),
            equalTo(NodeFailure.OTHER)
        );
        assertThat(
            DataNodeRequestSender.classifyNodeFailure(new RemoteTransportException("remote", new IOException("io"))),
            equalTo(NodeFailure.OTHER)
        );
        assertThat(DataNodeRequestSender.classifyNodeFailure(new IOException("io")), equalTo(NodeFailure.OTHER));
    }

    /**
     * A refusal leaves the node out for every shard that is resolved again, for the rest of the query. A lost connection leaves the
     * node out only for the shard that lost it: another one may be resolved onto that node, since the loss may have been transient.
     */
    public void testOnlyRefusalExcludesNodeForOtherShards() {
        boolean refused = randomBoolean();
        Exception unavailable = refused ? refusal(node1) : new NodeDisconnectedException(node1, ComputeService.DATA_ACTION_NAME);
        var resolved = ConcurrentCollections.<Set<ShardId>>newQueue();
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var node1Attempts = new AtomicInteger();
        // shard2 is only resolved again once shard1 has been retried, so node1 has failed by then
        var shard1Retried = new SubscribableListener<Void>();
        var future = sendRequests(true, -1, List.of(targetShard(shard1, node1), targetShard(shard2, node2)), shardIds -> {
            resolved.add(Set.copyOf(shardIds));
            // the routing places both shards on node1, alone for shard2
            return shardIds.contains(shard1.shardId())
                ? Map.of(shard1.shardId(), List.of(node1, node3))
                : Map.of(shard2.shardId(), List.of(node1));
        }, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            if (node.equals(node2)) {
                shard1Retried.addListener(
                    ActionListener.running(
                        () -> runWithDelay(
                            () -> listener.onResponse(
                                new DataNodeComputeResponse(
                                    DriverCompletionInfo.EMPTY,
                                    Map.of(shard2.shardId(), new ShardNotFoundException(shard2.shardId()))
                                )
                            )
                        )
                    )
                );
            } else if (node.equals(node1) && node1Attempts.incrementAndGet() == 1) {
                runWithDelay(() -> listener.onFailure(unavailable, false));
            } else {
                if (shardIds.contains(shard1)) {
                    shard1Retried.onResponse(null);
                }
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            }
        });
        var response = safeGet(future);
        assertThat(resolved, contains(Set.of(shard1.shardId()), Set.of(shard2.shardId())));
        assertThat(response.totalShards, equalTo(2));
        assertThat("shard1 does not go back to the node that failed it", sent, hasItem(nodeRequest(node3, shard1)));
        if (refused) {
            assertThat("shard2 has no copy off the node that refused", response.failedShards, equalTo(1));
            assertThat(response.successfulShards, equalTo(1));
            assertThat(node1Attempts.get(), equalTo(1));
        } else {
            assertThat(
                "shard2 goes to the node that was unreachable for shard1",
                sent,
                containsInAnyOrder(
                    nodeRequest(node1, shard1),
                    nodeRequest(node2, shard2),
                    nodeRequest(node3, shard1),
                    nodeRequest(node1, shard2)
                )
            );
            assertThat(response.successfulShards, equalTo(2));
            assertThat(response.failedShards, equalTo(0));
        }
    }

    /**
     * The failure of a compute comes out of a {@code ComputeListener} without its transport wrapper, so it cannot be classified any
     * more: a connection failure that the data node ran into itself would look like a lost connection to that node. The caller says
     * what the request's failure meant, and here it meant nothing about the node, so the shard is not resolved again.
     */
    public void testUnwrappedConnectionFailureOfTheDataNodeIsNotResolvedAgain() {
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var response = safeGet(sendRequests(true, -1, List.of(targetShard(shard1, node1)), (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            runWithDelay(
                () -> listener.onFailure(new NodeDisconnectedException(node2, ComputeService.DATA_ACTION_NAME), false, NodeFailure.OTHER)
            );
        }));
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).getCause(), instanceOf(NodeDisconnectedException.class));
        assertThat(sent, contains(nodeRequest(node1, shard1)));
    }

    /**
     * A shard-level failure in a response says nothing about the node, which did answer: even a connection failure there does not make
     * the shard resolve again.
     */
    public void testConnectionFailureAsShardLevelFailureIsNotResolvedAgain() {
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var response = safeGet(sendRequests(true, -1, List.of(targetShard(shard1, node1)), (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            runWithDelay(
                () -> listener.onResponse(
                    new DataNodeComputeResponse(
                        DriverCompletionInfo.EMPTY,
                        Map.of(shard1.shardId(), new NodeDisconnectedException(node2, ComputeService.DATA_ACTION_NAME))
                    )
                )
            );
        }));
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).getCause(), instanceOf(NodeDisconnectedException.class));
        assertThat(sent, contains(nodeRequest(node1, shard1)));
    }

    /**
     * A node that refuses the request or cannot be reached, before any page was received, makes the shard resolve again. The node
     * is left out of the new copies, also when the routing still lists it, and before the other copy.
     */
    public void testRetryRefusedOrDisconnectedNodeElsewhere() {
        var attempt = new AtomicInteger();
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        Exception unavailable = randomNodeUnavailableFailure(node1);
        // the routing may still list the departing node
        boolean departingNodeListedFirst = randomBoolean();
        var response = safeGet(sendRequests(randomBoolean(), -1, List.of(targetShard(shard1, node1)), shardIds -> {
            attempt.incrementAndGet();
            return Map.of(shard1.shardId(), departingNodeListedFirst ? List.of(node1, node2) : List.of(node2, node1));
        }, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            if (node.equals(node1)) {
                runWithDelay(() -> listener.onFailure(unavailable, false));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            }
        }));
        assertThat(response.totalShards, equalTo(1));
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
        assertThat(attempt.get(), equalTo(1));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
    }

    /**
     * If the only copy that resolving again finds is on the node that refused or could not be reached, the shard fails in the same
     * pass, with that original failure: nothing waits for the routing to change, and the node is not asked again.
     */
    public void testRefusedNodeIsNotRetriedWhenItHoldsTheOnlyCopy() {
        var attempt = new AtomicInteger();
        var sentTo = ConcurrentCollections.<DiscoveryNode>newQueue();
        Exception unavailable = randomNodeUnavailableFailure(node1);
        boolean allowPartialResults = randomBoolean();
        var future = sendRequests(allowPartialResults, -1, List.of(targetShard(shard1, node1), targetShard(shard2, node2)), shardIds -> {
            attempt.incrementAndGet();
            return Map.of(shard1.shardId(), List.of(node1));
        }, (node, shardIds, aliasFilters, listener) -> {
            sentTo.add(node);
            if (node.equals(node1)) {
                runWithDelay(() -> listener.onFailure(unavailable, false));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            }
        });
        if (allowPartialResults) {
            var response = safeGet(future);
            assertThat(response.totalShards, equalTo(2));
            assertThat(response.successfulShards, equalTo(1));
            assertThat(response.failedShards, equalTo(1));
            assertThat(response.failures, hasSize(1));
            assertThat(response.failures.get(0).getCause(), instanceOf(unavailable.getCause().getClass()));
        } else {
            Exception e = expectThrows(Exception.class, future::actionGet);
            assertThat(e, instanceOf(unavailable.getCause().getClass()));
        }
        assertThat(attempt.get(), equalTo(1));
        assertThat("the node is asked once", sentTo.stream().filter(node1::equals).count(), equalTo(1L));
    }

    /**
     * A node that cannot be reached while the routing still lists it does not use up the resolution attempts: the shard that failed
     * on it is not given that node again, so resolving it gives no copy and is not charged. Here a single attempt is left for a
     * shard that moved afterwards, which `main` would retry too.
     */
    public void testUnreachableNodeDoesNotSpendTheAttemptsOfOtherShards() {
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        // shard2 only fails once the failure of shard1 has been handled
        var shard1Failed = new SubscribableListener<Void>();
        var response = safeGet(sendRequests(true, -1, List.of(targetShard(shard1, node1), targetShard(shard2, node2)), 1, shardIds -> {
            var resolved = new HashMap<ShardId, List<DiscoveryNode>>();
            if (shardIds.contains(shard1.shardId())) {
                resolved.put(shard1.shardId(), List.of(node1));
            }
            if (shardIds.contains(shard2.shardId())) {
                resolved.put(shard2.shardId(), List.of(node3));
            }
            return resolved;
        }, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            if (node.equals(node1)) {
                runWithDelay(() -> {
                    listener.onFailure(randomLostConnection(node1), false);
                    shard1Failed.onResponse(null);
                });
            } else if (node.equals(node2)) {
                shard1Failed.addListener(ActionListener.running(() -> runWithDelay(() -> listener.onResponse(shardNotFound(shard2)))));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            }
        }));
        assertThat(sent, containsInAnyOrder(nodeRequest(node1, shard1), nodeRequest(node2, shard2), nodeRequest(node3, shard2)));
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).shard().getShardId(), equalTo(shard1.shardId()));
    }

    /**
     * A request that fails while it is being sent, as one to a node that is not connected does, is picked up by the loop that is
     * sending, in its next pass, and not by another loop entered from inside it. Here every resolution places the shard on yet
     * another node that cannot be reached, so its request fails in every pass: entered from inside, the loops would nest once per
     * pass. The requests of the other tests complete on another thread, so they do not get here.
     */
    public void testInlineConnectionFailureDoesNotNestTheSendLoop() {
        int passes = 1000;
        var resolutions = new AtomicInteger();
        var stackDepths = ConcurrentCollections.<Long>newQueue();
        var response = safeGet(sendRequests(randomBoolean(), -1, List.of(targetShard(shard1, node1)), -1, shardIds -> {
            int resolution = resolutions.incrementAndGet();
            DiscoveryNode node = resolution < passes ? DiscoveryNodeUtils.create("not-connected-" + resolution) : node2;
            return Map.of(shard1.shardId(), List.of(node));
        }, (node, shardIds, aliasFilters, listener) -> {
            if (node.equals(node2)) {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            } else {
                stackDepths.add(StackWalker.getInstance().walk(Stream::count));
                // on the calling thread, which is sending and holds the lock for it, as a failed getConnection does
                listener.onFailure(new NodeNotConnectedException(node, "not connected"), false);
            }
        }));
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
        assertThat(stackDepths, hasSize(passes));
        assertThat("every request is sent from the same depth of the stack", Set.copyOf(stackDepths), hasSize(1));
    }

    /**
     * Resolving a shard again after a refusal is not charged against the resolution attempts, and is not held back once they are
     * spent. Here the attempts are spent by a shard that moved. A shard that is refused afterwards, and whose replacement copy only
     * shows when it is resolved again, is still asked there, while a shard that fails as unavailable afterwards fails without being
     * resolved again.
     */
    public void testRefusalIsNotChargedAgainstASpentBudget() {
        var resolved = ConcurrentCollections.<ShardId>newQueue();
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        // the other two nodes only answer once shard2 has used up the only attempt
        var attemptsSpent = new SubscribableListener<Void>();
        var response = safeGet(
            sendRequests(
                true,
                -1,
                List.of(targetShard(shard1, node1), targetShard(shard2, node2), targetShard(shard3, node3)),
                1,
                shardIds -> {
                    resolved.addAll(shardIds);
                    return shardIds.stream()
                        .collect(toMap(Function.identity(), shardId -> List.of(shardId.equals(shard2.shardId()) ? node4 : node5)));
                },
                (node, shardIds, aliasFilters, listener) -> {
                    sent.add(nodeRequest(node, shardIds));
                    if (node.equals(node1)) {
                        attemptsSpent.addListener(
                            ActionListener.running(() -> runWithDelay(() -> listener.onFailure(refusal(node1), false)))
                        );
                    } else if (node.equals(node2)) {
                        runWithDelay(() -> listener.onResponse(shardNotFound(shard2)));
                    } else if (node.equals(node3)) {
                        attemptsSpent.addListener(
                            ActionListener.running(() -> runWithDelay(() -> listener.onResponse(shardNotFound(shard3))))
                        );
                    } else {
                        if (node.equals(node4)) {
                            attemptsSpent.onResponse(null);
                        }
                        runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
                    }
                }
            )
        );
        assertThat("shard3 is not resolved again", resolved, containsInAnyOrder(shard2.shardId(), shard1.shardId()));
        assertThat(sent, hasItem(nodeRequest(node4, shard2)));
        assertThat("the refused shard is asked on the copy it resolved to", sent, hasItem(nodeRequest(node5, shard1)));
        assertThat(response.totalShards, equalTo(3));
        assertThat(response.successfulShards, equalTo(2));
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).shard().getShardId(), equalTo(shard3.shardId()));
    }

    /**
     * A pass in which every shard that is resolved again was refused leaves the resolution attempts whole. The one attempt there is
     * goes to a later retry after a failure that was not a refusal, and the next such failure ends the shard.
     */
    public void testRefusalPassesDoNotCountAgainstTheBudget() {
        var resolutions = new AtomicInteger();
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var response = safeGet(sendRequests(true, -1, List.of(targetShard(shard1, node1)), 1, shardIds -> {
            // the routing still lists the node that refused, which is left out
            return Map.of(shard1.shardId(), resolutions.incrementAndGet() == 1 ? List.of(node1, node2) : List.of(node1, node3));
        }, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            if (node.equals(node1)) {
                runWithDelay(() -> listener.onFailure(refusal(node1), false));
            } else {
                runWithDelay(() -> listener.onResponse(shardNotFound(shard1)));
            }
        }));
        assertThat("a third resolution would need a second attempt", resolutions.get(), equalTo(2));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1), nodeRequest(node3, shard1)));
        assertThat(response.totalShards, equalTo(1));
        assertThat(response.failedShards, equalTo(1));
        assertThat("the first failure is the one reported", response.failures.get(0).getCause(), instanceOf(NodeClosedException.class));
    }

    /**
     * A shard still has a copy listed on a node that it was never sent to, when that node refuses another shard. The copy is tried
     * all the same: the refusal it gets is what lets the shard be resolved again without attempts, here with none left, and reach the
     * copy that replaced it. Leaving the node out of the listed copies without a request would end the shard on its earlier failure.
     */
    public void testUnattemptedShardOnRefuserIsStillTried() {
        var resolved = ConcurrentCollections.<ShardId>newQueue();
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        // node2 only answers for shard2 once node1 has refused shard1
        var node1Refused = new SubscribableListener<Void>();
        var response = safeGet(
            sendRequests(
                true,
                -1,
                List.of(targetShard(shard1, node1), targetShard(shard2, node2, node1)),
                0,
                // the replacement only shows when the shard is resolved again; the routing still lists the node that refused
                shardIds -> {
                    resolved.addAll(shardIds);
                    return shardIds.stream().collect(toMap(Function.identity(), shardId -> List.of(node1, node3)));
                },
                (node, shardIds, aliasFilters, listener) -> {
                    sent.add(nodeRequest(node, shardIds));
                    if (node.equals(node1)) {
                        runWithDelay(() -> {
                            listener.onFailure(refusal(node1), false);
                            node1Refused.onResponse(null);
                        });
                    } else if (node.equals(node2)) {
                        node1Refused.addListener(
                            ActionListener.running(() -> runWithDelay(() -> listener.onResponse(shardNotFound(shard2))))
                        );
                    } else {
                        runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
                    }
                }
            )
        );
        assertThat("the copy on the node that refused shard1 is tried for shard2", sent, hasItem(nodeRequest(node1, shard2)));
        assertThat(sent, hasItem(nodeRequest(node3, shard2)));
        assertThat(resolved, containsInAnyOrder(shard1.shardId(), shard2.shardId()));
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.successfulShards, equalTo(2));
        assertThat(response.failedShards, equalTo(0));
    }

    /**
     * A node that may have received data of the query fails its shards for good, whatever its failure says about the node and
     * whatever is left of the resolution attempts: another copy would produce the same data again.
     */
    public void testReceivedDataFailureIsNeverResolvedAgain() {
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        NodeFailure nodeFailure = randomFrom(NodeFailure.UNREACHABLE, NodeFailure.REFUSED);
        var response = safeGet(
            sendRequests(true, -1, List.of(targetShard(shard1, node1, node2)), randomFrom(-1, 0, DEFAULT_RESOLUTION_ATTEMPTS), shardIds -> {
                throw new AssertionError("no shard resolution is expected here");
            }, (node, shardIds, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, shardIds));
                runWithDelay(
                    () -> listener.onFailure(new NodeDisconnectedException(node, ComputeService.DATA_ACTION_NAME), true, nodeFailure)
                );
            })
        );
        assertThat("the other listed copy is not asked either", sent, contains(nodeRequest(node1, shard1)));
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).getCause(), instanceOf(NodeDisconnectedException.class));
    }

    /**
     * Only the open is refused by a node that stopped admitting exchanges. A {@link NodeClosedException} that fails the data node
     * request came from further down, a lookup for example, and is not read as a refusal. Passed to the sender classified so, the
     * failure does not make the shard resolve again, and does not leave the node out for another shard that is resolved onto it
     * later.
     * <p>
     * Without partial results no failure of the data node request is read as one of the node, a lost connection included: it has
     * cancelled the query by the time the sender hears of it.
     */
    public void testDataRequestFailureIsNeverARefusal() {
        Exception lookupFailure = new RemoteTransportException(
            "remote",
            new RemoteTransportException("lookup", new NodeClosedException(node3))
        );
        assertThat(DataNodeRequestSender.classifyNodeFailure(lookupFailure), equalTo(NodeFailure.REFUSED));
        assertThat(DataNodeRequestSender.classifyDataRequestFailure(lookupFailure, true), equalTo(NodeFailure.OTHER));
        assertThat(DataNodeRequestSender.classifyDataRequestFailure(refusal(node1), true), equalTo(NodeFailure.OTHER));
        Exception disconnected = new NodeDisconnectedException(node1, ComputeService.DATA_ACTION_NAME);
        assertThat(DataNodeRequestSender.classifyDataRequestFailure(disconnected, true), equalTo(NodeFailure.UNREACHABLE));
        assertThat(DataNodeRequestSender.classifyDataRequestFailure(new IOException("io"), true), equalTo(NodeFailure.OTHER));
        for (Exception e : List.of(lookupFailure, refusal(node1), disconnected, randomLostConnection(node1), new IOException("io"))) {
            assertThat(e.toString(), DataNodeRequestSender.classifyDataRequestFailure(e, false), equalTo(NodeFailure.OTHER));
        }

        var resolved = ConcurrentCollections.<Set<ShardId>>newQueue();
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        // shard2 is only resolved again once the failure of shard1 has been handled
        var shard1Failed = new SubscribableListener<Void>();
        var response = safeGet(sendRequests(true, -1, List.of(targetShard(shard1, node1), targetShard(shard2, node2)), shardIds -> {
            resolved.add(Set.copyOf(shardIds));
            return Map.of(shard2.shardId(), List.of(node1));
        }, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            if (node.equals(node1) && shardIds.contains(shard1)) {
                runWithDelay(() -> {
                    listener.onFailure(lookupFailure, false, DataNodeRequestSender.classifyDataRequestFailure(lookupFailure, true));
                    shard1Failed.onResponse(null);
                });
            } else if (node.equals(node2)) {
                shard1Failed.addListener(ActionListener.running(() -> runWithDelay(() -> listener.onResponse(shardNotFound(shard2)))));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            }
        }));
        assertThat("shard1 is not resolved again", resolved, contains(Set.of(shard2.shardId())));
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).shard().getShardId(), equalTo(shard1.shardId()));
        assertThat(response.successfulShards, equalTo(1));
        assertThat("node1 still serves shard2", sent, hasItem(nodeRequest(node1, shard2)));
    }

    /**
     * Waiting for newer routing is not there yet, so the only timeout a sender takes is zero.
     */
    public void testConstructorRejectsNonZeroRoutingWaitTimeout() {
        Sender unused = (node, shardIds, aliasFilters, listener) -> { throw new AssertionError("no request is expected here"); };
        Resolver noResolution = shardIds -> { throw new AssertionError("no shard resolution is expected here"); };
        TimeValue timeout = randomTimeValue(1, 60, TimeUnit.SECONDS);
        var e = expectThrows(
            AssertionError.class,
            () -> newSender(SliceSelection.UNSPECIFIED, false, 1, List.of(), DEFAULT_RESOLUTION_ATTEMPTS, timeout, noResolution, unused)
        );
        assertThat(e.getMessage(), containsString("waiting for newer routing is not supported yet"));
    }

    /**
     * A coordinator that is closing fails its own requests with a {@link NodeClosedException}. That is not a refusal by the target: the
     * shard is not resolved again and no node is left out.
     */
    public void testLocalNodeClosedIsNotTreatedAsRefusal() {
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var response = safeGet(sendRequests(true, -1, List.of(targetShard(shard1, node1)), (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            runWithDelay(() -> listener.onFailure(localNodeClosed(node), false));
        }));
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).getCause(), instanceOf(NodeClosedException.class));
        assertThat(sent, contains(nodeRequest(node1, shard1)));
    }

    /**
     * A shard is resolved again only if every failure seen for it allows that: a failure that does not, followed by a disconnect from
     * another copy, fails the shard with the first failure.
     */
    public void testIneligibleFailureThenDisconnectDoesNotResolveAgain() {
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var response = safeGet(
            sendRequests(true, -1, List.of(targetShard(shard1, node1, node2)), (node, shardIds, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, shardIds));
                if (node.equals(node1)) {
                    runWithDelay(() -> listener.onFailure(new IOException("not retryable elsewhere"), false));
                } else {
                    runWithDelay(() -> listener.onFailure(randomNodeUnavailableFailure(node), false));
                }
            })
        );
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).getCause(), instanceOf(IOException.class));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
    }

    /**
     * Once pages were received from a node, its disconnect fails the shard as before, without resolving it again.
     */
    public void testDisconnectAfterPagesIsNotRetried() {
        var response = safeGet(
            sendRequests(
                true,
                -1,
                List.of(targetShard(shard1, node1)),
                (node, shardIds, aliasFilters, listener) -> runWithDelay(
                    () -> listener.onFailure(new NodeDisconnectedException(node, ComputeService.DATA_ACTION_NAME), true)
                )
            )
        );
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).getCause(), instanceOf(NodeDisconnectedException.class));
    }

    /**
     * A shard that was retried on another node, and fails there for good because that node disconnects after pages were received,
     * reports the failure of that node and not the one of the node it was retried from.
     */
    public void testDisconnectAfterPagesOnReplacementReportsThatFailure() {
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        boolean allowPartialResults = randomBoolean();
        // the nodes of this test have no name, so the two failures are told apart by instance
        Exception disconnected = new NodeDisconnectedException(node2, ComputeService.DATA_ACTION_NAME);
        var future = sendRequests(
            allowPartialResults,
            -1,
            List.of(targetShard(shard1, node1)),
            shardIds -> Map.of(shard1.shardId(), List.of(node2)),
            (node, shardIds, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, shardIds));
                if (node.equals(node1)) {
                    runWithDelay(() -> listener.onFailure(randomNodeUnavailableFailure(node1), false));
                } else {
                    runWithDelay(() -> listener.onFailure(disconnected, true, NodeFailure.UNREACHABLE));
                }
            }
        );
        final Throwable reported;
        if (allowPartialResults) {
            var response = safeGet(future);
            assertThat(response.failedShards, equalTo(1));
            assertThat(response.failures, hasSize(1));
            reported = response.failures.get(0).getCause();
        } else {
            reported = expectThrows(Exception.class, future::actionGet);
        }
        assertThat(reported, sameInstance(disconnected));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
    }

    /**
     * A query that is cancelled does not resolve its shards again, whatever their failure says about the node: nothing is started
     * on another node for the cancellation to stop.
     */
    public void testCancelledQueryIsNotResolvedAgain() {
        var resolutions = new AtomicInteger();
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        CancellableTask task = newTask();
        Exception unavailable = randomNodeUnavailableFailure(node1);
        boolean allowPartialResults = randomBoolean();
        PlainActionFuture<ComputeResponse> future = new PlainActionFuture<>();
        newSender(
            SliceSelection.UNSPECIFIED,
            allowPartialResults,
            -1,
            List.of(targetShard(shard1, node1)),
            DEFAULT_RESOLUTION_ATTEMPTS,
            TimeValue.ZERO,
            task,
            shardIds -> {
                resolutions.incrementAndGet();
                return Map.of(shard1.shardId(), List.of(node2));
            },
            (node, shardIds, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, shardIds));
                if (node.equals(node1)) {
                    runWithDelay(() -> {
                        TaskCancelHelper.cancel(task, "test");
                        listener.onFailure(unavailable, false);
                    });
                } else {
                    runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
                }
            }
        ).startComputeOnDataNodes(Set.of(randomAlphaOfLength(10)), () -> {}, usage -> {}, future);
        if (allowPartialResults) {
            var response = safeGet(future);
            assertThat(response.failedShards, equalTo(1));
            assertThat(response.failures.get(0).getCause(), instanceOf(unavailable.getCause().getClass()));
        } else {
            Exception e = expectThrows(Exception.class, future::actionGet);
            assertThat(e, instanceOf(unavailable.getCause().getClass()));
        }
        assertThat("the shard is not resolved again", resolutions.get(), equalTo(0));
        assertThat(sent, contains(nodeRequest(node1, shard1)));
    }

    /**
     * A data node request that loses its connection before any page was received is resolved again with partial results, and not
     * without them: the failure is passed to the sender the way the handler classifies it, and without partial results the shard
     * fails with it, the resolver is not asked and no other node gets a request.
     */
    public void testDataRequestDisconnectIsOnlyResolvedAgainWithPartialResults() {
        assertDataRequestDisconnectIsOnlyResolvedAgainWithPartialResults(true);
        assertDataRequestDisconnectIsOnlyResolvedAgainWithPartialResults(false);
    }

    private void assertDataRequestDisconnectIsOnlyResolvedAgainWithPartialResults(boolean allowPartialResults) {
        var resolutions = new AtomicInteger();
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        Exception disconnected = new NodeDisconnectedException(node1, ComputeService.DATA_ACTION_NAME);
        var future = sendRequests(allowPartialResults, -1, List.of(targetShard(shard1, node1)), shardIds -> {
            resolutions.incrementAndGet();
            return Map.of(shard1.shardId(), List.of(node2));
        }, (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            if (node.equals(node1)) {
                NodeFailure nodeFailure = DataNodeRequestSender.classifyDataRequestFailure(disconnected, allowPartialResults);
                runWithDelay(() -> listener.onFailure(disconnected, false, nodeFailure));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            }
        });
        if (allowPartialResults) {
            var response = safeGet(future);
            assertThat(response.successfulShards, equalTo(1));
            assertThat(response.failedShards, equalTo(0));
            assertThat(resolutions.get(), equalTo(1));
            assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
        } else {
            Exception e = expectThrows(Exception.class, future::actionGet);
            assertThat(e, instanceOf(NodeDisconnectedException.class));
            assertThat("the shard is not resolved again", resolutions.get(), equalTo(0));
            assertThat(sent, contains(nodeRequest(node1, shard1)));
        }
    }

    private static RemoteTransportException refusal(DiscoveryNode node) {
        return new RemoteTransportException("[" + node.getName() + "][open_exchange]", new NodeClosedException(node));
    }

    private static SendRequestTransportException localNodeClosed(DiscoveryNode target) {
        DiscoveryNode coordinator = DiscoveryNodeUtils.create("coordinator");
        return new SendRequestTransportException(target, ComputeService.DATA_ACTION_NAME, new NodeClosedException(coordinator));
    }

    /**
     * A refusal or a lost connection; the cause is what the shard failure reports.
     */
    private static Exception randomNodeUnavailableFailure(DiscoveryNode node) {
        return randomBoolean() ? refusal(node) : randomLostConnection(node);
    }

    /**
     * A connection that was lost when sending; the cause is what the shard failure reports.
     */
    private static Exception randomLostConnection(DiscoveryNode node) {
        return randomBoolean()
            ? new SendRequestTransportException(
                node,
                ComputeService.DATA_ACTION_NAME,
                new NodeDisconnectedException(node, ComputeService.DATA_ACTION_NAME)
            )
            : new SendRequestTransportException(node, "open_exchange", new NodeNotConnectedException(node, "not connected"));
    }

    private static DataNodeComputeResponse shardNotFound(DataNodeRequest.Shard shard) {
        return new DataNodeComputeResponse(
            DriverCompletionInfo.EMPTY,
            Map.of(shard.shardId(), new ShardNotFoundException(shard.shardId()))
        );
    }

    static DataNodeRequestSender.TargetShard targetShard(DataNodeRequest.Shard shard, DiscoveryNode... nodes) {
        return new DataNodeRequestSender.TargetShard(
            shard.shardId(),
            new ArrayList<>(Arrays.asList(nodes)),
            null,
            shard.splitShardCountSummary()
        );
    }

    static DataNodeRequestSender.NodeRequest nodeRequest(DiscoveryNode node, DataNodeRequest.Shard... shardIds) {
        return nodeRequest(node, Arrays.asList(shardIds));
    }

    static DataNodeRequestSender.NodeRequest nodeRequest(DiscoveryNode node, List<DataNodeRequest.Shard> shards) {
        var copy = new ArrayList<>(shards);
        Collections.sort(copy, Comparator.comparing(DataNodeRequest.Shard::shardId));
        return new NodeRequest(node, copy, Map.of());
    }

    static <T> Collection<T> take(Queue<T> queue, int limit) {
        var result = new ArrayList<T>(limit);
        for (int i = 0; i < limit; i++) {
            result.add(queue.remove());
        }
        return result;
    }

    void runWithDelay(Runnable runnable) {
        if (randomBoolean()) {
            threadPool.schedule(runnable, timeValueNanos(between(0, 5000)), executor);
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
        return sendRequests(allowPartialResults, concurrentRequests, shards, DEFAULT_RESOLUTION_ATTEMPTS, resolver, sender);
    }

    PlainActionFuture<ComputeResponse> sendRequests(
        boolean allowPartialResults,
        int concurrentRequests,
        List<DataNodeRequestSender.TargetShard> shards,
        int unavailableShardResolutionAttempts,
        Resolver resolver,
        Sender sender
    ) {
        return sendRequests(
            allowPartialResults,
            concurrentRequests,
            shards,
            unavailableShardResolutionAttempts,
            resolver,
            sender,
            usage -> {}
        );
    }

    PlainActionFuture<ComputeResponse> sendRequests(
        boolean allowPartialResults,
        int concurrentRequests,
        List<DataNodeRequestSender.TargetShard> shards,
        Resolver resolver,
        Sender sender,
        Consumer<DriverCompletionInfo> onFailureUsage
    ) {
        return sendRequests(allowPartialResults, concurrentRequests, shards, DEFAULT_RESOLUTION_ATTEMPTS, resolver, sender, onFailureUsage);
    }

    PlainActionFuture<ComputeResponse> sendRequests(
        boolean allowPartialResults,
        int concurrentRequests,
        List<DataNodeRequestSender.TargetShard> shards,
        int unavailableShardResolutionAttempts,
        Resolver resolver,
        Sender sender,
        Consumer<DriverCompletionInfo> onFailureUsage
    ) {
        PlainActionFuture<ComputeResponse> future = new PlainActionFuture<>();
        newSender(
            SliceSelection.UNSPECIFIED,
            allowPartialResults,
            concurrentRequests,
            shards,
            unavailableShardResolutionAttempts,
            TimeValue.ZERO,
            resolver,
            sender
        ).startComputeOnDataNodes(Set.of(randomAlphaOfLength(10)), () -> {}, onFailureUsage, future);
        return future;
    }

    private DataNodeRequestSender newSender(
        SliceSelection slices,
        boolean allowPartialResults,
        int concurrentRequests,
        List<DataNodeRequestSender.TargetShard> shards,
        Resolver resolver,
        Sender sender
    ) {
        return newSender(
            slices,
            allowPartialResults,
            concurrentRequests,
            shards,
            DEFAULT_RESOLUTION_ATTEMPTS,
            TimeValue.ZERO,
            resolver,
            sender
        );
    }

    private DataNodeRequestSender newSender(
        SliceSelection slices,
        boolean allowPartialResults,
        int concurrentRequests,
        List<DataNodeRequestSender.TargetShard> shards,
        int unavailableShardResolutionAttempts,
        TimeValue routingWaitTimeout,
        Resolver resolver,
        Sender sender
    ) {
        return newSender(
            slices,
            allowPartialResults,
            concurrentRequests,
            shards,
            unavailableShardResolutionAttempts,
            routingWaitTimeout,
            newTask(),
            resolver,
            sender
        );
    }

    private static CancellableTask newTask() {
        return new CancellableTask(
            randomNonNegativeLong(),
            "type",
            "action",
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
    }

    private DataNodeRequestSender newSender(
        SliceSelection slices,
        boolean allowPartialResults,
        int concurrentRequests,
        List<DataNodeRequestSender.TargetShard> shards,
        int unavailableShardResolutionAttempts,
        TimeValue routingWaitTimeout,
        CancellableTask task,
        Resolver resolver,
        Sender sender
    ) {
        TransportService transportService = MockTransportService.createNewService(
            Settings.EMPTY,
            VersionInformation.CURRENT,
            TransportVersion.current(),
            threadPool
        );
        return new DataNodeRequestSender(
            null,
            null,
            transportService,
            executor,
            task,
            new OriginalIndices(new String[0], SearchRequest.DEFAULT_INDICES_OPTIONS),
            null,
            slices,
            "",
            allowPartialResults,
            concurrentRequests,
            unavailableShardResolutionAttempts,
            routingWaitTimeout
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
                List<DataNodeRequest.Shard> shards,
                Map<Index, AliasFilter> aliasFilters,
                NodeListener listener
            ) {
                sender.sendRequestToOneNode(node, shards, aliasFilters, listener);
            }
        };
    }

    /**
     * A selection of named slices routes the query to the shards that hold them. Without a slice-enabled index there is
     * nothing to route by: the filter the selection is derived from matches no document.
     */
    public void testSliceSelectionRoutesShardResolution() {
        Sender unused = (node, shardIds, aliasFilters, listener) -> { throw new AssertionError("no request is expected here"); };
        Resolver noResolution = shardIds -> { throw new AssertionError("no shard resolution is expected here"); };

        for (boolean anySliceEnabled : List.of(true, false)) {
            var request = newSender(SliceSelection.UNSPECIFIED, false, 1, List.of(), noResolution, unused).searchShardsRequest(
                anySliceEnabled
            );
            assertNull(request.routing());
            assertFalse(request.isRoutingFromSlice());
        }

        var request = newSender(SliceSelection.of(List.of("s1", "s2")), false, 1, List.of(), noResolution, unused).searchShardsRequest(
            true
        );
        assertThat(request.routing(), equalTo("s1,s2"));
        assertTrue(request.isRoutingFromSlice());
        assertThat(request.searchSlice(), equalTo("s1,s2"));

        request = newSender(SliceSelection.of(List.of("s1", "s2")), false, 1, List.of(), noResolution, unused).searchShardsRequest(false);
        assertNull(request.routing());
        assertFalse(request.isRoutingFromSlice());
    }

    interface Resolver {
        Map<ShardId, List<DiscoveryNode>> resolve(Set<ShardId> shardIds);
    }

    interface Sender {
        void sendRequestToOneNode(
            DiscoveryNode node,
            List<DataNodeRequest.Shard> shards,
            Map<Index, AliasFilter> aliasFilters,
            NodeListener listener
        );
    }
}
