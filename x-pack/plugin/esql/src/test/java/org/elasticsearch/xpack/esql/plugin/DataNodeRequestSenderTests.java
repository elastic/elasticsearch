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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker.Durability;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.test.ComputeTestCase;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.CopyState;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeFailure;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeListener;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.SentCopy;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.ShardCopy;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.Untried;
import org.elasticsearch.xpack.esql.session.EsqlCCSUtils;
import org.elasticsearch.xpack.esql.session.Result;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_COLD_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_HOT_NODE_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_WARM_NODE_ROLE;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.NodeRequest;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class DataNodeRequestSenderTests extends ComputeTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private Executor executor = null;
    /** The version of the last state built by {@link #routingState}; the observer of a routing wait only reacts to newer ones. */
    private final AtomicLong stateVersion = new AtomicLong();

    /** The production default of {@link EsqlPlugin#SHARD_ROUTING_WAIT_TIMEOUT}. */
    private static final TimeValue DEFAULT_ROUTING_WAIT_TIMEOUT = TimeValue.timeValueSeconds(30);

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
        createThreadPool(randomBoolean() ? 1 : between(2, 16));
    }

    private void createThreadPool(int numThreads) {
        var settings = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "TransportActionTests").build();
        threadPool = new ThreadPool(
            new ThreadContext(settings),
            settings,
            MeterRegistry.NOOP,
            (s, allocatedProcessors) -> Map.of(),
            new FixedExecutorBuilder(Settings.EMPTY, ThreadPool.Names.SEARCH, numThreads, 1024, "", EsExecutors.TaskTrackingConfig.DEFAULT),
            // This pool is built without the default executors, so it has no generic one of its own. The timer of a routing wait
            // and the cluster service run on it.
            new FixedExecutorBuilder(Settings.EMPTY, ThreadPool.Names.GENERIC, numThreads, 1024, "", EsExecutors.TaskTrackingConfig.DEFAULT)
        );
        executor = threadPool.executor(ThreadPool.Names.SEARCH);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        stateVersion.set(clusterService.state().version());
    }

    /**
     * For a test that blocks a search thread while another one has to run the sending loop: replaces the pool, which may have a
     * single search thread, with one that has at least two.
     */
    private void requireTwoSearchThreads() {
        shutdownThreadPool();
        createThreadPool(between(2, 16));
    }

    @After
    public void shutdownThreadPool() {
        clusterService.close();
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
        var future = setup(targetShards, (node, shardIds, aliasFilters, listener) -> {
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
        }).resolver(shardIds -> { throw new AssertionError("No shard resolution is expected here"); })
            .onFailureUsage(usage -> assertTrue("reported once", reported.compareAndSet(null, usage)))
            .start();
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

    /**
     * The routing retry of {@code main}, which a timeout of zero keeps: every pass resolves again, and a copy that failed before is
     * asked again when the resolution still lists it.
     */
    public void testRetryMultipleMovedShardsAtZeroTimeout() {
        var attempt = new AtomicInteger(0);
        var response = safeGet(
            setup(
                List.of(targetShard(shard1, node1), targetShard(shard2, node2), targetShard(shard3, node3)),
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
            ).allowPartialResults(randomBoolean())
                .resolver(
                    shardIds -> shardIds.stream().collect(toMap(Function.identity(), shardId -> List.of(randomFrom(node1, node2, node3))))
                )
                .routingWaitTimeout(TimeValue.ZERO)
                .start()
        );
        assertThat(response.totalShards, equalTo(3));
        assertThat(response.successfulShards, equalTo(3));
        assertThat(response.skippedShards, equalTo(0));
        assertThat(response.failedShards, equalTo(0));
    }

    /**
     * With the timeout at zero the attempts are all that bounds the routing retry, as on {@code main}.
     */
    public void testDoesNotRetryMovedShardIndefinitelyAtZeroTimeout() {
        var attempt = new AtomicInteger(0);
        var response = safeGet(
            setup(
                List.of(targetShard(shard1, node1)),
                (node, shardIds, aliasFilters, listener) -> runWithDelay(() -> listener.onResponse(shardNotFound(shard1)))
            ).allowPartialResults(true).resolver(shardIds -> {
                attempt.incrementAndGet();
                return Map.of(shard1.shardId(), List.of(node2));
            }).routingWaitTimeout(TimeValue.ZERO).start()
        );
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
        var future = setup(
            List.of(targetShard(shard1, node1), targetShard(shard2, node2)),
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
        ).resolver(shardIds -> {
            attempt.incrementAndGet();
            return Map.of(shard1.shardId(), List.of());
        }).routingWaitTimeout(TimeValue.timeValueMillis(100)).start();
        expectThrows(NoShardAvailableActionException.class, containsString("no such shard"), future::actionGet);
        assertThat(attempt.get(), equalTo(2));
    }

    public void testRetryUnassignedShardWithPartialResults() {
        var attempt = new AtomicInteger(0);
        var response = safeGet(
            setup(
                List.of(targetShard(shard1, node1), targetShard(shard2, node2)),
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
            ).allowPartialResults(true).resolver(shardIds -> {
                attempt.incrementAndGet();
                return Map.of(shard1.shardId(), List.of());
            }).routingWaitTimeout(TimeValue.timeValueMillis(100)).start()
        );
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.skippedShards, equalTo(0));
        assertThat(response.failedShards, equalTo(1));
        assertThat(attempt.get(), equalTo(2));
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
     * A refusal leaves the node out for every shard, including one that already has it queued as a candidate. A lost connection only
     * does so for the shards of the request that lost it: another shard still tries that node, since the loss may have been transient.
     * <p>
     * After the refusal shard2 has no copy off the refuser. It waits for newer routing, here for a short timeout, and then fails:
     * the last try resolves it once more and still finds the refuser only.
     */
    public void testOnlyRefusalExcludesNodeForOtherShards() {
        boolean refused = randomBoolean();
        Exception unavailable = refused ? refusal(node1) : new NodeDisconnectedException(node1, ComputeService.DATA_ACTION_NAME);
        var resolved = ConcurrentCollections.<Set<ShardId>>newQueue();
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var node1Attempts = new AtomicInteger();
        // shard2 only moves past node2 once shard1 has been retried, so node1 has failed by then
        var shard1Retried = new SubscribableListener<Void>();
        var shard2Resolved = new SubscribableListener<Void>();
        var future = setup(
            List.of(targetShard(shard1, node1), targetShard(shard2, node2, node1)),
            (node, shardIds, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, shardIds));
                if (node.equals(node1)) {
                    if (node1Attempts.incrementAndGet() == 1) {
                        runWithDelay(() -> listener.onFailure(unavailable, false));
                    } else {
                        runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
                    }
                } else if (node.equals(node2)) {
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
                } else {
                    shard1Retried.onResponse(null);
                    Runnable respond = () -> runWithDelay(
                        () -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of()))
                    );
                    if (refused) {
                        // Nothing else completes until removing shard2's queued refuser causes a new resolution pass.
                        shard2Resolved.addListener(ActionListener.running(respond));
                    } else {
                        respond.run();
                    }
                }
            }
        ).allowPartialResults(true).resolver(shardIds -> {
            resolved.add(Set.copyOf(shardIds));
            // the routing places both shards on node1, alone for shard2
            if (shardIds.contains(shard1.shardId())) {
                return Map.of(shard1.shardId(), List.of(node1, node3));
            }
            shard2Resolved.onResponse(null);
            return Map.of(shard2.shardId(), List.of(node1));
        }).routingWaitTimeout(TimeValue.timeValueMillis(100)).start();
        var response = safeGet(future);
        if (refused) {
            // once when its copy failed, and once more for the last try when the wait timed out
            assertThat(resolved, contains(Set.of(shard1.shardId()), Set.of(shard2.shardId()), Set.of(shard2.shardId())));
        } else {
            assertThat(resolved, contains(Set.of(shard1.shardId())));
        }
        assertThat("shard1 never goes back to the node that failed it", sent, hasItem(nodeRequest(node3, shard1)));
        assertThat(response.totalShards, equalTo(2));
        if (refused) {
            assertThat(response.successfulShards, equalTo(1));
            assertThat(response.failedShards, equalTo(1));
            assertThat(node1Attempts.get(), equalTo(1));
        } else {
            assertThat(response.successfulShards, equalTo(2));
            assertThat(response.failedShards, equalTo(0));
            assertThat(node1Attempts.get(), equalTo(2));
            assertThat(sent, hasItem(nodeRequest(node1, shard2)));
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
     * A node that refuses the request or cannot be reached, before any page was received, makes the shard resolve again, and that node
     * is left out of the new copies.
     */
    public void testRetryRefusedOrDisconnectedNodeElsewhere() {
        var attempt = new AtomicInteger();
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        Exception unavailable = randomNodeUnavailableFailure(node1);
        var response = safeGet(sendRequests(randomBoolean(), -1, List.of(targetShard(shard1, node1)), shardIds -> {
            attempt.incrementAndGet();
            // the routing may still list the departing node
            return Map.of(shard1.shardId(), randomBoolean() ? List.of(node1, node2) : List.of(node2, node1));
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
     * If the only copy re-resolution finds is on the node that turned the request away, the shard waits for newer routing, here for
     * a short timeout, and then fails with that original failure. The last try at the timeout resolves once more. It does not go
     * back to a node that refused, and asks a node that could not be reached one more time.
     */
    public void testRefusedNodeIsNotRetriedWhenItHoldsTheOnlyCopy() {
        var attempt = new AtomicInteger();
        var sentTo = ConcurrentCollections.<DiscoveryNode>newQueue();
        Exception unavailable = randomNodeUnavailableFailure(node1);
        boolean allowPartialResults = randomBoolean();
        var future = setup(List.of(targetShard(shard1, node1), targetShard(shard2, node2)), (node, shardIds, aliasFilters, listener) -> {
            sentTo.add(node);
            if (node.equals(node1)) {
                runWithDelay(() -> listener.onFailure(unavailable, false));
            } else {
                runWithDelay(() -> listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of())));
            }
        }).allowPartialResults(allowPartialResults).resolver(shardIds -> {
            attempt.incrementAndGet();
            return Map.of(shard1.shardId(), List.of(node1));
        }).routingWaitTimeout(TimeValue.timeValueMillis(100)).start();
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
        assertThat(attempt.get(), equalTo(2));
        boolean refused = DataNodeRequestSender.classifyNodeFailure(unavailable) == NodeFailure.REFUSED;
        assertThat(
            "a refuser is asked once, an unreachable node once more by the last try",
            sentTo.stream().filter(node1::equals).count(),
            equalTo(refused ? 1L : 2L)
        );
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
     * another copy, fails the shard with the first failure. It is not resolved again, and so it does not wait for routing either.
     */
    public void testNoWaitAfterUnresolvableFailure() {
        RecordedMetrics recorded = new RecordedMetrics();
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        var response = safeGet(setup(List.of(targetShard(shard1, node1, node2)), (node, shardIds, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shardIds));
            if (node.equals(node1)) {
                runWithDelay(() -> listener.onFailure(new IOException("not retryable elsewhere"), false));
            } else {
                runWithDelay(() -> listener.onFailure(randomNodeUnavailableFailure(node), false));
            }
        }).allowPartialResults(true)
            .resolver(shardIds -> { throw new AssertionError("No shard resolution is expected here"); })
            .metrics(recorded)
            .start());
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).getCause(), instanceOf(IOException.class));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
        assertThat(recorded.routingWaits(), equalTo(0L));
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

    private static RemoteTransportException refusal(DiscoveryNode node) {
        return new RemoteTransportException("[" + node.getName() + "][open_exchange]", new NodeClosedException(node));
    }

    private static SendRequestTransportException localNodeClosed(DiscoveryNode target) {
        DiscoveryNode coordinator = DiscoveryNodeUtils.create("coordinator");
        return new SendRequestTransportException(target, ComputeService.DATA_ACTION_NAME, new NodeClosedException(coordinator));
    }

    /**
     * A refusal, or a lost connection either seen directly or when sending; the cause is what the shard failure reports.
     */
    private static Exception randomNodeUnavailableFailure(DiscoveryNode node) {
        return switch (between(0, 2)) {
            case 0 -> refusal(node);
            case 1 -> new SendRequestTransportException(
                node,
                ComputeService.DATA_ACTION_NAME,
                new NodeDisconnectedException(node, ComputeService.DATA_ACTION_NAME)
            );
            case 2 -> new SendRequestTransportException(node, "open_exchange", new NodeNotConnectedException(node, "not connected"));
            default -> throw new AssertionError("unexpected");
        };
    }

    public void testUntriedCopies() {
        ShardCopy active = new ShardCopy(node1, "active", CopyState.ACTIVE);
        ShardCopy target = new ShardCopy(node2, "target", CopyState.RELOCATION_TARGET);
        ShardCopy initializing = new ShardCopy(node3, "initializing", CopyState.INITIALIZING);

        var answer = DataNodeRequestSender.untriedCopies(List.of(active, target, initializing), Map.of(), Set.of());
        assertThat(answer.kind(), equalTo(Untried.COPY));
        assertThat(answer.nodes(), contains(node1, node2));

        answer = DataNodeRequestSender.untriedCopies(
            List.of(active, target, initializing),
            Map.of("active", true, "target", false),
            Set.of()
        );
        assertThat(answer.kind(), equalTo(Untried.NONE_YET));

        answer = DataNodeRequestSender.untriedCopies(List.of(initializing), Map.of(), Set.of());
        assertThat(answer.kind(), equalTo(Untried.NONE_YET));

        answer = DataNodeRequestSender.untriedCopies(List.of(active), Map.of("active", false), Set.of());
        assertThat("an initializing send is eligible once the same allocation starts", answer.kind(), equalTo(Untried.COPY));

        answer = DataNodeRequestSender.untriedCopies(List.of(active), Map.of(), Set.of(node1));
        assertThat(answer.kind(), equalTo(Untried.NONE_YET));

        assertThat(DataNodeRequestSender.untriedCopies(List.of(), Map.of(), Set.of()).kind(), equalTo(Untried.NONE_YET));
        assertThat(DataNodeRequestSender.untriedCopies(null, Map.of(), Set.of()).kind(), equalTo(Untried.GONE));
    }

    public void testLastTryAtTimeoutReachesRecoveredCopy() {
        AtomicInteger requests = new AtomicInteger();
        ComputeResponse response = safeGet(
            setup(
                List.of(targetShard(shard1, node1)),
                (node, shards, aliasFilters, listener) -> runWithDelay(
                    () -> listener.onResponse(requests.incrementAndGet() == 1 ? shardNotFound(shard1) : ok())
                )
            ).resolver(shardIds -> Map.of(shard1.shardId(), List.of(node1))).routingWaitTimeout(TimeValue.timeValueMillis(100)).start()
        );
        assertThat(response.successfulShards, equalTo(1));
        assertThat(requests.get(), equalTo(2));
    }

    public void testQueryFinishedDuringWaitSkipsShard() throws Exception {
        SubscribableListener<Void> queryFinished = new SubscribableListener<>();
        RecordedMetrics recorded = new RecordedMetrics();
        PlainActionFuture<ComputeResponse> future = setup(
            List.of(targetShard(shard1, node1)),
            (node, shards, aliasFilters, listener) -> runWithDelay(() -> listener.onResponse(shardNotFound(shard1)))
        ).allowPartialResults(true)
            .resolver(shardIds -> Map.of(shard1.shardId(), List.of(node1)))
            .queryFinished(queryFinished)
            .metrics(recorded)
            .start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        queryFinished.onResponse(null);
        ComputeResponse response = safeGet(future);
        assertThat(response.skippedShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
    }

    /**
     * The cancellation hook runs on the cancelling thread, which here is neither a search thread nor a transport thread. The wait
     * hands the outcome to the search pool, so the thread assertion of the sending loop holds.
     */
    public void testCancellationFromNonTransportThread() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        SenderSetup setup = setup(
            List.of(targetShard(shard1, node1)),
            (node, shards, aliasFilters, listener) -> runWithDelay(() -> listener.onResponse(shardNotFound(shard1)))
        ).resolver(shardIds -> Map.of(shard1.shardId(), List.of(node1))).metrics(recorded);
        PlainActionFuture<ComputeResponse> future = setup.start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        Thread cancellingThread = new Thread(() -> TaskCancelHelper.cancel(setup.task(), "test cancellation"));
        cancellingThread.start();
        cancellingThread.join();

        TaskCancelledException failure = expectThrows(TaskCancelledException.class, future::actionGet);
        assertThat(failure.getMessage(), containsString("test cancellation"));
    }

    /**
     * The query finishes on the thread that runs the pass, after shard1 was parked by a nested pass and before the wait is started.
     * shard1 is skipped by the start of its wait, shard2 by the request that found the query finished.
     */
    public void testSynchronousQueryFinishAfterParking() {
        DiscoveryNode coldNode = DiscoveryNodeUtils.builder("cold-node").roles(Set.of(DATA_COLD_NODE_ROLE)).build();
        SubscribableListener<Void> queryFinished = new SubscribableListener<>();
        ComputeResponse response = safeGet(
            setup(List.of(targetShard(shard1, node1), targetShard(shard2, coldNode)), (node, shards, aliasFilters, listener) -> {
                if (node.equals(node1)) {
                    listener.onResponse(shardNotFound(shard1));
                } else {
                    queryFinished.onResponse(null);
                    listener.onSkip();
                }
            }).allowPartialResults(true)
                .concurrentRequests(1)
                .resolver(shardIds -> Map.of(shard1.shardId(), List.of(node1)))
                .queryFinished(queryFinished)
                .start()
        );
        assertThat(response.skippedShards, equalTo(2));
        assertThat(response.failedShards, equalTo(0));
    }

    /**
     * Stale routing, then a newer state: the shard waits for the state that shows its new copy and is asked there, once per node.
     */
    public void testNewerRoutingEndsWaitAndRetriesOnNewCopy() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        PlainActionFuture<ComputeResponse> future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shards));
            runWithDelay(() -> listener.onResponse(node.equals(node1) ? shardNotFound(shard1) : ok()));
        }).metrics(recorded).start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        publish(Map.of(shard1.shardId(), List.of(started(node2, "allocation-2"))));
        ComputeResponse response = safeGet(future);
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
        assertThat(recorded.routingWaitTimeouts(), equalTo(0L));
    }

    /**
     * A finished query skips the shards that were about to be resolved again, and only those. A shard that failed for good keeps its
     * failure: it can be a failure after data was received, which a skip would hide.
     */
    public void testFinishedQueryKeepsFatalFailure() {
        for (boolean allowPartialResults : List.of(false, true)) {
            SubscribableListener<Void> queryFinished = new SubscribableListener<>();
            var future = setup(List.of(targetShard(shard1, node1), targetShard(shard2, node2)), (node, shards, aliasFilters, listener) -> {
                if (node.equals(node1)) {
                    runWithDelay(() -> {
                        queryFinished.onResponse(null);
                        listener.onFailure(new IOException("failed after data was received"), true);
                    });
                } else {
                    runWithDelay(() -> listener.onResponse(ok()));
                }
            }).allowPartialResults(allowPartialResults)
                .resolver(shardIds -> { throw new AssertionError("no shard resolution is expected here"); })
                .queryFinished(queryFinished)
                .start();
            if (allowPartialResults) {
                ComputeResponse response = safeGet(future);
                assertThat(response.totalShards, equalTo(2));
                assertThat(response.successfulShards, equalTo(1));
                assertThat("the failed shard is not counted as skipped", response.skippedShards, equalTo(0));
                assertThat(response.failedShards, equalTo(1));
                assertThat(response.failures, hasSize(1));
                assertThat(response.failures.get(0).shard().getShardId(), equalTo(shard1.shardId()));
                assertThat(response.failures.get(0).getCause(), instanceOf(IOException.class));
            } else {
                Exception e = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
                Throwable failure = ExceptionsHelper.unwrap(e, IOException.class);
                assertNotNull(e.toString(), failure);
                assertThat(failure.getMessage(), equalTo("failed after data was received"));
            }
        }
    }

    /**
     * The task is cancelled before the shard-not-available answer of the only copy is handled. The shard would stall under this
     * routing, but a cancelled query does not wait: the shard ends with the cancellation, at once, and not with its earlier failure.
     */
    public void testCancelledCandidateIsNotParked() {
        RecordedMetrics recorded = new RecordedMetrics();
        AtomicReference<CancellableTask> task = new AtomicReference<>();
        SenderSetup setup = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> runWithDelay(() -> {
            TaskCancelHelper.cancel(task.get(), "test cancellation");
            listener.onResponse(shardNotFound(shard1));
        })).resolver(shardIds -> Map.of(shard1.shardId(), List.of(node1))).metrics(recorded);
        task.set(setup.task());
        PlainActionFuture<ComputeResponse> future = setup.start();

        TaskCancelledException failure = expectThrows(TaskCancelledException.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(failure.getMessage(), containsString("test cancellation"));
        assertThat(recorded.routingWaits(), equalTo(0L));
    }

    /**
     * Disabling the routing wait restores the previous retry path. In particular, when a shard failure cancels the query before the
     * sender handles it, the sender still reports that failure rather than replacing it with the resulting cancellation.
     */
    public void testZeroWaitPreservesFailureThatCancelledQuery() {
        RecordedMetrics recorded = new RecordedMetrics();
        AtomicReference<CancellableTask> task = new AtomicReference<>();
        SenderSetup setup = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> runWithDelay(() -> {
            TaskCancelHelper.cancel(task.get(), "cancelled on failure");
            listener.onFailure(new NodeNotConnectedException(node, "not connected"), false);
        })).routingWaitTimeout(TimeValue.ZERO).resolver(shardIds -> Map.of(shard1.shardId(), List.of())).metrics(recorded);
        task.set(setup.task());
        PlainActionFuture<ComputeResponse> future = setup.start();

        expectThrows(NodeNotConnectedException.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(recorded.routingWaits(), equalTo(0L));
    }

    /**
     * A wait must not be armed once the sender has reported its failures: nothing would end it before its timeout, and it would
     * hold the compute listener until then.
     * <p>
     * shard1 is parked, and the thread that starts its wait is held inside the start, after the wait is listed as active and
     * before it reads the signals. Meanwhile shard2 fails for good on another thread, which reports the failures and ends the
     * listed wait. When the start goes on it must see the reported failure and stop: it counts no wait and arms neither a timer
     * nor an observer.
     * <p>
     * The start cannot be held any earlier without a hook in production code: nothing overridable runs between the pass handing
     * the wait over and the wait listing itself. A wait that fails in that gap is not listed yet, so only the start's own read of
     * the reported failure ends it; that interleaving is not forced here. {@link #testParkedShardSurvivesReportedFailure} covers
     * the outcome for a wait that is already armed.
     */
    public void testWaitNotStartedAfterReportedFailure() {
        requireTwoSearchThreads();
        RecordedMetrics recorded = new RecordedMetrics();
        CountDownLatch startHeld = new CountDownLatch(1);
        CountDownLatch releaseStart = new CountDownLatch(1);
        CountDownLatch shard2Failed = new CountDownLatch(1);
        SubscribableListener<Void> failShard2 = new SubscribableListener<>();
        var future = setup(List.of(targetShard(shard1, node1), targetShard(shard2, node2)), (node, shards, aliasFilters, listener) -> {
            if (node.equals(node1)) {
                runWithDelay(() -> listener.onResponse(shardNotFound(shard1)));
            } else {
                failShard2.addListener(ActionListener.running(() -> executor.execute(() -> {
                    listener.onFailure(new IOException("shard2 failed for good"), true);
                    shard2Failed.countDown();
                })));
            }
        }).resolver(shardIds -> Map.of(shard1.shardId(), List.of(node1))).metrics(recorded).onFirstWaitStarting(() -> {
            startHeld.countDown();
            safeAwait(releaseStart);
        }).start();

        safeAwait(startHeld);
        failShard2.onResponse(null);
        safeAwait(shard2Failed);
        releaseStart.countDown();

        Exception e = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        Throwable failure = ExceptionsHelper.unwrap(e, IOException.class);
        assertNotNull(e.toString(), failure);
        assertThat(failure.getMessage(), equalTo("shard2 failed for good"));
        assertThat(recorded.routingWaits(), equalTo(0L));
    }

    /**
     * What a resolution lists for a shard, in the order a retry tries the copies: active ones, relocation targets, and then
     * initializing copies, which only tell "none yet" from "gone". A shard, an index or a project that the state does not have
     * is a case and not an exception: the resolution also runs on the cluster applier thread.
     */
    public void testResolve() {
        Copy moving = relocating(node1, "allocation-1", node2);
        DiscoveryNode departed = DiscoveryNodeUtils.builder("departed").roles(Set.of(DATA_HOT_NODE_ROLE)).build();
        ClusterState state = routingState(
            ProjectId.DEFAULT,
            Map.of(
                shard1.shardId(),
                List.of(moving),
                shard2.shardId(),
                List.of(started(node3, "allocation-2"), initializing(node4, "allocation-3"), started(departed, "allocation-4"))
            )
        );
        Set<ShardId> shardIds = Set.of(shard1.shardId(), shard2.shardId(), shard3.shardId());

        Map<ShardId, List<ShardCopy>> resolved = DataNodeRequestSender.resolve(state, ProjectId.DEFAULT, shardIds);
        assertThat(
            resolved.get(shard1.shardId()),
            contains(
                new ShardCopy(node1, "allocation-1", CopyState.ACTIVE),
                new ShardCopy(node2, moving.targetAllocationId(), CopyState.RELOCATION_TARGET)
            )
        );
        assertThat(
            "a copy on a node the state does not list is left out",
            resolved.get(shard2.shardId()),
            contains(new ShardCopy(node3, "allocation-2", CopyState.ACTIVE), new ShardCopy(node4, "allocation-3", CopyState.INITIALIZING))
        );
        assertThat("the index has no such shard", resolved, not(hasKey(shard3.shardId())));
        assertThat(DataNodeRequestSender.untriedCopies(resolved.get(shard3.shardId()), Map.of(), Set.of()).kind(), equalTo(Untried.GONE));

        Map<ShardId, List<ShardCopy>> withoutIndex = DataNodeRequestSender.resolve(
            routingState(ProjectId.DEFAULT, Map.of()),
            ProjectId.DEFAULT,
            shardIds
        );
        assertThat(withoutIndex, anEmptyMap());
        assertThat(
            DataNodeRequestSender.untriedCopies(withoutIndex.get(shard1.shardId()), Map.of(), Set.of()).kind(),
            equalTo(Untried.GONE)
        );

        Map<ShardId, List<ShardCopy>> withoutProject = DataNodeRequestSender.resolve(state, ProjectId.fromId("deleted"), shardIds);
        assertThat(withoutProject, anEmptyMap());
        assertThat(
            DataNodeRequestSender.untriedCopies(withoutProject.get(shard1.shardId()), Map.of(), Set.of()).kind(),
            equalTo(Untried.GONE)
        );
    }

    /**
     * The copy a request goes to is read from the applied state when the request is sent: an active copy, an initializing one or
     * the target of a relocation. A node on which the state shows no copy of the shard gives nothing to record.
     */
    public void testSentCopy() {
        Copy moving = relocating(node1, "allocation-1", node2);
        ClusterState state = routingState(
            ProjectId.DEFAULT,
            Map.of(
                shard1.shardId(),
                List.of(moving),
                shard2.shardId(),
                List.of(started(node3, "allocation-2"), initializing(node4, "allocation-3"))
            )
        );
        Sender unused = (node, shardIds, aliasFilters, listener) -> { throw new AssertionError("no request is expected here"); };
        DataNodeRequestSender sender = setup(List.of(), unused).build();

        assertThat(sender.sentCopy(state, shard1.shardId(), node1), equalTo(new SentCopy("allocation-1", true)));
        assertThat(sender.sentCopy(state, shard1.shardId(), node2), equalTo(new SentCopy(moving.targetAllocationId(), false)));
        assertThat(sender.sentCopy(state, shard2.shardId(), node3), equalTo(new SentCopy("allocation-2", true)));
        assertThat(sender.sentCopy(state, shard2.shardId(), node4), equalTo(new SentCopy("allocation-3", false)));
        assertNull("no copy on the node", sender.sentCopy(state, shard2.shardId(), node1));
        assertNull("no such shard", sender.sentCopy(state, shard3.shardId(), node1));
        assertNull("no such index", sender.sentCopy(routingState(ProjectId.DEFAULT, Map.of()), shard1.shardId(), node1));

        ProjectId deleted = ProjectId.fromId("deleted");
        DataNodeRequestSender senderOfDeletedProject = withProjectHeader(
            deleted,
            () -> setup(List.of(), unused).projectResolver(TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext())).build()
        );
        assertNull("no such project", senderOfDeletedProject.sentCopy(state, shard1.shardId(), node1));
    }

    /**
     * A re-resolution under routing that shows the shard relocating goes on to the relocation target, without a wait.
     */
    public void testReResolutionTriesRelocationTarget() {
        RecordedMetrics recorded = new RecordedMetrics();
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        publish(Map.of(shard1.shardId(), List.of(relocating(node1, "allocation-1", node2))));
        ComputeResponse response = safeGet(setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shards));
            runWithDelay(() -> listener.onResponse(node.equals(node1) ? shardNotFound(shard1) : ok()));
        }).allowPartialResults(randomBoolean()).metrics(recorded).start());
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
        assertThat(recorded.routingWaits(), equalTo(0L));
    }

    /**
     * Both the relocating copy and its target answer "not available": the hand-off is under way. The target was tried while the
     * routing showed it initializing, so it counts as untried again once a state shows it started, under the same allocation id.
     */
    public void testCopyTriedWhileInitializingIsRetriedOnceStarted() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        AtomicInteger requestsToTarget = new AtomicInteger();
        Copy moving = relocating(node1, "allocation-1", node2);
        publish(Map.of(shard1.shardId(), List.of(moving)));
        var future = setup(List.of(targetShard(shard1, node1, node2)), (node, shards, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shards));
            boolean available = node.equals(node2) && requestsToTarget.incrementAndGet() == 2;
            runWithDelay(() -> listener.onResponse(available ? ok() : shardNotFound(shard1)));
        }).metrics(recorded).start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
        publish(Map.of(shard1.shardId(), List.of(started(node2, moving.targetAllocationId()))));

        ComputeResponse response = safeGet(future);
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1), nodeRequest(node2, shard1)));
    }

    /**
     * As {@link #testCopyTriedWhileInitializingIsRetriedOnceStarted}, with the state that shows the target started applied before
     * its failure is handled. What counts is how the routing showed the copy when the request was sent, which is recorded then:
     * read at the failure, the copy would look tried while active, and the shard would wait for routing that is already there.
     */
    public void testStartedStateAppliedBeforeFailureIsHandled() {
        RecordedMetrics recorded = new RecordedMetrics();
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        AtomicInteger requestsToTarget = new AtomicInteger();
        Copy moving = relocating(node1, "allocation-1", node2);
        publish(Map.of(shard1.shardId(), List.of(moving)));
        ComputeResponse response = safeGet(setup(List.of(targetShard(shard1, node1, node2)), (node, shards, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shards));
            if (node.equals(node1)) {
                runWithDelay(() -> listener.onResponse(shardNotFound(shard1)));
            } else if (requestsToTarget.incrementAndGet() == 1) {
                runWithDelay(() -> {
                    publish(Map.of(shard1.shardId(), List.of(started(node2, moving.targetAllocationId()))));
                    listener.onResponse(shardNotFound(shard1));
                });
            } else {
                runWithDelay(() -> listener.onResponse(ok()));
            }
        }).metrics(recorded).start());
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1), nodeRequest(node2, shard1)));
        assertThat(recorded.routingWaits(), equalTo(0L));
    }

    /**
     * Only the open is refused by a node that stopped admitting exchanges. A {@link NodeClosedException} that fails the data node
     * request came from further down, a lookup for example, and is not read as a refusal. Passed to the sender classified so, the
     * failure does not make the shard resolve again, and does not leave the node out for another shard that is resolved onto it
     * later.
     */
    public void testDataRequestFailureIsNeverARefusal() {
        Exception lookupFailure = new RemoteTransportException(
            "remote",
            new RemoteTransportException("lookup", new NodeClosedException(node3))
        );
        assertThat(DataNodeRequestSender.classifyDataRequestFailure(lookupFailure), equalTo(NodeFailure.OTHER));
        assertThat(DataNodeRequestSender.classifyDataRequestFailure(refusal(node1)), equalTo(NodeFailure.OTHER));
        assertThat(
            DataNodeRequestSender.classifyDataRequestFailure(new NodeDisconnectedException(node1, ComputeService.DATA_ACTION_NAME)),
            equalTo(NodeFailure.UNREACHABLE)
        );

        var resolved = ConcurrentCollections.<Set<ShardId>>newQueue();
        var sent = ConcurrentCollections.<NodeRequest>newQueue();
        // shard2 is only resolved again once the failure of shard1 has been handled
        var shard1Failed = new SubscribableListener<Void>();
        var response = safeGet(
            setup(List.of(targetShard(shard1, node1), targetShard(shard2, node2)), (node, shards, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, shards));
                if (node.equals(node1) && shards.contains(shard1)) {
                    runWithDelay(() -> {
                        listener.onFailure(lookupFailure, false, DataNodeRequestSender.classifyDataRequestFailure(lookupFailure));
                        shard1Failed.onResponse(null);
                    });
                } else if (node.equals(node2)) {
                    shard1Failed.addListener(ActionListener.running(() -> runWithDelay(() -> listener.onResponse(shardNotFound(shard2)))));
                } else {
                    runWithDelay(() -> listener.onResponse(ok()));
                }
            }).allowPartialResults(true).resolver(shardIds -> {
                resolved.add(Set.copyOf(shardIds));
                return Map.of(shard2.shardId(), List.of(node1));
            }).start()
        );
        assertThat("shard1 is not resolved again", resolved, contains(Set.of(shard2.shardId())));
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).shard().getShardId(), equalTo(shard1.shardId()));
        assertThat(response.successfulShards, equalTo(1));
        assertThat("node1 still serves shard2", sent, hasItem(nodeRequest(node1, shard2)));
    }

    /** Runs the action on this thread with the request header that names the project, as a request for that project arrives. */
    private <T> T withProjectHeader(ProjectId projectId, Supplier<T> action) {
        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.newStoredContext()) {
            threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId.id());
            return action.get();
        }
    }

    /**
     * The only copy answers "not available" and the routing has not changed: the coordinator is on stale routing. The shard is
     * resolved once and then waits. It does not spend the attempts on asking the same copy again.
     */
    public void testShardNotAvailableUnderStaleRoutingWaits() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        AtomicInteger resolutions = new AtomicInteger();
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        var future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shards));
            runWithDelay(() -> listener.onResponse(node.equals(node1) ? shardNotFound(shard1) : ok()));
        }).allowPartialResults(randomBoolean()).metrics(recorded).onResolve(resolved -> {
            resolutions.incrementAndGet();
            return resolved;
        }).start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        assertThat(resolutions.get(), equalTo(1));
        assertThat(sent, contains(nodeRequest(node1, shard1)));

        publish(Map.of(shard1.shardId(), List.of(started(node2, "allocation-2"))));
        ComputeResponse response = safeGet(future);
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
        assertThat(resolutions.get(), equalTo(2));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
    }

    /**
     * A newer cluster state that shows the same routing does not end the wait: nothing is handed back to the sending loop.
     */
    public void testUnrelatedStateDoesNotEndWait() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        GatedExecutor handOff = new GatedExecutor(executor);
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        var future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shards));
            runWithDelay(() -> listener.onResponse(node.equals(node1) ? shardNotFound(shard1) : ok()));
        }).metrics(recorded).handOffExecutor(handOff).start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        assertThat("the wait has not ended", handOff.submitted(), equalTo(0));
        assertFalse(future.isDone());
        assertThat(sent, contains(nodeRequest(node1, shard1)));

        publish(Map.of(shard1.shardId(), List.of(started(node2, "allocation-2"))));
        ComputeResponse response = safeGet(future);
        assertThat(response.successfulShards, equalTo(1));
        assertThat(handOff.submitted(), equalTo(1));
        assertThat(recorded.routingWaits(), equalTo(1L));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
    }

    /**
     * The shard comes back on the node that failed it, under a new allocation id. That is a copy that was not tried, so the wait
     * ends and the node is asked a second time.
     */
    public void testSameNodeNewAllocationEndsWait() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        AtomicInteger requests = new AtomicInteger();
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        var future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
            assertThat(node, equalTo(node1));
            boolean first = requests.incrementAndGet() == 1;
            runWithDelay(() -> listener.onResponse(first ? shardNotFound(shard1) : ok()));
        }).metrics(recorded).start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-2"))));
        ComputeResponse response = safeGet(future);
        assertThat(response.successfulShards, equalTo(1));
        assertThat(requests.get(), equalTo(2));
    }

    /**
     * A response for another shard runs the sending loop while shard1 waits. The loop must leave shard1 alone: it has no node
     * left and is not resolved again, which outside a wait marks a shard as failed.
     */
    public void testResponseDuringWaitLeavesWaitingShardParked() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        SubscribableListener<Void> releaseShard2 = new SubscribableListener<>();
        CountDownLatch shard2Handled = new CountDownLatch(1);
        publish(
            Map.of(shard1.shardId(), List.of(started(node1, "allocation-1")), shard2.shardId(), List.of(started(node2, "allocation-2")))
        );
        var future = setup(List.of(targetShard(shard1, node1), targetShard(shard2, node2)), (node, shards, aliasFilters, listener) -> {
            if (node.equals(node1)) {
                runWithDelay(() -> listener.onResponse(shardNotFound(shard1)));
            } else if (node.equals(node2)) {
                releaseShard2.addListener(ActionListener.running(() -> executor.execute(() -> {
                    listener.onResponse(ok());
                    shard2Handled.countDown();
                })));
            } else {
                runWithDelay(() -> listener.onResponse(ok()));
            }
        }).metrics(recorded).start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        releaseShard2.onResponse(null);
        safeAwait(shard2Handled);
        assertFalse("shard1 is still waiting, and was not failed", future.isDone());

        publish(
            Map.of(shard1.shardId(), List.of(started(node3, "allocation-3")), shard2.shardId(), List.of(started(node2, "allocation-2")))
        );
        ComputeResponse response = safeGet(future);
        assertThat(response.totalShards, equalTo(2));
        assertThat(response.successfulShards, equalTo(2));
        assertThat(response.failedShards, equalTo(0));
    }

    /**
     * The waiting shard is the last pending work: no request is in flight and no pass is running. The wait holds the compute
     * listener, so the query does not complete before the wait has ended and the shard was served.
     */
    public void testQueryCompletesOnlyAfterWaitEnds() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        CountDownLatch lastResponseHandled = new CountDownLatch(1);
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        var future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> runWithDelay(() -> {
            if (node.equals(node1)) {
                listener.onResponse(shardNotFound(shard1));
                lastResponseHandled.countDown();
            } else {
                listener.onResponse(ok());
            }
        })).allowPartialResults(randomBoolean()).metrics(recorded).start();

        safeAwait(lastResponseHandled);
        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        publish(Map.of(shard1.shardId(), List.of(started(node2, "allocation-2"))));
        ComputeResponse response = safeGet(future);
        assertThat(response.successfulShards, equalTo(1));
        assertThat(response.failedShards, equalTo(0));
    }

    /**
     * The routing does not change and the copy keeps answering "not available". When the wait times out the copy gets one last
     * try, and then the shard fails with the failure it had before the wait.
     */
    public void testWaitTimeoutReportsOriginalFailure() {
        for (boolean allowPartialResults : List.of(false, true)) {
            RecordedMetrics recorded = new RecordedMetrics();
            AtomicInteger requests = new AtomicInteger();
            publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
            var future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
                assertThat(node, equalTo(node1));
                requests.incrementAndGet();
                runWithDelay(() -> listener.onResponse(shardNotFound(shard1)));
            }).allowPartialResults(allowPartialResults).routingWaitTimeout(TimeValue.timeValueMillis(100)).metrics(recorded).start();

            if (allowPartialResults) {
                ComputeResponse response = safeGet(future);
                assertThat(response.totalShards, equalTo(1));
                assertThat(response.successfulShards, equalTo(0));
                assertThat(response.failedShards, equalTo(1));
                assertThat(response.failures, hasSize(1));
                assertThat(response.failures.get(0).getCause(), instanceOf(NoShardAvailableActionException.class));
                assertThat(response.failures.get(0).reason(), containsString("no such shard"));
            } else {
                expectThrows(
                    NoShardAvailableActionException.class,
                    containsString("no such shard"),
                    () -> future.actionGet(10, TimeUnit.SECONDS)
                );
            }
            assertThat("asked again by the last try, once", requests.get(), equalTo(2));
            assertThat(recorded.routingWaits(), equalTo(1L));
            assertThat(recorded.routingWaitTimeouts(), equalTo(1L));
            assertThat(recorded.attemptsExhausted(), equalTo(0L));
        }
    }

    /**
     * A cancellation ends the wait at once, long before its timeout.
     */
    public void testCancellationEndsWait() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        SenderSetup setup = setup(
            List.of(targetShard(shard1, node1)),
            (node, shards, aliasFilters, listener) -> runWithDelay(() -> listener.onResponse(shardNotFound(shard1)))
        ).metrics(recorded);
        PlainActionFuture<ComputeResponse> future = setup.start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        TaskCancelHelper.cancel(setup.task(), "test cancellation");

        TaskCancelledException failure = expectThrows(TaskCancelledException.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(failure.getMessage(), containsString("test cancellation"));
        assertThat(recorded.routingWaitTimeouts(), equalTo(0L));
    }

    /**
     * A state that shows the new copy is applied after the shard was resolved and before its wait is started. The wait observes
     * from the state the shard was resolved on, so it sees that state at once and needs no further one.
     */
    public void testStateAppliedBeforeWaitStartsEndsWait() {
        RecordedMetrics recorded = new RecordedMetrics();
        AtomicBoolean published = new AtomicBoolean();
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        ComputeResponse response = safeGet(setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, shards));
            runWithDelay(() -> listener.onResponse(node.equals(node1) ? shardNotFound(shard1) : ok()));
        }).metrics(recorded).onResolve(resolved -> {
            if (published.compareAndSet(false, true)) {
                publish(Map.of(shard1.shardId(), List.of(started(node2, "allocation-2"))));
            }
            // what the pass goes by is still the state it read before
            return resolved;
        }).start());
        assertThat(response.successfulShards, equalTo(1));
        assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
        assertThat(recorded.routingWaits(), equalTo(1L));
        assertThat(recorded.routingWaitTimeouts(), equalTo(0L));
    }

    /**
     * The index is deleted while the request is out. The routing has no copy of the shard and none will come: the shard fails in
     * the pass that handles the failure, with that failure, and without a wait.
     */
    public void testDeletedIndexFailsWithoutWait() {
        RecordedMetrics recorded = new RecordedMetrics();
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        var future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> runWithDelay(() -> {
            publish(Map.of());
            listener.onResponse(shardNotFound(shard1));
        })).metrics(recorded).start();

        expectThrows(NoShardAvailableActionException.class, containsString("no such shard"), () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(recorded.routingWaits(), equalTo(0L));
    }

    /**
     * The index is deleted while its shard waits. The state without the index ends the wait, and the shard fails with the
     * failure it had: the predicate of the wait does not throw on a routing table that lacks the index.
     */
    public void testIndexDeletedDuringWaitEndsWait() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        var future = setup(
            List.of(targetShard(shard1, node1)),
            (node, shards, aliasFilters, listener) -> runWithDelay(() -> listener.onResponse(shardNotFound(shard1)))
        ).metrics(recorded).start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        publish(Map.of());
        expectThrows(NoShardAvailableActionException.class, containsString("no such shard"), () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(recorded.routingWaitTimeouts(), equalTo(0L));
    }

    /**
     * One refused request stalls all of its shards in the same pass. They share one wait, and so one observer on the cluster
     * applier, however many they are.
     */
    public void testManyStalledShardsShareOneWait() throws Exception {
        List<DataNodeRequest.Shard> shards = new ArrayList<>();
        Map<ShardId, List<Copy>> onNode1 = new HashMap<>();
        Map<ShardId, List<Copy>> onNode2 = new HashMap<>();
        for (int i = 1; i <= 50; i++) {
            DataNodeRequest.Shard shard = new DataNodeRequest.Shard(new ShardId("index", "n/a", i), SplitShardCountSummary.UNSET);
            shards.add(shard);
            onNode1.put(shard.shardId(), List.of(started(node1, "old-" + i)));
            onNode2.put(shard.shardId(), List.of(started(node2, "new-" + i)));
        }
        RecordedMetrics recorded = new RecordedMetrics();
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        publish(onNode1);
        var future = setup(shards.stream().map(shard -> targetShard(shard, node1)).toList(), (node, requested, aliasFilters, listener) -> {
            sent.add(nodeRequest(node, requested));
            if (node.equals(node1)) {
                runWithDelay(() -> listener.onFailure(refusal(node1), false));
            } else {
                runWithDelay(() -> listener.onResponse(ok()));
            }
        }).allowPartialResults(randomBoolean()).metrics(recorded).start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        publish(onNode2);
        ComputeResponse response = safeGet(future);
        assertThat(response.totalShards, equalTo(50));
        assertThat(response.successfulShards, equalTo(50));
        assertThat(response.failedShards, equalTo(0));
        assertThat(sent, contains(nodeRequest(node1, shards), nodeRequest(node2, shards)));
        assertThat(recorded.routingWaits(), equalTo(1L));
    }

    /**
     * A query of a project other than the default one, resolved through the request header as in production. The predicate of
     * the wait runs on the cluster applier thread, which does not carry that header: it has to go by the project the sender
     * captured when it was built. A lookup through the resolver there would read the default project, find no index, and end
     * the wait on a state that changes nothing.
     * <p>
     * Then the project is removed: the shard fails as gone, and the predicate does not throw.
     */
    public void testWaitInNonDefaultProject() throws Exception {
        ProjectId projectId = randomUniqueProjectId();
        for (boolean projectRemoved : List.of(false, true)) {
            RecordedMetrics recorded = new RecordedMetrics();
            GatedExecutor handOff = new GatedExecutor(executor);
            Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
            publish(projectId, Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
            PlainActionFuture<ComputeResponse> future = withProjectHeader(
                projectId,
                () -> setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
                    sent.add(nodeRequest(node, shards));
                    runWithDelay(() -> listener.onResponse(node.equals(node1) ? shardNotFound(shard1) : ok()));
                }).projectResolver(TestProjectResolvers.usingRequestHeader(threadPool.getThreadContext()))
                    .metrics(recorded)
                    .handOffExecutor(handOff)
                    .start()
            );

            assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
            publish(projectId, Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
            assertThat("unchanged routing of the project does not end the wait", handOff.submitted(), equalTo(0));
            assertFalse(future.isDone());

            if (projectRemoved) {
                publish(null, Map.of());
                expectThrows(
                    NoShardAvailableActionException.class,
                    containsString("no such shard"),
                    () -> future.actionGet(10, TimeUnit.SECONDS)
                );
                assertThat(sent, contains(nodeRequest(node1, shard1)));
            } else {
                publish(projectId, Map.of(shard1.shardId(), List.of(started(node2, "allocation-2"))));
                ComputeResponse response = safeGet(future);
                assertThat(response.successfulShards, equalTo(1));
                assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
            }
            assertThat(handOff.submitted(), equalTo(1));
        }
    }

    /**
     * The outcome of a wait sits in the queue of the search pool while the query finishes or is cancelled. Only one outcome is
     * ever queued per wait, so the pass that applies it reads both signals again. Without that the queued timeout would give the
     * shard a last try, and the queued routing change, here a deleted index, would fail it: for a query that needs no more data,
     * or one that was cancelled.
     */
    public void testQueuedOutcomeAfterQueryFinished() throws Exception {
        for (boolean timedOut : List.of(true, false)) {
            for (boolean cancelled : List.of(false, true)) {
                RecordedMetrics recorded = new RecordedMetrics();
                GatedExecutor handOff = new GatedExecutor(executor);
                handOff.hold();
                SubscribableListener<Void> queryFinished = new SubscribableListener<>();
                AtomicInteger requests = new AtomicInteger();
                publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
                SenderSetup setup = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
                    requests.incrementAndGet();
                    runWithDelay(() -> listener.onResponse(shardNotFound(shard1)));
                }).allowPartialResults(cancelled == false && randomBoolean())
                    .routingWaitTimeout(timedOut ? TimeValue.timeValueMillis(100) : DEFAULT_ROUTING_WAIT_TIMEOUT)
                    .queryFinished(queryFinished)
                    .metrics(recorded)
                    .handOffExecutor(handOff);
                PlainActionFuture<ComputeResponse> future = setup.start();

                assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
                if (timedOut) {
                    assertBusy(() -> assertThat(recorded.routingWaitTimeouts(), equalTo(1L)));
                } else {
                    publish(Map.of());
                }
                assertBusy(() -> assertThat("the outcome is queued", handOff.submitted(), equalTo(1)));
                assertFalse(future.isDone());

                if (cancelled) {
                    TaskCancelHelper.cancel(setup.task(), "test cancellation");
                } else {
                    queryFinished.onResponse(null);
                }
                handOff.release();

                if (cancelled) {
                    TaskCancelledException failure = expectThrows(
                        TaskCancelledException.class,
                        () -> future.actionGet(10, TimeUnit.SECONDS)
                    );
                    assertThat(failure.getMessage(), containsString("test cancellation"));
                } else {
                    ComputeResponse response = safeGet(future);
                    assertThat(response.totalShards, equalTo(1));
                    assertThat(response.skippedShards, equalTo(1));
                    assertThat(response.failedShards, equalTo(0));
                    assertThat(response.failures, empty());
                }
                assertThat("no last try for a query that is over", requests.get(), equalTo(1));
                assertThat(handOff.submitted(), equalTo(1));
            }
        }
    }

    /**
     * Without partial results, a fatal failure of shard2 fails the query while shard1 waits. Reporting the failures empties the
     * failures of every shard, shard1's too. The parked shard must not be picked up as one that has no node and no failure, which
     * would add a "no shard copies found" for it, and its wait must end with the pass and not hold the query until its timeout.
     * <p>
     * The failure shard1 had before it waited is reported along with shard2's, as every retained failure is when a query fails.
     */
    public void testParkedShardSurvivesReportedFailure() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        SubscribableListener<Void> failShard2 = new SubscribableListener<>();
        var future = setup(List.of(targetShard(shard1, node1), targetShard(shard2, node2)), (node, shards, aliasFilters, listener) -> {
            if (node.equals(node1)) {
                runWithDelay(() -> listener.onResponse(shardNotFound(shard1)));
            } else {
                failShard2.addListener(
                    ActionListener.running(() -> runWithDelay(() -> listener.onFailure(new IOException("shard2 failed for good"), true)))
                );
            }
        }).resolver(shardIds -> Map.of(shard1.shardId(), List.of(node1))).metrics(recorded).start();

        assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
        assertFalse(future.isDone());
        failShard2.onResponse(null);

        Exception e = expectThrows(Exception.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        Throwable failure = ExceptionsHelper.unwrap(e, IOException.class);
        assertNotNull(e.toString(), failure);
        assertThat(failure.getMessage(), equalTo("shard2 failed for good"));
        for (Throwable suppressed : failure.getSuppressed()) {
            assertThat(suppressed.getMessage(), not(containsString("no shard copies found")));
        }
        assertThat(recorded.routingWaitTimeouts(), equalTo(0L));
    }

    /**
     * What is left of a wait once it has ended, as far as it shows from outside. A wait hands exactly one outcome back to the
     * sending loop, whatever ends it: its timer, the timeout of its observer, a cancellation or the finished query. The observer
     * cannot be cancelled and stays with the cluster applier; a state it would have accepted, published later, does nothing.
     * And the sender registers its cancellation and query-finished hooks once, not once per wait.
     * <p>
     * That the lingering observer callback holds no reference to the wait is not visible from here: the callback is private to
     * the sender and the applier does not expose its listeners. An outcome delivered twice would show; a retained reference does
     * not.
     */
    public void testEndedWaitReleasesItsState() throws Exception {
        enum Ending {
            TIMER,
            CANCELLATION,
            QUERY_FINISHED
        }
        for (Ending ending : Ending.values()) {
            RecordedMetrics recorded = new RecordedMetrics();
            GatedExecutor handOff = new GatedExecutor(executor);
            SubscribableListener<Void> queryFinished = new SubscribableListener<>();
            AtomicInteger requests = new AtomicInteger();
            publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
            SenderSetup setup = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
                requests.incrementAndGet();
                runWithDelay(() -> listener.onResponse(shardNotFound(shard1)));
            }).allowPartialResults(true)
                .routingWaitTimeout(ending == Ending.TIMER ? TimeValue.timeValueMillis(100) : DEFAULT_ROUTING_WAIT_TIMEOUT)
                .queryFinished(queryFinished)
                .metrics(recorded)
                .handOffExecutor(handOff);
            PlainActionFuture<ComputeResponse> future = setup.start();

            assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
            switch (ending) {
                case TIMER -> {
                }
                case CANCELLATION -> TaskCancelHelper.cancel(setup.task(), "test cancellation");
                case QUERY_FINISHED -> queryFinished.onResponse(null);
            }
            ComputeResponse response = safeGet(future);
            assertThat(response.successfulShards, equalTo(0));
            int requestsWhenDone = requests.get();
            assertThat(requestsWhenDone, equalTo(ending == Ending.TIMER ? 2 : 1));

            // a copy that was not tried: the predicate of the ended wait would have accepted this state
            publish(Map.of(shard1.shardId(), List.of(started(node2, "allocation-2"))));
            assertThat("one outcome per wait", handOff.submitted(), equalTo(1));
            assertThat(requests.get(), equalTo(requestsWhenDone));
            assertThat(setup.queryFinishedListeners(), equalTo(1));
        }

        // three waits of one sender: each new copy answers "not available" too, until the last one
        RecordedMetrics recorded = new RecordedMetrics();
        List<DiscoveryNode> homes = List.of(node1, node2, node3, node4);
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        SenderSetup setup = setup(
            List.of(targetShard(shard1, node1)),
            (node, shards, aliasFilters, listener) -> runWithDelay(
                () -> listener.onResponse(node.equals(node4) ? ok() : shardNotFound(shard1))
            )
        ).metrics(recorded);
        PlainActionFuture<ComputeResponse> future = setup.start();
        for (int wait = 1; wait <= 3; wait++) {
            long expectedWaits = wait;
            assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(expectedWaits)));
            assertFalse(future.isDone());
            publish(Map.of(shard1.shardId(), List.of(started(homes.get(wait), "allocation-" + (wait + 1)))));
        }
        assertThat(safeGet(future).successfulShards, equalTo(1));
        assertThat(recorded.routingWaits(), equalTo(3L));
        assertThat("the hooks are registered with the first wait only", setup.queryFinishedListeners(), equalTo(1));
    }

    /**
     * Three shards move twice while the query runs, and each move reaches the coordinator as a published state. Whether a shard
     * waits for the state or finds it applied when its copy fails, it follows the move, and no copy is asked twice.
     */
    public void testRetryMultipleMovedShards() throws Exception {
        List<DataNodeRequest.Shard> shards = List.of(shard1, shard2, shard3);
        List<DiscoveryNode> nodes = List.of(node1, node2, node3);
        IntFunction<Map<ShardId, List<Copy>>> placement = round -> {
            Map<ShardId, List<Copy>> routing = new HashMap<>();
            for (int i = 0; i < shards.size(); i++) {
                routing.put(shards.get(i).shardId(), List.of(started(nodes.get((i + round) % nodes.size()), "round-" + round + "-" + i)));
            }
            return routing;
        };
        AtomicBoolean settled = new AtomicBoolean();
        AtomicInteger unavailableAnswers = new AtomicInteger();
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        publish(placement.apply(0));
        var future = setup(
            List.of(targetShard(shard1, node1), targetShard(shard2, node2), targetShard(shard3, node3)),
            (node, requested, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, requested));
                runWithDelay(() -> {
                    // a node only has a shard once the moves are over and the routing in effect places the shard on it
                    Map<ShardId, Exception> failures = new HashMap<>();
                    for (DataNodeRequest.Shard shard : requested) {
                        DiscoveryNode home = nodes.get((shards.indexOf(shard) + 2) % nodes.size());
                        if (settled.get() == false || home.equals(node) == false) {
                            failures.put(shard.shardId(), new ShardNotFoundException(shard.shardId()));
                        }
                    }
                    listener.onResponse(new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, failures));
                    unavailableAnswers.addAndGet(failures.size());
                });
            }
        ).allowPartialResults(randomBoolean()).start();

        assertBusy(() -> assertThat(unavailableAnswers.get(), equalTo(3)));
        publish(placement.apply(1));
        assertBusy(() -> assertThat(unavailableAnswers.get(), equalTo(6)));
        settled.set(true);
        publish(placement.apply(2));

        ComputeResponse response = safeGet(future);
        assertThat(response.totalShards, equalTo(3));
        assertThat(response.successfulShards, equalTo(3));
        assertThat(response.skippedShards, equalTo(0));
        assertThat(response.failedShards, equalTo(0));
        Map<DataNodeRequest.Shard, List<DiscoveryNode>> asked = new HashMap<>();
        for (NodeRequest request : sent) {
            for (DataNodeRequest.Shard shard : request.shards()) {
                asked.computeIfAbsent(shard, ignored -> new ArrayList<>()).add(request.node());
            }
        }
        for (int i = 0; i < shards.size(); i++) {
            assertThat(
                "each copy of a shard is asked once, in the order of the moves",
                asked.get(shards.get(i)),
                contains(nodes.get(i), nodes.get((i + 1) % nodes.size()), nodes.get((i + 2) % nodes.size()))
            );
        }
    }

    /**
     * Lets a test put several answers in front of one pass of the sending loop. The requests of the first pass are collected, and
     * the last of them is held inside {@code sendRequest}, where the pass holds the sending lock. Answers delivered meanwhile from
     * the test thread cannot run a pass of their own: they are all handled by the next pass of the thread that is held.
     */
    private static final class HeldPass {
        private final Map<DiscoveryNode, NodeListener> listeners = ConcurrentCollections.newConcurrentMap();
        private final AtomicInteger remaining;
        private final CountDownLatch allSent;
        private final CountDownLatch released = new CountDownLatch(1);

        HeldPass(int requests) {
            this.remaining = new AtomicInteger(requests);
            this.allSent = new CountDownLatch(requests);
        }

        /** Called for every request. Takes the ones of the first pass, and blocks on the last of them until {@link #release}. */
        boolean take(DiscoveryNode node, NodeListener listener) {
            int left = remaining.getAndDecrement();
            if (left <= 0) {
                return false;
            }
            listeners.put(node, listener);
            allSent.countDown();
            if (left == 1) {
                safeAwait(released);
            }
            return true;
        }

        /** The listener of the request of the first pass to the node, once the whole pass has been sent and is held. */
        NodeListener listener(DiscoveryNode node) {
            safeAwait(allSent);
            return Objects.requireNonNull(listeners.get(node));
        }

        void release() {
            released.countDown();
        }
    }

    /**
     * With a single attempt: a pass that only moves a refused shard to another copy and parks a stalled one costs nothing. The
     * attempt is still there when newer routing gives the parked shard a copy, which is what takes it. After that a third shard
     * that stalls fails at once, without a wait.
     */
    public void testRefusalAndParkingPassesAreFree() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        HeldPass firstPass = new HeldPass(3);
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        // shard1 has a second copy the first resolution did not list, shard2 and shard3 have a single copy
        publish(
            Map.of(
                shard1.shardId(),
                List.of(started(node1, "shard1-a"), started(node3, "shard1-b")),
                shard2.shardId(),
                List.of(started(node2, "shard2-a")),
                shard3.shardId(),
                List.of(started(node4, "shard3-a"))
            )
        );
        var future = setup(
            List.of(targetShard(shard1, node1), targetShard(shard2, node2), targetShard(shard3, node4)),
            (node, shards, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, shards));
                if (firstPass.take(node, listener) == false) {
                    runWithDelay(() -> listener.onResponse(ok()));
                }
            }
        ).allowPartialResults(true).attempts(1).metrics(recorded).start();

        // one pass: shard1 was refused and has another copy, shard2 stalls
        NodeListener shard3Request = firstPass.listener(node4);
        firstPass.listener(node1).onFailure(refusal(node1), false);
        firstPass.listener(node2).onResponse(shardNotFound(shard2));
        firstPass.release();
        assertBusy(() -> {
            assertThat(recorded.routingWaits(), equalTo(1L));
            assertThat(sent, hasItem(nodeRequest(node3, shard1)));
        });
        assertFalse(future.isDone());

        // the attempt is still there: shard2 gets the copy that newer routing shows
        publish(
            Map.of(
                shard1.shardId(),
                List.of(started(node1, "shard1-a"), started(node3, "shard1-b")),
                shard2.shardId(),
                List.of(started(node5, "shard2-b")),
                shard3.shardId(),
                List.of(started(node4, "shard3-a"))
            )
        );
        assertBusy(() -> assertThat(sent, hasItem(nodeRequest(node5, shard2))));
        assertThat(recorded.attemptsExhausted(), equalTo(0L));

        // and now it is spent: shard3 stalls and fails
        shard3Request.onResponse(shardNotFound(shard3));
        ComputeResponse response = safeGet(future);
        assertThat(response.totalShards, equalTo(3));
        assertThat(response.successfulShards, equalTo(2));
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).shard().getShardId(), equalTo(shard3.shardId()));
        assertThat(response.failures.get(0).reason(), containsString("no such shard"));
        assertThat("shard3 did not wait", recorded.routingWaits(), equalTo(1L));
        assertThat(recorded.attemptsExhausted(), equalTo(1L));
        assertThat(sent, hasSize(5));
    }

    /**
     * With two attempts: one pass moves a refused shard, parks a stalled one and gives a third, which answered "not available", a
     * copy it had not tried. Only the last of these is charged, and the pass counts once. The second attempt goes to the parked
     * shard when newer routing wakes it. Its new copy answers "not available" as well, and then nothing is left: it fails without
     * a second wait.
     */
    public void testMixedPassCountsOnce() throws Exception {
        RecordedMetrics recorded = new RecordedMetrics();
        HeldPass firstPass = new HeldPass(3);
        Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
        publish(
            Map.of(
                shard1.shardId(),
                List.of(started(node1, "shard1-a"), started(node3, "shard1-b")),
                shard2.shardId(),
                List.of(started(node2, "shard2-a")),
                shard3.shardId(),
                List.of(started(node4, "shard3-a"), started(node5, "shard3-b"))
            )
        );
        var future = setup(
            List.of(targetShard(shard1, node1), targetShard(shard2, node2), targetShard(shard3, node4)),
            (node, shards, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, shards));
                if (firstPass.take(node, listener) == false) {
                    runWithDelay(() -> listener.onResponse(shards.contains(shard2) ? shardNotFound(shard2) : ok()));
                }
            }
        ).allowPartialResults(true).attempts(2).metrics(recorded).start();

        firstPass.listener(node1).onFailure(refusal(node1), false);
        firstPass.listener(node2).onResponse(shardNotFound(shard2));
        firstPass.listener(node4).onResponse(shardNotFound(shard3));
        firstPass.release();
        assertBusy(() -> {
            assertThat(recorded.routingWaits(), equalTo(1L));
            assertThat(sent, hasItem(nodeRequest(node3, shard1)));
            assertThat(sent, hasItem(nodeRequest(node5, shard3)));
        });
        assertFalse(future.isDone());

        // had the pass counted twice, shard2 would fail here without being asked on its new copy
        publish(
            Map.of(
                shard1.shardId(),
                List.of(started(node1, "shard1-a"), started(node3, "shard1-b")),
                shard2.shardId(),
                List.of(started(node3, "shard2-b")),
                shard3.shardId(),
                List.of(started(node4, "shard3-a"), started(node5, "shard3-b"))
            )
        );
        // had it not counted at all, shard2 would wait a second time and the query would not complete
        ComputeResponse response = safeGet(future);
        assertThat(sent, hasItem(nodeRequest(node3, shard2)));
        assertThat(response.totalShards, equalTo(3));
        assertThat(response.successfulShards, equalTo(2));
        assertThat(response.failedShards, equalTo(1));
        assertThat(response.failures.get(0).shard().getShardId(), equalTo(shard2.shardId()));
        assertThat(recorded.routingWaits(), equalTo(1L));
        assertThat(recorded.attemptsExhausted(), equalTo(1L));
        assertThat(sent, hasSize(6));
    }

    /**
     * With a single attempt, which shard2 takes while shard1 waits. What shard1 gets after that depends on how its wait ends:
     * <ul>
     * <li>newer routing shows a new copy: going there would open another round, which has to be paid for, and nothing is left.
     * The shard fails with the failure it had, and the new copy is not asked;</li>
     * <li>the wait times out: the last try ends the shard either way, so it is free, and the copy is asked once more.</li>
     * </ul>
     */
    public void testBudgetSpentWhileAnotherShardIsParked() throws Exception {
        for (boolean timedOut : List.of(false, true)) {
            RecordedMetrics recorded = new RecordedMetrics();
            Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
            SubscribableListener<Void> failShard2 = new SubscribableListener<>();
            AtomicInteger requestsToNode1 = new AtomicInteger();
            Map<ShardId, List<Copy>> routing = Map.of(
                shard1.shardId(),
                List.of(started(node1, "shard1-a")),
                shard2.shardId(),
                List.of(started(node2, "shard2-a"), started(node3, "shard2-b"))
            );
            publish(routing);
            var future = setup(List.of(targetShard(shard1, node1), targetShard(shard2, node2)), (node, shards, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, shards));
                if (node.equals(node1)) {
                    // not available at first, and back for the last try
                    boolean first = requestsToNode1.incrementAndGet() == 1;
                    runWithDelay(() -> listener.onResponse(first ? shardNotFound(shard1) : ok()));
                } else if (node.equals(node2)) {
                    failShard2.addListener(ActionListener.running(() -> runWithDelay(() -> listener.onResponse(shardNotFound(shard2)))));
                } else {
                    runWithDelay(() -> listener.onResponse(ok()));
                }
            }).allowPartialResults(true)
                .attempts(1)
                .routingWaitTimeout(timedOut ? TimeValue.timeValueMillis(500) : DEFAULT_ROUTING_WAIT_TIMEOUT)
                .metrics(recorded)
                .start();

            assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
            failShard2.onResponse(null);
            // shard2 has taken the attempt once it is sent to its other copy
            assertBusy(() -> assertThat(sent, hasItem(nodeRequest(node3, shard2))));

            if (timedOut) {
                ComputeResponse response = safeGet(future);
                assertThat(response.successfulShards, equalTo(2));
                assertThat(response.failedShards, equalTo(0));
                assertThat("asked again by the last try", requestsToNode1.get(), equalTo(2));
                assertThat(recorded.routingWaitTimeouts(), equalTo(1L));
                assertThat(recorded.attemptsExhausted(), equalTo(0L));
            } else {
                publish(
                    Map.of(
                        shard1.shardId(),
                        List.of(started(node4, "shard1-b")),
                        shard2.shardId(),
                        List.of(started(node2, "shard2-a"), started(node3, "shard2-b"))
                    )
                );
                ComputeResponse response = safeGet(future);
                assertThat(response.successfulShards, equalTo(1));
                assertThat(response.failedShards, equalTo(1));
                assertThat(response.failures.get(0).shard().getShardId(), equalTo(shard1.shardId()));
                assertThat(response.failures.get(0).reason(), containsString("no such shard"));
                assertThat("the new copy is not asked", sent, not(hasItem(nodeRequest(node4, shard1))));
                assertThat(recorded.attemptsExhausted(), equalTo(1L));
            }
            assertThat(recorded.routingWaits(), equalTo(1L));
        }
    }

    /**
     * No attempt is left from the start. A shard that stalls after "not available" fails at once: a wait would only postpone
     * that, since the copy it waits for could not be paid for. A shard that stalls after a refusal is not held to the attempts.
     * It waits, and goes on to the copy that newer routing shows.
     */
    public void testStallWithSpentBudgetFailsWithoutWaiting() throws Exception {
        {
            RecordedMetrics recorded = new RecordedMetrics();
            publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
            var future = setup(
                List.of(targetShard(shard1, node1)),
                (node, shards, aliasFilters, listener) -> runWithDelay(() -> listener.onResponse(shardNotFound(shard1)))
            ).attempts(0).metrics(recorded).start();
            expectThrows(
                NoShardAvailableActionException.class,
                containsString("no such shard"),
                () -> future.actionGet(10, TimeUnit.SECONDS)
            );
            assertThat(recorded.routingWaits(), equalTo(0L));
            assertThat(recorded.attemptsExhausted(), equalTo(1L));
        }
        {
            RecordedMetrics recorded = new RecordedMetrics();
            Queue<NodeRequest> sent = ConcurrentCollections.newQueue();
            publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
            var future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
                sent.add(nodeRequest(node, shards));
                if (node.equals(node1)) {
                    runWithDelay(() -> listener.onFailure(refusal(node1), false));
                } else {
                    runWithDelay(() -> listener.onResponse(ok()));
                }
            }).attempts(0).metrics(recorded).start();
            assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
            assertFalse(future.isDone());
            publish(Map.of(shard1.shardId(), List.of(started(node2, "allocation-2"))));
            ComputeResponse response = safeGet(future);
            assertThat(response.successfulShards, equalTo(1));
            assertThat(sent, contains(nodeRequest(node1, shard1), nodeRequest(node2, shard1)));
            assertThat(recorded.attemptsExhausted(), equalTo(0L));
        }
    }

    /**
     * A timeout of zero switches the wait off and keeps the routing retry of {@code main}: under unchanged routing every pass
     * resolves again and asks the active copy again, whether it failed before or not, until the attempts are spent.
     */
    public void testZeroTimeoutRetriesAsOnMain() {
        {
            // a copy that fails twice and then answers is reached
            RecordedMetrics recorded = new RecordedMetrics();
            AtomicInteger requests = new AtomicInteger();
            publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
            ComputeResponse response = safeGet(setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
                boolean available = requests.incrementAndGet() == 3;
                runWithDelay(() -> listener.onResponse(available ? ok() : shardNotFound(shard1)));
            }).allowPartialResults(randomBoolean()).routingWaitTimeout(TimeValue.ZERO).metrics(recorded).start());
            assertThat(response.successfulShards, equalTo(1));
            assertThat(response.failedShards, equalTo(0));
            assertThat(requests.get(), equalTo(3));
            assertThat(recorded.routingWaits(), equalTo(0L));
            assertThat(recorded.attemptsExhausted(), equalTo(0L));
        }
        {
            // a copy that always fails is asked until the ten attempts are spent
            RecordedMetrics recorded = new RecordedMetrics();
            AtomicInteger requests = new AtomicInteger();
            AtomicInteger resolutions = new AtomicInteger();
            publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
            var future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
                requests.incrementAndGet();
                runWithDelay(() -> listener.onResponse(shardNotFound(shard1)));
            }).routingWaitTimeout(TimeValue.ZERO).metrics(recorded).onResolve(resolved -> {
                resolutions.incrementAndGet();
                return resolved;
            }).start();
            expectThrows(
                NoShardAvailableActionException.class,
                containsString("no such shard"),
                () -> future.actionGet(10, TimeUnit.SECONDS)
            );
            assertThat(resolutions.get(), equalTo(10));
            assertThat(requests.get(), equalTo(11));
            assertThat(recorded.routingWaits(), equalTo(0L));
            assertThat(recorded.attemptsExhausted(), equalTo(1L));
        }
    }

    /**
     * A timeout of zero also takes the relocation targets out of the retry, as on {@code main}: only active copies are asked.
     * Under the same routing the default timeout goes on to the target.
     */
    public void testZeroTimeoutDoesNotTryRelocationTarget() {
        for (boolean waitEnabled : List.of(false, true)) {
            RecordedMetrics recorded = new RecordedMetrics();
            Queue<DiscoveryNode> asked = ConcurrentCollections.newQueue();
            publish(Map.of(shard1.shardId(), List.of(relocating(node1, "allocation-1", node2))));
            var future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
                asked.add(node);
                runWithDelay(() -> listener.onResponse(node.equals(node1) ? shardNotFound(shard1) : ok()));
            }).routingWaitTimeout(waitEnabled ? DEFAULT_ROUTING_WAIT_TIMEOUT : TimeValue.ZERO).metrics(recorded).start();
            if (waitEnabled) {
                assertThat(safeGet(future).successfulShards, equalTo(1));
                assertThat(asked, contains(node1, node2));
            } else {
                expectThrows(
                    NoShardAvailableActionException.class,
                    containsString("no such shard"),
                    () -> future.actionGet(10, TimeUnit.SECONDS)
                );
                assertThat("the relocation target is never asked", asked, not(hasItem(node2)));
                assertThat(asked, hasSize(11));
            }
            assertThat(recorded.routingWaits(), equalTo(0L));
        }
    }

    /**
     * A timeout of zero restores the routing retry, not the old reading of a failure: a node that refused is still left out. With
     * no other copy the shard fails at once, and the refuser is not asked again although the routing still lists it.
     */
    public void testZeroTimeoutStillExcludesRefuser() {
        RecordedMetrics recorded = new RecordedMetrics();
        AtomicInteger requests = new AtomicInteger();
        AtomicInteger resolutions = new AtomicInteger();
        publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
        var future = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
            requests.incrementAndGet();
            runWithDelay(() -> listener.onFailure(refusal(node1), false));
        }).routingWaitTimeout(TimeValue.ZERO).metrics(recorded).onResolve(resolved -> {
            resolutions.incrementAndGet();
            return resolved;
        }).start();
        expectThrows(NodeClosedException.class, () -> future.actionGet(10, TimeUnit.SECONDS));
        assertThat(requests.get(), equalTo(1));
        assertThat(resolutions.get(), equalTo(1));
        assertThat(recorded.routingWaits(), equalTo(0L));
        assertThat(recorded.attemptsExhausted(), equalTo(0L));
    }

    /**
     * The search pool rejects the hand-off of an ended wait, as it does once it is shutting down. The loop will not run again for
     * the parked shard, so the wait fails the compute listener itself: the listener of the sender fails with the failure the shard
     * had before it waited, in both partial modes, and never completes with a response. The failed slot also runs the hook that
     * cancels the query.
     * <p>
     * This is the boundary of the sender. What the client gets is traced in the Javadoc of {@code RoutingWait#end}, see
     * {@link #testRejectedHandOffAtTheQueryBoundary}.
     */
    public void testRejectedHandOffFailsTheSenderListener() throws Exception {
        for (boolean allowPartialResults : List.of(false, true)) {
            RecordedMetrics recorded = new RecordedMetrics();
            GatedExecutor handOff = new GatedExecutor(executor);
            AtomicInteger requests = new AtomicInteger();
            AtomicReference<CancellableTask> task = new AtomicReference<>();
            publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
            SenderSetup setup = setup(List.of(targetShard(shard1, node1)), (node, shards, aliasFilters, listener) -> {
                requests.incrementAndGet();
                runWithDelay(() -> listener.onResponse(shardNotFound(shard1)));
            }).allowPartialResults(allowPartialResults)
                .metrics(recorded)
                .handOffExecutor(handOff)
                // what ComputeService#cancelQueryOnFailure does to the task of the query
                .runOnTaskFailure(() -> TaskCancelHelper.cancel(task.get(), "cancelled on failure"));
            task.set(setup.task());
            PlainActionFuture<ComputeResponse> future = setup.start();

            assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
            assertFalse(future.isDone());
            handOff.reject();
            publish(Map.of(shard1.shardId(), List.of(started(node2, "allocation-2"))));

            expectThrows(
                NoShardAvailableActionException.class,
                containsString("no such shard"),
                () -> future.actionGet(10, TimeUnit.SECONDS)
            );
            assertThat(handOff.submitted(), equalTo(1));
            assertTrue("the failed slot cancels the query", setup.task().isCancelled());
            assertThat("the new copy is not asked: the loop did not run", requests.get(), equalTo(1));
        }
    }

    /**
     * What the client gets after a rejected hand-off, in both partial modes: a failure, never a success and never a partial result.
     * <p>
     * This is not driven through a {@code ComputeService}, which is what the plan of this change asked for. A real one needs a
     * real search pool, and the only way to make that reject a forced task is to shut it down. With assertions enabled the
     * transport layer then trips on the rejected completion of the coordinator's driver ({@code ForkingResponseHandlerRunnable}),
     * and the request never completes. So the sender runs here with the rejecting executor of this class, and the test applies
     * above it the steps that {@code ComputeService#executePlan} and {@code TransportEsqlQueryAction} apply to the failure of
     * the data node compute, with their own code where that is reachable:
     * <ol>
     * <li>the failed slot cancels the task of the query, as {@code ComputeService#cancelQueryOnFailure} does;</li>
     * <li>with partial results, a failure that {@link EsqlCCSUtils#canAllowPartial} accepts is recorded and the compute goes
     * on; otherwise the failure is the outcome;</li>
     * <li>a compute that went on ends in {@link TransportEsqlQueryAction#failIfCancelled}.</li>
     * </ol>
     * The steps are copied from the callers, so this pins what was traced. It does not notice a change of the wiring itself.
     */
    public void testRejectedHandOffAtTheQueryBoundary() throws Exception {
        for (boolean allowPartialResults : List.of(false, true)) {
            RecordedMetrics recorded = new RecordedMetrics();
            GatedExecutor handOff = new GatedExecutor(executor);
            AtomicReference<CancellableTask> task = new AtomicReference<>();
            publish(Map.of(shard1.shardId(), List.of(started(node1, "allocation-1"))));
            SenderSetup setup = setup(
                List.of(targetShard(shard1, node1)),
                (node, shards, aliasFilters, listener) -> runWithDelay(() -> listener.onResponse(shardNotFound(shard1)))
            ).allowPartialResults(allowPartialResults)
                .metrics(recorded)
                .handOffExecutor(handOff)
                .runOnTaskFailure(() -> TaskCancelHelper.cancel(task.get(), "cancelled on failure"));
            task.set(setup.task());
            PlainActionFuture<ComputeResponse> dataNodeCompute = setup.start();

            assertBusy(() -> assertThat(recorded.routingWaits(), equalTo(1L)));
            handOff.reject();
            publish(Map.of(shard1.shardId(), List.of(started(node2, "allocation-2"))));

            Exception shardFailure = expectThrows(Exception.class, () -> dataNodeCompute.actionGet(10, TimeUnit.SECONDS));
            assertThat(shardFailure, instanceOf(NoShardAvailableActionException.class));
            assertTrue(setup.task().isCancelled());

            if (allowPartialResults) {
                assertTrue("the failure is one a partial result may record", EsqlCCSUtils.canAllowPartial(shardFailure));
                // the compute goes on and completes, and its result is turned down because the task was cancelled
                Result result = new Result(List.of(), List.of(), Map.of(), null, DriverCompletionInfo.EMPTY, null, null);
                TaskCancelledException cancelled = expectThrows(
                    TaskCancelledException.class,
                    () -> TransportEsqlQueryAction.failIfCancelled(setup.task(), result)
                );
                assertThat(cancelled.getMessage(), containsString("cancelled on failure"));
            } else {
                // the failure of the data node compute is the failure of the request
                assertThat(shardFailure.getMessage(), containsString("no such shard"));
            }
        }
    }

    private static DataNodeComputeResponse ok() {
        return new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, Map.of());
    }

    /** The response of a node that no longer has the shards, as a shard-level failure per shard. */
    private static DataNodeComputeResponse shardNotFound(DataNodeRequest.Shard... shards) {
        Map<ShardId, Exception> failures = new HashMap<>();
        for (DataNodeRequest.Shard shard : shards) {
            failures.put(shard.shardId(), new ShardNotFoundException(shard.shardId()));
        }
        return new DataNodeComputeResponse(DriverCompletionInfo.EMPTY, failures);
    }

    /**
     * One copy of a shard as a published routing table lists it. The allocation id is explicit, since it is what the sender tells
     * copies apart by.
     */
    private record Copy(DiscoveryNode node, AllocationId allocationId, ShardRoutingState state, @Nullable DiscoveryNode relocatingTo) {

        /** The id the copy on the relocation target has, and keeps once it has started there. */
        String targetAllocationId() {
            assert relocatingTo != null;
            return allocationId.getRelocationId();
        }

        ShardRouting routing(ShardId shardId, boolean primary) {
            var builder = TestShardRouting.shardRoutingBuilder(shardId, node.getId(), primary, state).withAllocationId(allocationId);
            if (relocatingTo != null) {
                builder.withRelocatingNodeId(relocatingTo.getId());
            }
            return builder.build();
        }
    }

    private static Copy started(DiscoveryNode node, String allocationId) {
        return new Copy(node, AllocationId.newInitializing(allocationId), ShardRoutingState.STARTED, null);
    }

    private static Copy initializing(DiscoveryNode node, String allocationId) {
        return new Copy(node, AllocationId.newInitializing(allocationId), ShardRoutingState.INITIALIZING, null);
    }

    private static Copy relocating(DiscoveryNode node, String allocationId, DiscoveryNode target) {
        AllocationId relocation = AllocationId.newRelocation(AllocationId.newInitializing(allocationId));
        return new Copy(node, relocation, ShardRoutingState.RELOCATING, target);
    }

    /**
     * A cluster state, newer than every one built before, that routes the given shards of one index in the project. The first copy
     * of a shard is its primary.
     * <p>
     * An index routing table needs every shard from 0 to its highest one, and the shards of these tests start at 1: the shards that
     * are not given are filled in, started on {@code node5}.
     *
     * @param projectId the project the routing belongs to, or {@code null} for a state that has neither the project nor its routing
     * @param routing   the copies per shard; empty for a project without the index
     */
    private ClusterState routingState(@Nullable ProjectId projectId, Map<ShardId, List<Copy>> routing) {
        Metadata.Builder metadata = Metadata.builder();
        GlobalRoutingTable.Builder globalRouting = GlobalRoutingTable.builder();
        if (projectId != null) {
            RoutingTable.Builder projectRouting = RoutingTable.builder();
            if (routing.isEmpty() == false) {
                Index index = routing.keySet().iterator().next().getIndex();
                int highestShard = routing.keySet().stream().mapToInt(ShardId::id).max().orElseThrow();
                IndexRoutingTable.Builder indexTable = IndexRoutingTable.builder(index);
                for (int id = 0; id <= highestShard; id++) {
                    ShardId shardId = new ShardId(index, id);
                    List<Copy> copies = routing.getOrDefault(shardId, List.of(started(node5, "filler-" + id)));
                    var shardTable = new IndexShardRoutingTable.Builder(shardId);
                    for (int i = 0; i < copies.size(); i++) {
                        shardTable.addShard(copies.get(i).routing(shardId, i == 0));
                    }
                    indexTable.addIndexShard(shardTable);
                }
                projectRouting.add(indexTable.build());
            }
            metadata.put(ProjectMetadata.builder(projectId));
            globalRouting.put(projectId, projectRouting.build());
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .version(stateVersion.incrementAndGet())
            .nodes(DiscoveryNodes.builder().add(node1).add(node2).add(node3).add(node4).add(node5))
            .metadata(metadata)
            .routingTable(globalRouting.build())
            .build();
    }

    /** Applies a new state with this routing for the default project, and returns once every listener has seen it. */
    private void publish(Map<ShardId, List<Copy>> routing) {
        publish(ProjectId.DEFAULT, routing);
    }

    private void publish(@Nullable ProjectId projectId, Map<ShardId, List<Copy>> routing) {
        ClusterServiceUtils.setState(clusterService, routingState(projectId, routing));
    }

    /**
     * The three counters of {@link ShardResolutionMetrics}, read back from the registry that recorded them. The class under test
     * is the real one; only the registry is the recording one of the test framework.
     */
    private static final class RecordedMetrics {
        private final RecordingMeterRegistry registry = new RecordingMeterRegistry();
        private final ShardResolutionMetrics metrics = new ShardResolutionMetrics(registry);

        long routingWaits() {
            return count(ShardResolutionMetrics.ROUTING_WAITS);
        }

        long routingWaitTimeouts() {
            return count(ShardResolutionMetrics.ROUTING_WAIT_TIMEOUTS);
        }

        long attemptsExhausted() {
            return count(ShardResolutionMetrics.ATTEMPTS_EXHAUSTED);
        }

        private long count(String name) {
            return registry.getRecorder().getMeasurements(InstrumentType.LONG_COUNTER, name).stream().mapToLong(Measurement::getLong).sum();
        }
    }

    /**
     * Stands in for the search pool at the one place the sender submits to it: the hand-off of an ended wait back to the sending
     * loop. Tasks run on the real search pool, so the loop's thread assertion holds. The test can hold them back, to let an outcome
     * wait in the queue while the query moves on, or reject them the way a pool that is shutting down does.
     */
    private static final class GatedExecutor implements Executor {
        private enum Mode {
            RUN,
            HOLD,
            REJECT
        }

        private final Executor delegate;
        private final List<Runnable> held = new ArrayList<>();
        private final AtomicInteger submitted = new AtomicInteger();
        private Mode mode = Mode.RUN;

        GatedExecutor(Executor delegate) {
            this.delegate = delegate;
        }

        @Override
        public void execute(Runnable command) {
            submitted.incrementAndGet();
            final Mode current;
            synchronized (this) {
                current = mode;
                if (current == Mode.HOLD) {
                    held.add(command);
                }
            }
            switch (current) {
                case RUN -> delegate.execute(command);
                case HOLD -> {
                }
                case REJECT -> {
                    // what EsThreadPoolExecutor does with a task its queue turned away
                    var rejection = new EsRejectedExecutionException("executor is shutting down", true);
                    if (command instanceof AbstractRunnable abstractRunnable) {
                        try {
                            abstractRunnable.onRejection(rejection);
                        } finally {
                            abstractRunnable.onAfter();
                        }
                    } else {
                        throw rejection;
                    }
                }
            }
        }

        synchronized void hold() {
            mode = Mode.HOLD;
        }

        synchronized void reject() {
            mode = Mode.REJECT;
        }

        /** Runs what was held, and everything submitted from now on. */
        void release() {
            final List<Runnable> toRun;
            synchronized (this) {
                mode = Mode.RUN;
                toRun = List.copyOf(held);
                held.clear();
            }
            toRun.forEach(delegate::execute);
        }

        /** The tasks submitted so far, whatever became of them. */
        int submitted() {
            return submitted.get();
        }
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
        return setup(shards, sender).allowPartialResults(allowPartialResults)
            .concurrentRequests(concurrentRequests)
            .resolver(resolver)
            .start();
    }

    private SenderSetup setup(List<DataNodeRequestSender.TargetShard> shards, Sender sender) {
        return new SenderSetup(shards, sender);
    }

    /**
     * What one sender under test is built from. Everything but the shards and the node behaviour has a default: partial results
     * off, no limit on concurrent requests, ten resolution attempts, the routing wait at its production default of 30 seconds, a
     * query that never finishes, no counters, the default project and the real search pool for the hand-off of an ended wait.
     * <p>
     * The sender runs in one of two modes:
     * <ul>
     * <li>Legacy mode, with a {@link #resolver}: the re-resolution comes from the lambda, as node lists, and the allocation id of
     * a copy is its node id. The predicate of a routing wait does not go through the lambda. It reads the published state, which
     * has no routing for the shards, so any newer state ends the wait as "gone". A stall in this mode therefore ends by its
     * timeout only as long as the test publishes no state, and a test that stalls sets a short timeout or zero.</li>
     * <li>Routing mode, without one: the sender resolves from the states the test publishes, see {@link #routingState}. The first
     * resolution stays with the shards given here and must list the nodes the published state does.</li>
     * </ul>
     */
    private final class SenderSetup {
        private final List<DataNodeRequestSender.TargetShard> shards;
        private final Sender sender;
        private final CancellableTask task = new CancellableTask(
            randomNonNegativeLong(),
            "type",
            "action",
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
        private SliceSelection slices = SliceSelection.UNSPECIFIED;
        private boolean allowPartialResults = false;
        private int concurrentRequests = -1;
        private int attempts = 10;
        private TimeValue routingWaitTimeout = DEFAULT_ROUTING_WAIT_TIMEOUT;
        private Resolver resolver = null;
        private UnaryOperator<DataNodeRequestSender.ResolvedShards> onResolve = UnaryOperator.identity();
        private SubscribableListener<Void> queryFinished = new SubscribableListener<>();
        private Runnable onFirstWaitStarting = () -> {};
        private ShardResolutionMetrics metrics = ShardResolutionMetrics.NOOP;
        private ProjectResolver projectResolver = TestProjectResolvers.DEFAULT_PROJECT_ONLY;
        private Executor handOffExecutor = executor;
        private Consumer<DriverCompletionInfo> onFailureUsage = usage -> {};
        private Runnable runOnTaskFailure = () -> {};
        private final AtomicInteger queryFinishedListeners = new AtomicInteger();

        private SenderSetup(List<DataNodeRequestSender.TargetShard> shards, Sender sender) {
            this.shards = shards;
            this.sender = sender;
        }

        SenderSetup slices(SliceSelection slices) {
            this.slices = slices;
            return this;
        }

        SenderSetup allowPartialResults(boolean allowPartialResults) {
            this.allowPartialResults = allowPartialResults;
            return this;
        }

        SenderSetup concurrentRequests(int concurrentRequests) {
            this.concurrentRequests = concurrentRequests;
            return this;
        }

        /** The pragma {@code unavailable_shard_resolution_attempts}. */
        SenderSetup attempts(int attempts) {
            this.attempts = attempts;
            return this;
        }

        SenderSetup routingWaitTimeout(TimeValue routingWaitTimeout) {
            this.routingWaitTimeout = routingWaitTimeout;
            return this;
        }

        /** Switches to legacy mode. */
        SenderSetup resolver(Resolver resolver) {
            this.resolver = resolver;
            return this;
        }

        /** Routing mode only: sees, and may replace, what each resolution read from the applied state. */
        SenderSetup onResolve(UnaryOperator<DataNodeRequestSender.ResolvedShards> onResolve) {
            this.onResolve = onResolve;
            return this;
        }

        /** Backs both {@code isQueryFinished} and the listener the sender registers. */
        SenderSetup queryFinished(SubscribableListener<Void> queryFinished) {
            this.queryFinished = queryFinished;
            return this;
        }

        /**
         * Runs on the thread that starts the first wait of the sender, when it registers the query-finished listener: after the wait
         * is listed as active and before it reads cancellation, the finished query and a reported failure.
         */
        SenderSetup onFirstWaitStarting(Runnable onFirstWaitStarting) {
            this.onFirstWaitStarting = onFirstWaitStarting;
            return this;
        }

        SenderSetup metrics(RecordedMetrics recorded) {
            this.metrics = recorded.metrics;
            return this;
        }

        /** The project is read from it once, on the thread that calls {@link #start}. */
        SenderSetup projectResolver(ProjectResolver projectResolver) {
            this.projectResolver = projectResolver;
            return this;
        }

        /** Where an ended wait is handed back to the sending loop. */
        SenderSetup handOffExecutor(Executor handOffExecutor) {
            this.handOffExecutor = handOffExecutor;
            return this;
        }

        SenderSetup onFailureUsage(Consumer<DriverCompletionInfo> onFailureUsage) {
            this.onFailureUsage = onFailureUsage;
            return this;
        }

        /** Runs when a slot of the sender's compute listener fails; in production it cancels the query. */
        SenderSetup runOnTaskFailure(Runnable runOnTaskFailure) {
            this.runOnTaskFailure = runOnTaskFailure;
            return this;
        }

        CancellableTask task() {
            return task;
        }

        /** How often the sender registered a query-finished listener. It registers its two hooks together, once. */
        int queryFinishedListeners() {
            return queryFinishedListeners.get();
        }

        PlainActionFuture<ComputeResponse> start() {
            PlainActionFuture<ComputeResponse> future = new PlainActionFuture<>();
            build().startComputeOnDataNodes(Set.of(randomAlphaOfLength(10)), runOnTaskFailure, onFailureUsage, future);
            return future;
        }

        DataNodeRequestSender build() {
            TransportService transportService = MockTransportService.createNewService(
                Settings.EMPTY,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            );
            return new DataNodeRequestSender(
                clusterService,
                projectResolver,
                transportService,
                handOffExecutor,
                task,
                new OriginalIndices(new String[0], SearchRequest.DEFAULT_INDICES_OPTIONS),
                null,
                slices,
                "",
                allowPartialResults,
                concurrentRequests,
                attempts,
                routingWaitTimeout,
                metrics
            ) {
                @Override
                boolean isQueryFinished() {
                    return queryFinished.isDone();
                }

                @Override
                void addQueryFinishedListener(ActionListener<Void> listener) {
                    queryFinishedListeners.incrementAndGet();
                    onFirstWaitStarting.run();
                    queryFinished.addListener(listener);
                }

                @Override
                void searchShards(Set<String> concreteIndices, ActionListener<TargetShards> listener) {
                    runWithDelay(
                        () -> listener.onResponse(
                            new TargetShards(shards.stream().collect(toMap(TargetShard::shardId, Function.identity())), shards.size(), 0)
                        )
                    );
                }

                @Override
                ResolvedShards resolveShards(Set<ShardId> shardIds) {
                    if (resolver == null) {
                        return onResolve.apply(super.resolveShards(shardIds));
                    }
                    Map<ShardId, List<ShardCopy>> copies = new HashMap<>();
                    resolver.resolve(shardIds)
                        .forEach(
                            (shardId, nodes) -> copies.put(
                                shardId,
                                nodes.stream().map(node -> new ShardCopy(node, node.getId(), CopyState.ACTIVE)).toList()
                            )
                        );
                    return new ResolvedShards(clusterService.state(), copies);
                }

                @Override
                SentCopy sentCopy(ClusterState applied, ShardId shardId, DiscoveryNode node) {
                    return resolver == null ? super.sentCopy(applied, shardId, node) : new SentCopy(node.getId(), true);
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
    }

    /**
     * A selection of named slices routes the query to the shards that hold them. Without a slice-enabled index there is
     * nothing to route by: the filter the selection is derived from matches no document.
     */
    public void testSliceSelectionRoutesShardResolution() {
        Sender unused = (node, shardIds, aliasFilters, listener) -> { throw new AssertionError("no request is expected here"); };
        Resolver noResolution = shardIds -> { throw new AssertionError("no shard resolution is expected here"); };

        for (boolean anySliceEnabled : List.of(true, false)) {
            var request = setup(List.of(), unused).resolver(noResolution).build().searchShardsRequest(anySliceEnabled);
            assertNull(request.routing());
            assertFalse(request.isRoutingFromSlice());
        }

        var request = setup(List.of(), unused).slices(SliceSelection.of(List.of("s1", "s2")))
            .resolver(noResolution)
            .build()
            .searchShardsRequest(true);
        assertThat(request.routing(), equalTo("s1,s2"));
        assertTrue(request.isRoutingFromSlice());
        assertThat(request.searchSlice(), equalTo("s1,s2"));

        request = setup(List.of(), unused).slices(SliceSelection.of(List.of("s1", "s2")))
            .resolver(noResolution)
            .build()
            .searchShardsRequest(false);
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
