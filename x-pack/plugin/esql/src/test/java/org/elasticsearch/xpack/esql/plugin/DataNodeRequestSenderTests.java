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
import org.elasticsearch.tasks.CancellableTask;
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
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.ShardCopy;
import org.elasticsearch.xpack.esql.plugin.DataNodeRequestSender.Untried;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
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
        assertThat(
            DataNodeRequestSender.classifyNodeFailure(new RemoteTransportException("remote", new NodeClosedException(node1))),
            equalTo(NodeFailure.REFUSED)
        );
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
            DataNodeRequestSender.classifyNodeFailure(
                new SendRequestTransportException(node1, "action", new NodeClosedException(DiscoveryNodeUtils.create("coordinator")))
            ),
            equalTo(NodeFailure.OTHER)
        );
        assertThat(
            DataNodeRequestSender.classifyDataRequestFailure(new RemoteTransportException("remote", new NodeClosedException(node1))),
            equalTo(NodeFailure.OTHER)
        );
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

        assertThat(DataNodeRequestSender.untriedCopies(List.of(), Map.of(), Set.of()).kind(), equalTo(Untried.GONE));
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

        CancellableTask task() {
            return task;
        }

        /** How often the sender registered a query-finished listener. It registers its two hooks together, once. */
        int queryFinishedListeners() {
            return queryFinishedListeners.get();
        }

        PlainActionFuture<ComputeResponse> start() {
            PlainActionFuture<ComputeResponse> future = new PlainActionFuture<>();
            build().startComputeOnDataNodes(Set.of(randomAlphaOfLength(10)), () -> {}, onFailureUsage, future);
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
