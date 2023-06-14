/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class SearchAsyncActionTests extends ESTestCase {

    public void testSkipSearchShards() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        request.allowPartialSearchResults(true);
        int numShards = 10;

        AtomicReference<SearchResponse> searchResponse = new AtomicReference<>();
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(searchResponse::set, (e) -> {
            throw new AssertionError("unexpected", e);
        });
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node_2");

        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards,
            randomBoolean(),
            primaryNode,
            replicaNode
        );
        int numSkipped = 0;
        for (SearchShardIterator iter : shardsIter) {
            if (iter.shardId().id() % 2 == 0) {
                iter.skip(true);
                numSkipped++;
            }
        }
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean searchPhaseDidRun = new AtomicBoolean(false);

        SearchTransportService transportService = new SearchTransportService(null, null, null);
        Map<String, Transport.Connection> lookup = new HashMap<>();
        Map<ShardId, Boolean> seenShard = new ConcurrentHashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", AliasFilter.EMPTY);
        AtomicInteger numRequests = new AtomicInteger(0);
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction = new AbstractSearchAsyncAction<TestSearchPhaseResult>(
            "test",
            logger,
            transportService,
            (cluster, node) -> {
                assert cluster == null : "cluster was not null: " + cluster;
                return lookup.get(node);
            },
            aliasFilters,
            Collections.emptyMap(),
            null,
            request,
            responseListener,
            shardsIter,
            new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
            ClusterState.EMPTY_STATE,
            null,
            new ArraySearchPhaseResults<>(shardsIter.size()),
            request.getMaxConcurrentShardRequests(),
            SearchResponse.Clusters.EMPTY
        ) {

            @Override
            protected void executePhaseOnShard(
                SearchShardIterator shardIt,
                SearchShardTarget shard,
                SearchActionListener<TestSearchPhaseResult> listener
            ) {
                seenShard.computeIfAbsent(shard.getShardId(), (i) -> {
                    numRequests.incrementAndGet(); // only count this once per replica
                    return Boolean.TRUE;
                });

                new Thread(() -> {
                    Transport.Connection connection = getConnection(null, shard.getNodeId());
                    TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(
                        new ShardSearchContextId(UUIDs.randomBase64UUID(), contextIdGenerator.incrementAndGet()),
                        connection.getNode()
                    );
                    listener.onResponse(testSearchPhaseResult);

                }).start();
            }

            @Override
            protected SearchPhase getNextPhase(SearchPhaseResults<TestSearchPhaseResult> results, SearchPhaseContext context) {
                return new SearchPhase("test") {
                    @Override
                    public void run() {
                        assertTrue(searchPhaseDidRun.compareAndSet(false, true));
                        latch.countDown();
                    }
                };
            }
        };
        asyncAction.start();
        latch.await();
        assertTrue(searchPhaseDidRun.get());
        assertEquals(shardsIter.size() - numSkipped, numRequests.get());

        asyncAction.sendSearchResponse(null, null);
        assertNotNull(searchResponse.get());
        assertEquals(0, searchResponse.get().getFailedShards());
        assertEquals(numSkipped, searchResponse.get().getSkippedShards());
        assertEquals(shardsIter.size(), searchResponse.get().getSuccessfulShards());
    }

    public void testLimitConcurrentShardRequests() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        request.allowPartialSearchResults(true);
        int numConcurrent = randomIntBetween(1, 5);
        request.setMaxConcurrentShardRequests(numConcurrent);
        boolean doReplicas = randomBoolean();
        int numShards = randomIntBetween(5, 10);
        Boolean[] shardFailures = new Boolean[numShards];
        // at least one response otherwise the entire request fails
        shardFailures[randomIntBetween(0, shardFailures.length - 1)] = false;
        for (int i = 0; i < shardFailures.length; i++) {
            if (shardFailures[i] == null) {
                boolean failure = randomBoolean();
                shardFailures[i] = failure;
            }
        }
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean searchPhaseDidRun = new AtomicBoolean(false);
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(
            response -> {},
            (e) -> { throw new AssertionError("unexpected", e); }
        );
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");
        // for the sake of this test we place the replica on the same node. ie. this is not a mistake since we limit per node now
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node_1");

        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards,
            doReplicas,
            primaryNode,
            replicaNode
        );
        SearchTransportService transportService = new SearchTransportService(null, null, null);
        Map<String, Transport.Connection> lookup = new HashMap<>();
        Map<ShardId, Boolean> seenShard = new ConcurrentHashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", AliasFilter.EMPTY);
        CountDownLatch awaitInitialRequests = new CountDownLatch(1);
        AtomicInteger numRequests = new AtomicInteger(0);
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction = new AbstractSearchAsyncAction<TestSearchPhaseResult>(
            "test",
            logger,
            transportService,
            (cluster, node) -> {
                assert cluster == null : "cluster was not null: " + cluster;
                return lookup.get(node);
            },
            aliasFilters,
            Collections.emptyMap(),
            null,
            request,
            responseListener,
            shardsIter,
            new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
            ClusterState.EMPTY_STATE,
            null,
            new ArraySearchPhaseResults<>(shardsIter.size()),
            request.getMaxConcurrentShardRequests(),
            SearchResponse.Clusters.EMPTY
        ) {

            @Override
            protected void executePhaseOnShard(
                SearchShardIterator shardIt,
                SearchShardTarget shard,
                SearchActionListener<TestSearchPhaseResult> listener
            ) {
                seenShard.computeIfAbsent(shard.getShardId(), (i) -> {
                    numRequests.incrementAndGet(); // only count this once per shard copy
                    return Boolean.TRUE;
                });

                new Thread(() -> {
                    try {
                        awaitInitialRequests.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    Transport.Connection connection = getConnection(null, shard.getNodeId());
                    TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(
                        new ShardSearchContextId(UUIDs.randomBase64UUID(), contextIdGenerator.incrementAndGet()),
                        connection.getNode()
                    );
                    if (shardFailures[shard.getShardId().id()]) {
                        listener.onFailure(new RuntimeException());
                    } else {
                        listener.onResponse(testSearchPhaseResult);
                    }
                }).start();
            }

            @Override
            protected SearchPhase getNextPhase(SearchPhaseResults<TestSearchPhaseResult> results, SearchPhaseContext context) {
                return new SearchPhase("test") {
                    @Override
                    public void run() {
                        assertTrue(searchPhaseDidRun.compareAndSet(false, true));
                        latch.countDown();
                    }
                };
            }
        };
        asyncAction.start();
        assertEquals(numConcurrent, numRequests.get());
        awaitInitialRequests.countDown();
        latch.await();
        assertTrue(searchPhaseDidRun.get());
        assertEquals(numShards, numRequests.get());
    }

    public void testFanOutAndCollect() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        request.allowPartialSearchResults(true);
        request.setMaxConcurrentShardRequests(randomIntBetween(1, 100));
        AtomicReference<TestSearchResponse> response = new AtomicReference<>();
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(
            searchResponse -> response.set((TestSearchResponse) searchResponse),
            (e) -> {
                throw new AssertionError("unexpected", e);
            }
        );
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node_2");

        Map<DiscoveryNode, Set<ShardSearchContextId>> nodeToContextMap = newConcurrentMap();
        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        int numShards = randomIntBetween(1, 10);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards,
            randomBoolean(),
            primaryNode,
            replicaNode
        );
        AtomicInteger numFreedContext = new AtomicInteger();
        SearchTransportService transportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendFreeContext(Transport.Connection connection, ShardSearchContextId contextId, OriginalIndices originalIndices) {
                numFreedContext.incrementAndGet();
                assertTrue(nodeToContextMap.containsKey(connection.getNode()));
                assertTrue(nodeToContextMap.get(connection.getNode()).remove(contextId));
            }
        };
        Map<String, Transport.Connection> lookup = new HashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", AliasFilter.EMPTY);
        ExecutorService executor = Executors.newFixedThreadPool(randomIntBetween(1, Runtime.getRuntime().availableProcessors()));
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean latchTriggered = new AtomicBoolean();
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction = new AbstractSearchAsyncAction<TestSearchPhaseResult>(
            "test",
            logger,
            transportService,
            (cluster, node) -> {
                assert cluster == null : "cluster was not null: " + cluster;
                return lookup.get(node);
            },
            aliasFilters,
            Collections.emptyMap(),
            executor,
            request,
            responseListener,
            shardsIter,
            new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
            ClusterState.EMPTY_STATE,
            null,
            new ArraySearchPhaseResults<>(shardsIter.size()),
            request.getMaxConcurrentShardRequests(),
            SearchResponse.Clusters.EMPTY
        ) {
            TestSearchResponse response = new TestSearchResponse();

            @Override
            protected void executePhaseOnShard(
                SearchShardIterator shardIt,
                SearchShardTarget shard,
                SearchActionListener<TestSearchPhaseResult> listener
            ) {
                assertTrue("shard: " + shard.getShardId() + " has been queried twice", response.queried.add(shard.getShardId()));
                Transport.Connection connection = getConnection(null, shard.getNodeId());
                TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(
                    new ShardSearchContextId(UUIDs.randomBase64UUID(), contextIdGenerator.incrementAndGet()),
                    connection.getNode()
                );
                Set<ShardSearchContextId> ids = nodeToContextMap.computeIfAbsent(connection.getNode(), (n) -> newConcurrentSet());
                ids.add(testSearchPhaseResult.getContextId());
                if (randomBoolean()) {
                    listener.onResponse(testSearchPhaseResult);
                } else {
                    new Thread(() -> listener.onResponse(testSearchPhaseResult)).start();
                }
            }

            @Override
            protected SearchPhase getNextPhase(SearchPhaseResults<TestSearchPhaseResult> results, SearchPhaseContext context) {
                return new SearchPhase("test") {
                    @Override
                    public void run() {
                        for (int i = 0; i < results.getNumShards(); i++) {
                            TestSearchPhaseResult result = results.getAtomicArray().get(i);
                            assertEquals(result.node.getId(), result.getSearchShardTarget().getNodeId());
                            sendReleaseSearchContext(result.getContextId(), new MockConnection(result.node), OriginalIndices.NONE);
                        }
                        responseListener.onResponse(response);
                        if (latchTriggered.compareAndSet(false, true) == false) {
                            throw new AssertionError("latch triggered twice");
                        }
                        latch.countDown();
                    }
                };
            }
        };
        asyncAction.start();
        latch.await();
        assertNotNull(response.get());
        assertFalse(nodeToContextMap.isEmpty());
        assertTrue(nodeToContextMap.toString(), nodeToContextMap.containsKey(primaryNode) || nodeToContextMap.containsKey(replicaNode));
        assertEquals(shardsIter.size(), numFreedContext.get());
        if (nodeToContextMap.containsKey(primaryNode)) {
            assertTrue(nodeToContextMap.get(primaryNode).toString(), nodeToContextMap.get(primaryNode).isEmpty());
        } else {
            assertTrue(nodeToContextMap.get(replicaNode).toString(), nodeToContextMap.get(replicaNode).isEmpty());
        }
        final List<Runnable> runnables = executor.shutdownNow();
        assertThat(runnables, equalTo(Collections.emptyList()));
    }

    public void testFanOutAndFail() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        request.allowPartialSearchResults(true);
        request.setMaxConcurrentShardRequests(randomIntBetween(1, 100));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(
            searchResponse -> { throw new AssertionError("unexpected response"); },
            exc -> {
                failure.set(exc);
                latch.countDown();
            }
        );
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node_2");

        Map<DiscoveryNode, Set<ShardSearchContextId>> nodeToContextMap = newConcurrentMap();
        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        int numShards = randomIntBetween(2, 10);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards,
            randomBoolean(),
            primaryNode,
            replicaNode
        );
        AtomicInteger numFreedContext = new AtomicInteger();
        SearchTransportService transportService = new SearchTransportService(null, null, null) {
            @Override
            public void sendFreeContext(Transport.Connection connection, ShardSearchContextId contextId, OriginalIndices originalIndices) {
                assertNotNull(contextId);
                numFreedContext.incrementAndGet();
                assertTrue(nodeToContextMap.containsKey(connection.getNode()));
                assertTrue(nodeToContextMap.get(connection.getNode()).remove(contextId));
            }
        };
        Map<String, Transport.Connection> lookup = new HashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", AliasFilter.EMPTY);
        ExecutorService executor = Executors.newFixedThreadPool(randomIntBetween(1, Runtime.getRuntime().availableProcessors()));
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction = new AbstractSearchAsyncAction<TestSearchPhaseResult>(
            "test",
            logger,
            transportService,
            (cluster, node) -> {
                assert cluster == null : "cluster was not null: " + cluster;
                return lookup.get(node);
            },
            aliasFilters,
            Collections.emptyMap(),
            executor,
            request,
            responseListener,
            shardsIter,
            new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
            ClusterState.EMPTY_STATE,
            null,
            new ArraySearchPhaseResults<>(shardsIter.size()),
            request.getMaxConcurrentShardRequests(),
            SearchResponse.Clusters.EMPTY
        ) {
            TestSearchResponse response = new TestSearchResponse();

            @Override
            protected void executePhaseOnShard(
                SearchShardIterator shardIt,
                SearchShardTarget shard,
                SearchActionListener<TestSearchPhaseResult> listener
            ) {
                assertTrue("shard: " + shard.getShardId() + " has been queried twice", response.queried.add(shard.getShardId()));
                Transport.Connection connection = getConnection(null, shard.getNodeId());
                final TestSearchPhaseResult testSearchPhaseResult;
                if (shard.getShardId().id() == 0) {
                    testSearchPhaseResult = new TestSearchPhaseResult(null, connection.getNode());
                } else {
                    testSearchPhaseResult = new TestSearchPhaseResult(
                        new ShardSearchContextId(UUIDs.randomBase64UUID(), contextIdGenerator.incrementAndGet()),
                        connection.getNode()
                    );
                    Set<ShardSearchContextId> ids = nodeToContextMap.computeIfAbsent(connection.getNode(), (n) -> newConcurrentSet());
                    ids.add(testSearchPhaseResult.getContextId());
                }
                if (randomBoolean()) {
                    listener.onResponse(testSearchPhaseResult);
                } else {
                    new Thread(() -> listener.onResponse(testSearchPhaseResult)).start();
                }
            }

            @Override
            protected SearchPhase getNextPhase(SearchPhaseResults<TestSearchPhaseResult> results, SearchPhaseContext context) {
                return new SearchPhase("test") {
                    @Override
                    public void run() {
                        throw new RuntimeException("boom");
                    }
                };
            }
        };
        asyncAction.start();
        latch.await();
        assertNotNull(failure.get());
        assertThat(failure.get().getCause().getMessage(), containsString("boom"));
        assertFalse(nodeToContextMap.isEmpty());
        assertTrue(nodeToContextMap.toString(), nodeToContextMap.containsKey(primaryNode) || nodeToContextMap.containsKey(replicaNode));
        assertEquals(shardsIter.size() - 1, numFreedContext.get());
        if (nodeToContextMap.containsKey(primaryNode)) {
            assertTrue(nodeToContextMap.get(primaryNode).toString(), nodeToContextMap.get(primaryNode).isEmpty());
        } else {
            assertTrue(nodeToContextMap.get(replicaNode).toString(), nodeToContextMap.get(replicaNode).isEmpty());
        }
        final List<Runnable> runnables = executor.shutdownNow();
        assertThat(runnables, equalTo(Collections.emptyList()));
    }

    public void testAllowPartialResults() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        request.allowPartialSearchResults(false);
        int numConcurrent = randomIntBetween(1, 5);
        request.setMaxConcurrentShardRequests(numConcurrent);
        int numShards = randomIntBetween(5, 10);
        AtomicBoolean searchPhaseDidRun = new AtomicBoolean(false);
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(
            response -> {},
            (e) -> { throw new AssertionError("unexpected", e); }
        );
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");
        // for the sake of this test we place the replica on the same node. ie. this is not a mistake since we limit per node now
        DiscoveryNode replicaNode = DiscoveryNodeUtils.create("node_1");

        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter(
            "idx",
            new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards,
            true,
            primaryNode,
            replicaNode
        );
        CountDownLatch latch = new CountDownLatch(1);

        SearchTransportService transportService = new SearchTransportService(null, null, null);
        Map<String, Transport.Connection> lookup = new HashMap<>();
        Map<ShardId, Boolean> seenShard = new ConcurrentHashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", AliasFilter.EMPTY);
        AtomicInteger numRequests = new AtomicInteger(0);
        AtomicInteger numFailReplicas = new AtomicInteger(0);
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction = new AbstractSearchAsyncAction<>(
            "test",
            logger,
            transportService,
            (cluster, node) -> {
                assert cluster == null : "cluster was not null: " + cluster;
                return lookup.get(node);
            },
            aliasFilters,
            Collections.emptyMap(),
            null,
            request,
            responseListener,
            shardsIter,
            new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
            ClusterState.EMPTY_STATE,
            null,
            new ArraySearchPhaseResults<>(shardsIter.size()),
            request.getMaxConcurrentShardRequests(),
            SearchResponse.Clusters.EMPTY
        ) {

            @Override
            protected void executePhaseOnShard(
                SearchShardIterator shardIt,
                SearchShardTarget shard,
                SearchActionListener<TestSearchPhaseResult> listener
            ) {
                seenShard.computeIfAbsent(shard.getShardId(), (i) -> {
                    numRequests.incrementAndGet(); // only count this once per shard copy
                    return Boolean.TRUE;
                });
                new Thread(() -> {
                    Transport.Connection connection = getConnection(null, shard.getNodeId());
                    TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(
                        new ShardSearchContextId(UUIDs.randomBase64UUID(), contextIdGenerator.incrementAndGet()),
                        connection.getNode()
                    );
                    if (shardIt.remaining() > 0) {
                        numFailReplicas.incrementAndGet();
                        listener.onFailure(new RuntimeException());
                    } else {
                        listener.onResponse(testSearchPhaseResult);
                    }
                }).start();
            }

            @Override
            protected SearchPhase getNextPhase(SearchPhaseResults<TestSearchPhaseResult> results, SearchPhaseContext context) {
                return new SearchPhase("test") {
                    @Override
                    public void run() {
                        assertTrue(searchPhaseDidRun.compareAndSet(false, true));
                        latch.countDown();
                    }
                };
            }
        };
        asyncAction.start();
        latch.await();
        assertTrue(searchPhaseDidRun.get());
        assertEquals(numShards, numRequests.get());
        assertThat(numFailReplicas.get(), greaterThanOrEqualTo(1));
    }

    public void testSkipUnavailableSearchShards() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        request.allowPartialSearchResults(true);

        AtomicReference<SearchResponse> searchResponse = new AtomicReference<>();
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(searchResponse::set, (e) -> {
            throw new AssertionError("unexpected", e);
        });
        DiscoveryNode primaryNode = DiscoveryNodeUtils.create("node_1");

        final int numUnavailableSkippedShards = randomIntBetween(1, 10);
        List<SearchShardIterator> searchShardIterators = new ArrayList<>(numUnavailableSkippedShards);
        OriginalIndices originalIndices = new OriginalIndices(new String[] { "idx" }, SearchRequest.DEFAULT_INDICES_OPTIONS);
        for (int i = 0; i < numUnavailableSkippedShards; i++) {
            Index index = new Index("idx", "_na_");
            SearchShardIterator searchShardIterator = new SearchShardIterator(
                null,
                new ShardId(index, 0),
                Collections.emptyList(),
                originalIndices
            );
            // Skip all the shards
            searchShardIterator.skip(true);
            searchShardIterator.reset();
            searchShardIterators.add(searchShardIterator);
        }
        GroupShardsIterator<SearchShardIterator> shardsIter = new GroupShardsIterator<>(searchShardIterators);
        Map<String, Transport.Connection> lookup = Map.of(primaryNode.getId(), new MockConnection(primaryNode));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean searchPhaseDidRun = new AtomicBoolean(false);
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction = new AbstractSearchAsyncAction<>(
            "test",
            logger,
            new SearchTransportService(null, null, null),
            (cluster, node) -> {
                assert cluster == null : "cluster was not null: " + cluster;
                return lookup.get(node);
            },
            Map.of("_na_", AliasFilter.EMPTY),
            Collections.emptyMap(),
            null,
            request,
            responseListener,
            shardsIter,
            new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
            ClusterState.EMPTY_STATE,
            null,
            new ArraySearchPhaseResults<>(shardsIter.size()),
            request.getMaxConcurrentShardRequests(),
            SearchResponse.Clusters.EMPTY
        ) {

            @Override
            protected void executePhaseOnShard(
                SearchShardIterator shardIt,
                SearchShardTarget shard,
                SearchActionListener<TestSearchPhaseResult> listener
            ) {
                assert false : "Expected to skip all shards";
            }

            @Override
            protected SearchPhase getNextPhase(SearchPhaseResults<TestSearchPhaseResult> results, SearchPhaseContext context) {
                return new SearchPhase("test") {
                    @Override
                    public void run() {
                        assertTrue(searchPhaseDidRun.compareAndSet(false, true));
                        latch.countDown();
                    }
                };
            }
        };
        asyncAction.start();
        assertThat(latch.await(4, TimeUnit.SECONDS), equalTo(true));
        assertThat(searchPhaseDidRun.get(), equalTo(true));

        asyncAction.sendSearchResponse(null, null);
        assertNotNull(searchResponse.get());
        assertThat(searchResponse.get().getSkippedShards(), equalTo(numUnavailableSkippedShards));
        assertThat(searchResponse.get().getFailedShards(), equalTo(0));
        assertThat(searchResponse.get().getSuccessfulShards(), equalTo(shardsIter.size()));
    }

    static GroupShardsIterator<SearchShardIterator> getShardsIter(
        String index,
        OriginalIndices originalIndices,
        int numShards,
        boolean doReplicas,
        DiscoveryNode primaryNode,
        DiscoveryNode replicaNode
    ) {
        return new GroupShardsIterator<>(
            getShardsIter(new Index(index, "_na_"), originalIndices, numShards, doReplicas, primaryNode, replicaNode)
        );
    }

    static List<SearchShardIterator> getShardsIter(
        Index index,
        OriginalIndices originalIndices,
        int numShards,
        boolean doReplicas,
        DiscoveryNode primaryNode,
        DiscoveryNode replicaNode
    ) {
        ArrayList<SearchShardIterator> list = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            ArrayList<ShardRouting> started = new ArrayList<>();
            ArrayList<ShardRouting> initializing = new ArrayList<>();
            ArrayList<ShardRouting> unassigned = new ArrayList<>();

            ShardRouting routing = ShardRouting.newUnassigned(
                new ShardId(index, i),
                true,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
                ShardRouting.Role.DEFAULT
            );
            if (primaryNode != null) {
                routing = routing.initialize(primaryNode.getId(), i + "p", 0);
                routing = routing.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                started.add(routing);
            }
            if (doReplicas && primaryNode != null) {
                routing = ShardRouting.newUnassigned(
                    new ShardId(index, i),
                    false,
                    RecoverySource.PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"),
                    ShardRouting.Role.DEFAULT
                );
                if (replicaNode != null) {
                    routing = routing.initialize(replicaNode.getId(), i + "r", 0);
                    if (randomBoolean()) {
                        routing = routing.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                        started.add(routing);
                    } else {
                        initializing.add(routing);
                    }
                } else {
                    unassigned.add(routing); // unused yet
                }
            }
            Collections.shuffle(started, random());
            started.addAll(initializing);
            list.add(new SearchShardIterator(null, new ShardId(index, i), started, originalIndices));
        }
        return list;
    }

    public static class TestSearchResponse extends SearchResponse {
        final Set<ShardId> queried = new HashSet<>();

        TestSearchResponse() {
            super(InternalSearchResponse.EMPTY_WITH_TOTAL_HITS, null, 0, 0, 0, 0L, ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY, null);
        }
    }

    public static class TestSearchPhaseResult extends SearchPhaseResult {
        final DiscoveryNode node;

        TestSearchPhaseResult(ShardSearchContextId contextId, DiscoveryNode node) {
            this.contextId = contextId;
            this.node = node;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }

    public static final class MockConnection implements Transport.Connection {

        private final DiscoveryNode node;

        MockConnection(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public DiscoveryNode getNode() {
            return node;
        }

        @Override
        public TransportVersion getTransportVersion() {
            return TransportVersion.current();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {}

        @Override
        public void addRemovedListener(ActionListener<Void> listener) {}

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onRemoved() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void incRef() {}

        @Override
        public boolean tryIncRef() {
            return true;
        }

        @Override
        public boolean decRef() {
            assert false : "shouldn't release a mock connection";
            return false;
        }

        @Override
        public boolean hasReferences() {
            return true;
        }
    }
}
