/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContextId;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentSet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class SearchAsyncActionTests extends ESTestCase {

    public void testSkipSearchShards() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        request.allowPartialSearchResults(true);
        int numShards = 10;
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(response -> {},
            (e) -> { throw new AssertionError("unexpected", e);});
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);

        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards, randomBoolean(), primaryNode, replicaNode);
        int numSkipped = 0;
        for (SearchShardIterator iter : shardsIter) {
            if (iter.shardId().id() % 2 == 0) {
                iter.resetAndSkip();
                numSkipped++;
            }
        }
        CountDownLatch latch = new CountDownLatch(numShards - numSkipped);
        AtomicBoolean searchPhaseDidRun = new AtomicBoolean(false);

        SearchTransportService transportService = new SearchTransportService(null, null);
        Map<String, Transport.Connection> lookup = new HashMap<>();
        Map<ShardId, Boolean> seenShard = new ConcurrentHashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY));
        AtomicInteger numRequests = new AtomicInteger(0);
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction =
            new AbstractSearchAsyncAction<TestSearchPhaseResult>(
                "test",
                logger,
                transportService,
                (cluster, node) -> {
                    assert cluster == null : "cluster was not null: " + cluster;
                    return lookup.get(node); },
                aliasFilters,
                Collections.emptyMap(),
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
                SearchResponse.Clusters.EMPTY) {

                @Override
                protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard,
                                                   SearchActionListener<TestSearchPhaseResult> listener) {
                    seenShard.computeIfAbsent(shard.shardId(), (i) -> {
                        numRequests.incrementAndGet(); // only count this once per replica
                        return Boolean.TRUE;
                    });

                    new Thread(() -> {
                        Transport.Connection connection = getConnection(null, shard.currentNodeId());
                        TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(
                            new SearchContextId(UUIDs.randomBase64UUID(), contextIdGenerator.incrementAndGet()),
                        connection.getNode());
                        listener.onResponse(testSearchPhaseResult);

                    }).start();
                }

                @Override
                protected SearchPhase getNextPhase(SearchPhaseResults<TestSearchPhaseResult> results, SearchPhaseContext context) {
                    return new SearchPhase("test") {
                        @Override
                        public void run() {
                            assertTrue(searchPhaseDidRun.compareAndSet(false, true));
                        }
                    };
                }

                @Override
                protected void executeNext(Runnable runnable, Thread originalThread) {
                    super.executeNext(runnable, originalThread);
                    latch.countDown();
                }
            };
        asyncAction.start();
        latch.await();
        assertTrue(searchPhaseDidRun.get());
        SearchResponse searchResponse = asyncAction.buildSearchResponse(null, null, asyncAction.buildShardFailures());
        assertEquals(shardsIter.size() - numSkipped, numRequests.get());
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(numSkipped, searchResponse.getSkippedShards());
        assertEquals(shardsIter.size(), searchResponse.getSuccessfulShards());
    }

    public void testLimitConcurrentShardRequests() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        request.allowPartialSearchResults(true);
        int numConcurrent = randomIntBetween(1, 5);
        request.setMaxConcurrentShardRequests(numConcurrent);
        boolean doReplicas = randomBoolean();
        int numShards = randomIntBetween(5, 10);
        int numShardAttempts = numShards;
        Boolean[] shardFailures = new Boolean[numShards];
        // at least one response otherwise the entire request fails
        shardFailures[randomIntBetween(0, shardFailures.length - 1)] = false;
        for (int i = 0; i < shardFailures.length; i++) {
            if (shardFailures[i] == null) {
                boolean failure = randomBoolean();
                shardFailures[i] = failure;
                if (failure && doReplicas) {
                    numShardAttempts++;
                }
            }
        }
        CountDownLatch latch = new CountDownLatch(numShardAttempts);
        AtomicBoolean searchPhaseDidRun = new AtomicBoolean(false);
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(response -> {},
            (e) -> { throw new AssertionError("unexpected", e);});
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        // for the sake of this test we place the replica on the same node. ie. this is not a mistake since we limit per node now
        DiscoveryNode replicaNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);

        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards, doReplicas, primaryNode, replicaNode);
        SearchTransportService transportService = new SearchTransportService(null, null);
        Map<String, Transport.Connection> lookup = new HashMap<>();
        Map<ShardId, Boolean> seenShard = new ConcurrentHashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY));
        CountDownLatch awaitInitialRequests = new CountDownLatch(1);
        AtomicInteger numRequests = new AtomicInteger(0);
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction =
            new AbstractSearchAsyncAction<TestSearchPhaseResult>(
                "test",
                logger,
                transportService,
                (cluster, node) -> {
                    assert cluster == null : "cluster was not null: " + cluster;
                    return lookup.get(node); },
                aliasFilters,
                Collections.emptyMap(),
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
                SearchResponse.Clusters.EMPTY) {

                @Override
                protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard,
                                                   SearchActionListener<TestSearchPhaseResult> listener) {
                    seenShard.computeIfAbsent(shard.shardId(), (i) -> {
                        numRequests.incrementAndGet(); // only count this once per shard copy
                        return Boolean.TRUE;
                    });

                    new Thread(() -> {
                        try {
                            awaitInitialRequests.await();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        Transport.Connection connection = getConnection(null, shard.currentNodeId());
                        TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(
                            new SearchContextId(UUIDs.randomBase64UUID(), contextIdGenerator.incrementAndGet()), connection.getNode());
                        if (shardFailures[shard.shardId().id()]) {
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
                        }
                    };
                }

                @Override
                protected void executeNext(Runnable runnable, Thread originalThread) {
                    super.executeNext(runnable, originalThread);
                    latch.countDown();
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
            (e) -> { throw new AssertionError("unexpected", e);});
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);

        Map<DiscoveryNode, Set<SearchContextId>> nodeToContextMap = newConcurrentMap();
        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        int numShards = randomIntBetween(1, 10);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
                new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
                numShards, randomBoolean(), primaryNode, replicaNode);
        AtomicInteger numFreedContext = new AtomicInteger();
        SearchTransportService transportService = new SearchTransportService(null, null) {
            @Override
            public void sendFreeContext(Transport.Connection connection, SearchContextId contextId, OriginalIndices originalIndices) {
                numFreedContext.incrementAndGet();
                assertTrue(nodeToContextMap.containsKey(connection.getNode()));
                assertTrue(nodeToContextMap.get(connection.getNode()).remove(contextId));
            }
        };
        Map<String, Transport.Connection> lookup = new HashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY));
        ExecutorService executor = Executors.newFixedThreadPool(randomIntBetween(1, Runtime.getRuntime().availableProcessors()));
        final CountDownLatch latch = new CountDownLatch(numShards);
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction =
                new AbstractSearchAsyncAction<TestSearchPhaseResult>(
                        "test",
                        logger,
                        transportService,
                        (cluster, node) -> {
                            assert cluster == null : "cluster was not null: " + cluster;
                            return lookup.get(node); },
                        aliasFilters,
                        Collections.emptyMap(),
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
                        SearchResponse.Clusters.EMPTY) {
            TestSearchResponse response = new TestSearchResponse();

            @Override
            protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard, SearchActionListener<TestSearchPhaseResult>
                listener) {
                assertTrue("shard: " + shard.shardId() + " has been queried twice", response.queried.add(shard.shardId()));
                Transport.Connection connection = getConnection(null, shard.currentNodeId());
                TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(
                    new SearchContextId(UUIDs.randomBase64UUID(), contextIdGenerator.incrementAndGet()), connection.getNode());
                Set<SearchContextId> ids = nodeToContextMap.computeIfAbsent(connection.getNode(), (n) -> newConcurrentSet());
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
                    }
                };
            }

            @Override
            protected void executeNext(Runnable runnable, Thread originalThread) {
                super.executeNext(runnable, originalThread);
                latch.countDown();
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
        executor.shutdown();
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
            });
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);

        Map<DiscoveryNode, Set<SearchContextId>> nodeToContextMap = newConcurrentMap();
        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        int numShards = randomIntBetween(2, 10);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards, randomBoolean(), primaryNode, replicaNode);
        AtomicInteger numFreedContext = new AtomicInteger();
        SearchTransportService transportService = new SearchTransportService(null, null) {
            @Override
            public void sendFreeContext(Transport.Connection connection, SearchContextId contextId, OriginalIndices originalIndices) {
                assertNotNull(contextId);
                numFreedContext.incrementAndGet();
                assertTrue(nodeToContextMap.containsKey(connection.getNode()));
                assertTrue(nodeToContextMap.get(connection.getNode()).remove(contextId));
            }
        };
        Map<String, Transport.Connection> lookup = new HashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY));
        ExecutorService executor = Executors.newFixedThreadPool(randomIntBetween(1, Runtime.getRuntime().availableProcessors()));
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction =
            new AbstractSearchAsyncAction<TestSearchPhaseResult>(
                "test",
                logger,
                transportService,
                (cluster, node) -> {
                    assert cluster == null : "cluster was not null: " + cluster;
                    return lookup.get(node); },
                aliasFilters,
                Collections.emptyMap(),
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
                SearchResponse.Clusters.EMPTY) {
                TestSearchResponse response = new TestSearchResponse();

                @Override
                protected void executePhaseOnShard(SearchShardIterator shardIt,
                                                   ShardRouting shard,
                                                   SearchActionListener<TestSearchPhaseResult> listener) {
                    assertTrue("shard: " + shard.shardId() + " has been queried twice", response.queried.add(shard.shardId()));
                    Transport.Connection connection = getConnection(null, shard.currentNodeId());
                    final TestSearchPhaseResult testSearchPhaseResult;
                    if (shard.shardId().id() == 0) {
                        testSearchPhaseResult = new TestSearchPhaseResult(null, connection.getNode());
                    } else {
                        testSearchPhaseResult = new TestSearchPhaseResult(new SearchContextId(UUIDs.randomBase64UUID(),
                            contextIdGenerator.incrementAndGet()), connection.getNode());
                        Set<SearchContextId> ids = nodeToContextMap.computeIfAbsent(connection.getNode(), (n) -> newConcurrentSet());
                        ids.add(testSearchPhaseResult.getContextId());
                    }
                    if (randomBoolean()) {
                        listener.onResponse(testSearchPhaseResult);
                    } else {
                        new Thread(() -> listener.onResponse(testSearchPhaseResult)).start();
                    }
                }

                @Override
                protected SearchPhase getNextPhase(SearchPhaseResults<TestSearchPhaseResult> results,
                                                   SearchPhaseContext context) {
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
        assertEquals(shardsIter.size()-1, numFreedContext.get());
        if (nodeToContextMap.containsKey(primaryNode)) {
            assertTrue(nodeToContextMap.get(primaryNode).toString(), nodeToContextMap.get(primaryNode).isEmpty());
        } else {
            assertTrue(nodeToContextMap.get(replicaNode).toString(), nodeToContextMap.get(replicaNode).isEmpty());
        }
        executor.shutdown();
    }

    public void testAllowPartialResults() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        request.allowPartialSearchResults(false);
        int numConcurrent = randomIntBetween(1, 5);
        request.setMaxConcurrentShardRequests(numConcurrent);
        int numShards = randomIntBetween(5, 10);
        AtomicBoolean searchPhaseDidRun = new AtomicBoolean(false);
        ActionListener<SearchResponse> responseListener = ActionListener.wrap(response -> {},
            (e) -> { throw new AssertionError("unexpected", e);} );
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        // for the sake of this test we place the replica on the same node. ie. this is not a mistake since we limit per node now
        DiscoveryNode replicaNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);

        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            numShards, true, primaryNode, replicaNode);
        int numShardAttempts = 0;
        for (SearchShardIterator it : shardsIter) {
            numShardAttempts += it.remaining();
        }
        CountDownLatch latch = new CountDownLatch(numShardAttempts);

        SearchTransportService transportService = new SearchTransportService(null, null);
        Map<String, Transport.Connection> lookup = new HashMap<>();
        Map<ShardId, Boolean> seenShard = new ConcurrentHashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY));
        AtomicInteger numRequests = new AtomicInteger(0);
        AtomicInteger numFailReplicas = new AtomicInteger(0);
        AbstractSearchAsyncAction<TestSearchPhaseResult> asyncAction =
            new AbstractSearchAsyncAction<>(
                "test",
                logger,
                transportService,
                (cluster, node) -> {
                    assert cluster == null : "cluster was not null: " + cluster;
                    return lookup.get(node); },
                aliasFilters,
                Collections.emptyMap(),
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
                SearchResponse.Clusters.EMPTY) {

                @Override
                protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard,
                                                   SearchActionListener<TestSearchPhaseResult> listener) {
                    seenShard.computeIfAbsent(shard.shardId(), (i) -> {
                        numRequests.incrementAndGet(); // only count this once per shard copy
                        return Boolean.TRUE;
                    });
                    new Thread(() -> {
                        Transport.Connection connection = getConnection(null, shard.currentNodeId());
                        TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(
                            new SearchContextId(UUIDs.randomBase64UUID(), contextIdGenerator.incrementAndGet()), connection.getNode());
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
                        }
                    };
                }

                @Override
                protected void executeNext(Runnable runnable, Thread originalThread) {
                    super.executeNext(runnable, originalThread);
                    latch.countDown();
                }
            };
        asyncAction.start();
        latch.await();
        assertTrue(searchPhaseDidRun.get());
        assertEquals(numShards, numRequests.get());
        assertThat(numFailReplicas.get(), greaterThanOrEqualTo(1));
    }

    static GroupShardsIterator<SearchShardIterator> getShardsIter(String index, OriginalIndices originalIndices, int numShards,
                                                     boolean doReplicas, DiscoveryNode primaryNode, DiscoveryNode replicaNode) {
        ArrayList<SearchShardIterator> list = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            ArrayList<ShardRouting> started = new ArrayList<>();
            ArrayList<ShardRouting> initializing = new ArrayList<>();
            ArrayList<ShardRouting> unassigned = new ArrayList<>();

            ShardRouting routing = ShardRouting.newUnassigned(new ShardId(new Index(index, "_na_"), i), true,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"));
            routing = routing.initialize(primaryNode.getId(), i + "p", 0);
            routing.started();
            started.add(routing);
            if (doReplicas) {
                routing = ShardRouting.newUnassigned(new ShardId(new Index(index, "_na_"), i), false,
                    RecoverySource.PeerRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"));
                if (replicaNode != null) {
                    routing = routing.initialize(replicaNode.getId(), i + "r", 0);
                    if (randomBoolean()) {
                        routing.started();
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
            list.add(new SearchShardIterator(null, new ShardId(new Index(index, "_na_"), i), started, originalIndices));
        }
        return new GroupShardsIterator<>(list);
    }

    public static class TestSearchResponse extends SearchResponse {
        final Set<ShardId> queried = new HashSet<>();

        TestSearchResponse() {
            super(InternalSearchResponse.empty(), null, 0, 0, 0, 0L, ShardSearchFailure.EMPTY_ARRAY, Clusters.EMPTY);
        }
    }

    public static class TestSearchPhaseResult extends SearchPhaseResult {
        final DiscoveryNode node;
        TestSearchPhaseResult(SearchContextId contextId, DiscoveryNode node) {
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
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {

        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
            throw new UnsupportedOperationException();
        }
    }
}
