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
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.internal.AliasFilter;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SearchAsyncActionTests extends ESTestCase {

    public void testSkipSearchShards() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestSearchResponse> response = new AtomicReference<>();
        ActionListener<SearchResponse> responseListener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                response.set((TestSearchResponse) searchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("test failed", e);
                fail(e.getMessage());
            }
        };
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);

        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, IndicesOptions.strictExpandOpenAndForbidClosed()),
            10, randomBoolean(), primaryNode, replicaNode);
        int numSkipped = 0;
        for (SearchShardIterator iter : shardsIter) {
            if (iter.shardId().id() % 2 == 0) {
                iter.resetAndSkip();
                numSkipped++;
            }
        }

        SearchTransportService transportService = new SearchTransportService(Settings.EMPTY, null, null);
        Map<String, Transport.Connection> lookup = new HashMap<>();
        Map<ShardId, Boolean> seenShard = new ConcurrentHashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY));
        AtomicInteger numRequests = new AtomicInteger(0);
        AbstractSearchAsyncAction asyncAction =
            new AbstractSearchAsyncAction<TestSearchPhaseResult>(
                "test",
                logger,
                transportService,
                (cluster, node) -> {
                    assert cluster == null : "cluster was not null: " + cluster;
                    return lookup.get(node); },
                aliasFilters,
                Collections.emptyMap(),
                null,
                request,
                responseListener,
                shardsIter,
                new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
                0,
                null,
                new InitialSearchPhase.ArraySearchPhaseResults<>(shardsIter.size()),
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
                        TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(contextIdGenerator.incrementAndGet(),
                        connection.getNode());
                        listener.onResponse(testSearchPhaseResult);

                    }).start();
                }

                @Override
                protected SearchPhase getNextPhase(SearchPhaseResults<TestSearchPhaseResult> results, SearchPhaseContext context) {
                    return new SearchPhase("test") {
                        @Override
                        public void run() throws IOException {
                            latch.countDown();
                        }
                    };
                }
            };
        asyncAction.start();
        latch.await();
        SearchResponse searchResponse = asyncAction.buildSearchResponse(null, null);
        assertEquals(shardsIter.size()-numSkipped, numRequests.get());
        assertEquals(0, searchResponse.getFailedShards());
        assertEquals(numSkipped, searchResponse.getSkippedShards());
        assertEquals(shardsIter.size(), searchResponse.getSuccessfulShards());
    }

    public void testLimitConcurrentShardRequests() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        int numConcurrent = randomIntBetween(1, 5);
        request.setMaxConcurrentShardRequests(numConcurrent);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestSearchResponse> response = new AtomicReference<>();
        ActionListener<SearchResponse> responseListener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                response.set((TestSearchResponse) searchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("test failed", e);
                fail(e.getMessage());
            }
        };
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);

        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, IndicesOptions.strictExpandOpenAndForbidClosed()),
            10, randomBoolean(), primaryNode, replicaNode);
        SearchTransportService transportService = new SearchTransportService(Settings.EMPTY, null, null);
        Map<String, Transport.Connection> lookup = new HashMap<>();
        Map<ShardId, Boolean> seenShard = new ConcurrentHashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY));
        CountDownLatch awaitInitialRequests = new CountDownLatch(1);
        AtomicInteger numRequests = new AtomicInteger(0);
        AtomicInteger numResponses = new AtomicInteger(0);
        AbstractSearchAsyncAction asyncAction =
            new AbstractSearchAsyncAction<TestSearchPhaseResult>(
                "test",
                logger,
                transportService,
                (cluster, node) -> {
                    assert cluster == null : "cluster was not null: " + cluster;
                    return lookup.get(node); },
                aliasFilters,
                Collections.emptyMap(),
                null,
                request,
                responseListener,
                shardsIter,
                new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
                0,
                null,
                new InitialSearchPhase.ArraySearchPhaseResults<>(shardsIter.size()),
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
                        try {
                            awaitInitialRequests.await();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        Transport.Connection connection = getConnection(null, shard.currentNodeId());
                        TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(contextIdGenerator.incrementAndGet(),
                            connection.getNode());
                        if (numResponses.getAndIncrement() > 0 && randomBoolean()) { // at least one response otherwise the entire
                            // request fails
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
                        public void run() throws IOException {
                            latch.countDown();
                        }
                    };
                }
            };
        asyncAction.start();
        assertEquals(numConcurrent, numRequests.get());
        awaitInitialRequests.countDown();
        latch.await();
        assertEquals(10, numRequests.get());
    }

    public void testFanOutAndCollect() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        request.setMaxConcurrentShardRequests(randomIntBetween(1, 100));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestSearchResponse> response = new AtomicReference<>();
        ActionListener<SearchResponse> responseListener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                response.set((TestSearchResponse) searchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("test failed", e);
                fail(e.getMessage());
            }
        };
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);

        Map<DiscoveryNode, Set<Long>> nodeToContextMap = new HashMap<>();
        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        GroupShardsIterator<SearchShardIterator> shardsIter = getShardsIter("idx",
                new OriginalIndices(new String[]{"idx"}, IndicesOptions.strictExpandOpenAndForbidClosed()),
                randomIntBetween(1, 10), randomBoolean(), primaryNode, replicaNode);
        AtomicInteger numFreedContext = new AtomicInteger();
        SearchTransportService transportService = new SearchTransportService(Settings.EMPTY, null, null) {
            @Override
            public void sendFreeContext(Transport.Connection connection, long contextId, OriginalIndices originalIndices) {
                numFreedContext.incrementAndGet();
                assertTrue(nodeToContextMap.containsKey(connection.getNode()));
                assertTrue(nodeToContextMap.get(connection.getNode()).remove(contextId));
            }
        };
        Map<String, Transport.Connection> lookup = new HashMap<>();
        lookup.put(primaryNode.getId(), new MockConnection(primaryNode));
        lookup.put(replicaNode.getId(), new MockConnection(replicaNode));
        Map<String, AliasFilter> aliasFilters = Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY));
        final ExecutorService executor = Executors.newFixedThreadPool(randomIntBetween(1, Runtime.getRuntime().availableProcessors()));
        AbstractSearchAsyncAction asyncAction =
                new AbstractSearchAsyncAction<TestSearchPhaseResult>(
                        "test",
                        logger,
                        transportService,
                        (cluster, node) -> {
                            assert cluster == null : "cluster was not null: " + cluster;
                            return lookup.get(node); },
                        aliasFilters,
                        Collections.emptyMap(),
                        executor,
                        request,
                        responseListener,
                        shardsIter,
                        new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
                        0,
                        null,
                        new InitialSearchPhase.ArraySearchPhaseResults<>(shardsIter.size()),
                        request.getMaxConcurrentShardRequests(),
                        SearchResponse.Clusters.EMPTY) {
            TestSearchResponse response = new TestSearchResponse();

            @Override
            protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard, SearchActionListener<TestSearchPhaseResult>
                listener) {
                assertTrue("shard: " + shard.shardId() + " has been queried twice", response.queried.add(shard.shardId()));
                Transport.Connection connection = getConnection(null, shard.currentNodeId());
                TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(contextIdGenerator.incrementAndGet(),
                    connection.getNode());
                Set<Long> ids = nodeToContextMap.computeIfAbsent(connection.getNode(), (n) -> new HashSet<>());
                ids.add(testSearchPhaseResult.getRequestId());
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
                    public void run() throws IOException {
                        for (int i = 0; i < results.getNumShards(); i++) {
                            TestSearchPhaseResult result = results.getAtomicArray().get(i);
                            assertEquals(result.node.getId(), result.getSearchShardTarget().getNodeId());
                            sendReleaseSearchContext(result.getRequestId(), new MockConnection(result.node), OriginalIndices.NONE);
                        }
                        responseListener.onResponse(response);
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
        executor.shutdown();
    }

    static GroupShardsIterator<SearchShardIterator> getShardsIter(String index, OriginalIndices originalIndices, int numShards,
                                                     boolean doReplicas, DiscoveryNode primaryNode, DiscoveryNode replicaNode) {
        ArrayList<SearchShardIterator> list = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            ArrayList<ShardRouting> started = new ArrayList<>();
            ArrayList<ShardRouting> initializing = new ArrayList<>();
            ArrayList<ShardRouting> unassigned = new ArrayList<>();

            ShardRouting routing = ShardRouting.newUnassigned(new ShardId(new Index(index, "_na_"), i), true,
                RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"));
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
        public final Set<ShardId> queried = new HashSet<>();
    }

    public static class TestSearchPhaseResult extends SearchPhaseResult {
        final DiscoveryNode node;

        public TestSearchPhaseResult(long id, DiscoveryNode node) {
            this.requestId = id;
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }

    public static final class MockConnection implements Transport.Connection {

        private final DiscoveryNode node;

        public MockConnection(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public DiscoveryNode getNode() {
            return node;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
