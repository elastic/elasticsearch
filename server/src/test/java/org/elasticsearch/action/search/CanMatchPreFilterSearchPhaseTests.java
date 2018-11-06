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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class CanMatchPreFilterSearchPhaseTests extends ESTestCase {

    public void testFilterShards() throws InterruptedException {

        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(),
            System::nanoTime);

        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));
        final boolean shard1 = randomBoolean();
        final boolean shard2 = randomBoolean();

        SearchTransportService searchTransportService = new SearchTransportService(null, null) {
            @Override
            public void sendCanMatch(Transport.Connection connection, ShardSearchTransportRequest request, SearchTask task,
                                     ActionListener<SearchService.CanMatchResponse> listener) {
                new Thread(() -> listener.onResponse(new SearchService.CanMatchResponse(request.shardId().id() == 0 ? shard1 :
                    shard2))).start();
            }
        };

        AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            2, randomBoolean(), primaryNode, replicaNode);
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);

        CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(), Collections.emptyMap(), EsExecutors.newDirectExecutorService(),
            searchRequest, null, shardsIter, timeProvider, 0, null,
            (iter) -> new SearchPhase("test") {
                    @Override
                    public void run() throws IOException {
                        result.set(iter);
                        latch.countDown();
                    }}, SearchResponse.Clusters.EMPTY);

        canMatchPhase.start();
        latch.await();

        if (shard1 && shard2) {
            for (SearchShardIterator i : result.get()) {
                assertFalse(i.skip());
            }
        } else if (shard1 == false &&  shard2 == false) {
            assertFalse(result.get().get(0).skip());
            assertTrue(result.get().get(1).skip());
        } else {
            assertEquals(0, result.get().get(0).shardId().id());
            assertEquals(1, result.get().get(1).shardId().id());
            assertEquals(shard1, !result.get().get(0).skip());
            assertEquals(shard2, !result.get().get(1).skip());
        }
    }

    public void testFilterWithFailure() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(),
            System::nanoTime);
        Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));
        final boolean shard1 = randomBoolean();
        SearchTransportService searchTransportService = new SearchTransportService(null, null) {
            @Override
            public void sendCanMatch(Transport.Connection connection, ShardSearchTransportRequest request, SearchTask task,
                                     ActionListener<SearchService.CanMatchResponse> listener) {
                boolean throwException = request.shardId().id() != 0;
                if (throwException && randomBoolean()) {
                    throw new IllegalArgumentException("boom");
                } else {
                    new Thread(() -> {
                        if (throwException == false) {
                            listener.onResponse(new SearchService.CanMatchResponse(shard1));
                        } else {
                            listener.onFailure(new NullPointerException());
                        }
                    }).start();
                }
            }
        };

        AtomicReference<GroupShardsIterator<SearchShardIterator>> result = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        GroupShardsIterator<SearchShardIterator> shardsIter = SearchAsyncActionTests.getShardsIter("idx",
            new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS),
            2, randomBoolean(), primaryNode, replicaNode);

        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);

        CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(logger,
            searchTransportService,
            (clusterAlias, node) -> lookup.get(node),
            Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
            Collections.emptyMap(), Collections.emptyMap(), EsExecutors.newDirectExecutorService(),
            searchRequest, null, shardsIter, timeProvider, 0, null,
            (iter) -> new SearchPhase("test") {
                @Override
                public void run() throws IOException {
                    result.set(iter);
                    latch.countDown();
                }}, SearchResponse.Clusters.EMPTY);

        canMatchPhase.start();
        latch.await();

        assertEquals(0, result.get().get(0).shardId().id());
        assertEquals(1, result.get().get(1).shardId().id());
        assertEquals(shard1, !result.get().get(0).skip());
        assertFalse(result.get().get(1).skip()); // never skip the failure
    }

    /*
     * In cases that a query coordinating node held all the shards for a query, the can match phase would recurse and end in stack overflow
     * when subjected to max concurrent search requests. This test is a test for that situation.
     */
    public void testLotsOfShards() throws InterruptedException {
        final TransportSearchAction.SearchTimeProvider timeProvider =
                new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);

        final Map<String, Transport.Connection> lookup = new ConcurrentHashMap<>();
        final DiscoveryNode primaryNode = new DiscoveryNode("node_1", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode replicaNode = new DiscoveryNode("node_2", buildNewFakeTransportAddress(), Version.CURRENT);
        lookup.put("node1", new SearchAsyncActionTests.MockConnection(primaryNode));
        lookup.put("node2", new SearchAsyncActionTests.MockConnection(replicaNode));


        final SearchTransportService searchTransportService =
                new SearchTransportService(null, null) {
                    @Override
                    public void sendCanMatch(
                            Transport.Connection connection,
                            ShardSearchTransportRequest request,
                            SearchTask task,
                            ActionListener<SearchService.CanMatchResponse> listener) {
                        listener.onResponse(new SearchService.CanMatchResponse(randomBoolean()));
                    }
                };

        final CountDownLatch latch = new CountDownLatch(1);
        final OriginalIndices originalIndices = new OriginalIndices(new String[]{"idx"}, SearchRequest.DEFAULT_INDICES_OPTIONS);
        final GroupShardsIterator<SearchShardIterator> shardsIter =
                SearchAsyncActionTests.getShardsIter("idx", originalIndices, 4096, randomBoolean(), primaryNode, replicaNode);
        final ExecutorService executor = Executors.newFixedThreadPool(randomIntBetween(1, Runtime.getRuntime().availableProcessors()));
        final SearchRequest searchRequest = new SearchRequest();
        searchRequest.allowPartialSearchResults(true);
        final CanMatchPreFilterSearchPhase canMatchPhase = new CanMatchPreFilterSearchPhase(
                logger,
                searchTransportService,
                (clusterAlias, node) -> lookup.get(node),
                Collections.singletonMap("_na_", new AliasFilter(null, Strings.EMPTY_ARRAY)),
                Collections.emptyMap(),
                Collections.emptyMap(),
                EsExecutors.newDirectExecutorService(),
                searchRequest,
                null,
                shardsIter,
                timeProvider,
                0,
                null,
                (iter) -> new InitialSearchPhase<SearchPhaseResult>("test", searchRequest,
                        iter, logger, randomIntBetween(1, 32), executor) {
                    @Override
                    void onPhaseDone() {
                        latch.countDown();
                    }

                    @Override
                    void onShardFailure(final int shardIndex, final SearchShardTarget shardTarget, final Exception ex) {

                    }

                    @Override
                    void onShardSuccess(final SearchPhaseResult result) {

                    }

                    @Override
                    protected void executePhaseOnShard(
                            final SearchShardIterator shardIt,
                            final ShardRouting shard,
                            final SearchActionListener<SearchPhaseResult> listener) {
                        if (randomBoolean()) {
                            listener.onResponse(new SearchPhaseResult() {});
                        } else {
                            listener.onFailure(new Exception("failure"));
                        }
                    }
                }, SearchResponse.Clusters.EMPTY);

        canMatchPhase.start();
        latch.await();
        executor.shutdown();
    }
}
