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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.SearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class SearchScrollAsyncActionTests extends ESTestCase {

    public void testSendRequestsToNodes() throws InterruptedException {

        ParsedScrollId scrollId = getParsedScrollId(
            new ScrollIdForNode(null, "node1", new SearchContextId(UUIDs.randomBase64UUID(), 1)),
            new ScrollIdForNode(null, "node2", new SearchContextId(UUIDs.randomBase64UUID(), 2)),
            new ScrollIdForNode(null, "node3", new SearchContextId(UUIDs.randomBase64UUID(), 17)),
            new ScrollIdForNode(null, "node1", new SearchContextId(UUIDs.randomBase64UUID(), 0)),
            new ScrollIdForNode(null, "node3", new SearchContextId(UUIDs.randomBase64UUID(), 0)));
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT)).build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action =
            new SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult>(scrollId, logger, discoveryNodes, dummyListener(),
                null, request, null)
            {
                @Override
                protected void executeInitialPhase(Transport.Connection connection, InternalScrollSearchRequest internalRequest,
                                                   SearchActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener)
                {
                    new Thread(() -> {
                        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult =
                            new SearchAsyncActionTests.TestSearchPhaseResult(internalRequest.contextId(), connection.getNode());
                        testSearchPhaseResult.setSearchShardTarget(new SearchShardTarget(connection.getNode().getId(),
                            new ShardId("test", "_na_", 1), null, OriginalIndices.NONE));
                        searchActionListener.onResponse(testSearchPhaseResult);
                    }).start();
                }

                @Override
                protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }

                @Override
                protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                    assertEquals(1, movedCounter.incrementAndGet());
                    return new SearchPhase("test") {
                        @Override
                        public void run() throws IOException {
                            latch.countDown();
                        }
                    };
                }

                @Override
                protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                    results.setOnce(shardId, result);
                }
            };

        action.run();
        latch.await();
        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(0, shardSearchFailures.length);
        ScrollIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            assertNotNull(results.get(i));
            assertEquals(context[i].getContextId(), results.get(i).getContextId());
            assertEquals(context[i].getNode(), results.get(i).node.getId());
        }
    }

    public void testFailNextPhase() throws InterruptedException {

        ParsedScrollId scrollId = getParsedScrollId(
            new ScrollIdForNode(null, "node1", new SearchContextId("", 1)),
            new ScrollIdForNode(null, "node2", new SearchContextId("a", 2)),
            new ScrollIdForNode(null, "node3", new SearchContextId("b", 17)),
            new ScrollIdForNode(null, "node1", new SearchContextId("c", 0)),
            new ScrollIdForNode(null, "node3", new SearchContextId("d", 0)));
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT)).build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse o) {
                try {
                    fail("got a result");
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    assertTrue(e instanceof SearchPhaseExecutionException);
                    SearchPhaseExecutionException ex = (SearchPhaseExecutionException) e;
                    assertEquals("BOOM", ex.getCause().getMessage());
                    assertEquals("TEST_PHASE", ex.getPhaseName());
                    assertEquals("Phase failed", ex.getMessage());
                } finally {
                    latch.countDown();
                }
            }
        };
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action =
            new SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult>(scrollId, logger, discoveryNodes, listener, null,
                request, null) {
                @Override
                protected void executeInitialPhase(Transport.Connection connection, InternalScrollSearchRequest internalRequest,
                                                   SearchActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener)
                {
                    new Thread(() -> {
                        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult =
                            new SearchAsyncActionTests.TestSearchPhaseResult(internalRequest.contextId(), connection.getNode());
                        testSearchPhaseResult.setSearchShardTarget(new SearchShardTarget(connection.getNode().getId(),
                            new ShardId("test", "_na_", 1), null, OriginalIndices.NONE));
                        searchActionListener.onResponse(testSearchPhaseResult);
                    }).start();
                }

                @Override
                protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }

                @Override
                protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                    assertEquals(1, movedCounter.incrementAndGet());
                    return new SearchPhase("TEST_PHASE") {
                        @Override
                        public void run() throws IOException {
                            throw new IllegalArgumentException("BOOM");
                        }
                    };
                }

                @Override
                protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                    results.setOnce(shardId, result);
                }
            };

        action.run();
        latch.await();
        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(0, shardSearchFailures.length);
        ScrollIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            assertNotNull(results.get(i));
            assertEquals(context[i].getContextId(), results.get(i).getContextId());
            assertEquals(context[i].getNode(), results.get(i).node.getId());
        }
    }

    public void testNodeNotAvailable() throws InterruptedException {
        ParsedScrollId scrollId = getParsedScrollId(
            new ScrollIdForNode(null, "node1", new SearchContextId("", 1)),
            new ScrollIdForNode(null, "node2", new SearchContextId("", 2)),
            new ScrollIdForNode(null, "node3", new SearchContextId("", 17)),
            new ScrollIdForNode(null, "node1", new SearchContextId("", 0)),
            new ScrollIdForNode(null, "node3", new SearchContextId("", 0)));
        // node2 is not available
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT)).build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action =
            new SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult>(scrollId, logger, discoveryNodes, dummyListener()
                , null, request, null)
            {
                @Override
                protected void executeInitialPhase(Transport.Connection connection, InternalScrollSearchRequest internalRequest,
                                                   SearchActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener)
                {
                    try {
                        assertNotEquals("node2 is not available", "node2", connection.getNode().getId());
                    } catch (NullPointerException e) {
                        logger.warn(e);
                    }
                    new Thread(() -> {
                        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult =
                            new SearchAsyncActionTests.TestSearchPhaseResult(internalRequest.contextId(), connection.getNode());
                        testSearchPhaseResult.setSearchShardTarget(new SearchShardTarget(connection.getNode().getId(),
                            new ShardId("test", "_na_", 1), null, OriginalIndices.NONE));
                        searchActionListener.onResponse(testSearchPhaseResult);
                    }).start();
                }

                @Override
                protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }

                @Override
                protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                    assertEquals(1, movedCounter.incrementAndGet());
                    return new SearchPhase("test") {
                        @Override
                        public void run() throws IOException {
                            latch.countDown();
                        }
                    };
                }

                @Override
                protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                    results.setOnce(shardId, result);
                }
            };

        action.run();
        latch.await();
        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(1, shardSearchFailures.length);
        // .reason() returns the full stack trace
        assertThat(shardSearchFailures[0].reason(), startsWith("java.lang.IllegalStateException: node [node2] is not available"));

        ScrollIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            if (context[i].getNode().equals("node2")) {
                assertNull(results.get(i));
            } else {
                assertNotNull(results.get(i));
                assertEquals(context[i].getContextId(), results.get(i).getContextId());
                assertEquals(context[i].getNode(), results.get(i).node.getId());
            }
        }
    }

    public void testShardFailures() throws InterruptedException {
        ParsedScrollId scrollId = getParsedScrollId(
            new ScrollIdForNode(null, "node1", new SearchContextId("", 1)),
            new ScrollIdForNode(null, "node2", new SearchContextId("", 2)),
            new ScrollIdForNode(null, "node3", new SearchContextId("",17)),
            new ScrollIdForNode(null, "node1", new SearchContextId("", 0)),
            new ScrollIdForNode(null, "node3", new SearchContextId("", 0)));
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT)).build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action =
            new SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult>(scrollId, logger, discoveryNodes, dummyListener(),
                null, request, null)
            {
                @Override
                protected void executeInitialPhase(Transport.Connection connection, InternalScrollSearchRequest internalRequest,
                                                   SearchActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener)
                {
                    new Thread(() -> {
                        if (internalRequest.contextId().getId() == 17) {
                            searchActionListener.onFailure(new IllegalArgumentException("BOOM on shard"));
                        } else {
                            SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult =
                                new SearchAsyncActionTests.TestSearchPhaseResult(internalRequest.contextId(), connection.getNode());
                            testSearchPhaseResult.setSearchShardTarget(new SearchShardTarget(connection.getNode().getId(),
                                new ShardId("test", "_na_", 1), null, OriginalIndices.NONE));
                            searchActionListener.onResponse(testSearchPhaseResult);
                        }
                    }).start();
                }

                @Override
                protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }

                @Override
                protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                    assertEquals(1, movedCounter.incrementAndGet());
                    return new SearchPhase("test") {
                        @Override
                        public void run() throws IOException {
                            latch.countDown();
                        }
                    };
                }

                @Override
                protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                    results.setOnce(shardId, result);
                }
            };

        action.run();
        latch.await();
        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(1, shardSearchFailures.length);
        assertThat(shardSearchFailures[0].reason(), containsString("IllegalArgumentException: BOOM on shard"));

        ScrollIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            if (context[i].getContextId().getId() == 17) {
                assertNull(results.get(i));
            } else {
                assertNotNull(results.get(i));
                assertEquals(context[i].getContextId(), results.get(i).getContextId());
                assertEquals(context[i].getNode(), results.get(i).node.getId());
            }
        }
    }

    public void testAllShardsFailed() throws InterruptedException {
        ParsedScrollId scrollId = getParsedScrollId(
            new ScrollIdForNode(null, "node1", new SearchContextId("", 1)),
            new ScrollIdForNode(null, "node2", new SearchContextId("", 2)),
            new ScrollIdForNode(null, "node3", new SearchContextId("", 17)),
            new ScrollIdForNode(null, "node1", new SearchContextId("", 0)),
            new ScrollIdForNode(null, "node3", new SearchContextId("", 0)));
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(new DiscoveryNode("node1", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node2", buildNewFakeTransportAddress(), Version.CURRENT))
            .add(new DiscoveryNode("node3", buildNewFakeTransportAddress(), Version.CURRENT)).build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(new Scroll(TimeValue.timeValueMinutes(1)));
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse o) {
                try {
                    fail("got a result");
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    assertTrue(e instanceof SearchPhaseExecutionException);
                    SearchPhaseExecutionException ex = (SearchPhaseExecutionException) e;
                    assertEquals("BOOM on shard", ex.getCause().getMessage());
                    assertEquals("query", ex.getPhaseName());
                    assertEquals("all shards failed", ex.getMessage());
                } finally {
                    latch.countDown();
                }
            }
        };
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action =
            new SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult>(scrollId, logger, discoveryNodes, listener, null,
                request, null) {
                @Override
                protected void executeInitialPhase(Transport.Connection connection, InternalScrollSearchRequest internalRequest,
                                                   SearchActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener)
                {
                    new Thread(() -> searchActionListener.onFailure(new IllegalArgumentException("BOOM on shard"))).start();
                }

                @Override
                protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                    return new SearchAsyncActionTests.MockConnection(node);
                }

                @Override
                protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                   fail("don't move all shards failed");
                   return null;
                }

                @Override
                protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                    results.setOnce(shardId, result);
                }
            };

        action.run();
        latch.await();
        ScrollIdForNode[] context = scrollId.getContext();

        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(context.length, shardSearchFailures.length);
        assertThat(shardSearchFailures[0].reason(), containsString("IllegalArgumentException: BOOM on shard"));

        for (int i = 0; i < results.length(); i++) {
            assertNull(results.get(i));
        }
    }

    private static ParsedScrollId getParsedScrollId(ScrollIdForNode... idsForNodes) {
        List<ScrollIdForNode> scrollIdForNodes = Arrays.asList(idsForNodes);
        Collections.shuffle(scrollIdForNodes, random());
        return new ParsedScrollId("", "test", scrollIdForNodes.toArray(new ScrollIdForNode[0]));
    }

    private ActionListener<SearchResponse> dummyListener() {
        return new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                fail("dummy");
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        };
    }
}
