/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class SearchScrollAsyncActionTests extends ESTestCase {

    public void testSendRequestsToNodes() throws InterruptedException {

        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId(UUIDs.randomBase64UUID(), 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId(UUIDs.randomBase64UUID(), 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId(UUIDs.randomBase64UUID(), 17)),
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId(UUIDs.randomBase64UUID(), 0)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId(UUIDs.randomBase64UUID(), 0))
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node1"))
            .add(DiscoveryNodeUtils.create("node2"))
            .add(DiscoveryNodeUtils.create("node3"))
            .build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(TimeValue.timeValueMinutes(1));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<>(
            scrollId,
            logger,
            discoveryNodes,
            dummyListener(),
            request,
            null
        ) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                ActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                new Thread(() -> {
                    SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult = new SearchAsyncActionTests.TestSearchPhaseResult(
                        internalRequest.contextId(),
                        connection.getNode()
                    );
                    testSearchPhaseResult.setSearchShardTarget(
                        new SearchShardTarget(connection.getNode().getId(), new ShardId("test", "_na_", 1), null)
                    );
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
                    protected void run() {
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
        SearchContextIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            assertNotNull(results.get(i));
            assertEquals(context[i].getSearchContextId(), results.get(i).getContextId());
            assertEquals(context[i].getNode(), results.get(i).node.getId());
        }
    }

    public void testFailNextPhase() throws InterruptedException {

        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId("a", 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("b", 17)),
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("c", 0)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("d", 0))
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node1"))
            .add(DiscoveryNodeUtils.create("node2"))
            .add(DiscoveryNodeUtils.create("node3"))
            .build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(TimeValue.timeValueMinutes(1));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        ActionListener<SearchResponse> listener = new ActionListener<>() {
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
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<>(
            scrollId,
            logger,
            discoveryNodes,
            listener,
            request,
            null
        ) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                ActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                new Thread(() -> {
                    SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult = new SearchAsyncActionTests.TestSearchPhaseResult(
                        internalRequest.contextId(),
                        connection.getNode()
                    );
                    testSearchPhaseResult.setSearchShardTarget(
                        new SearchShardTarget(connection.getNode().getId(), new ShardId("test", "_na_", 1), null)
                    );
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
                    protected void run() {
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
        SearchContextIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            assertNotNull(results.get(i));
            assertEquals(context[i].getSearchContextId(), results.get(i).getContextId());
            assertEquals(context[i].getNode(), results.get(i).node.getId());
        }
    }

    public void testNodeNotAvailable() throws InterruptedException {
        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId("", 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 17)),
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 0)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 0))
        );
        // node2 is not available
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node1"))
            .add(DiscoveryNodeUtils.create("node3"))
            .build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(TimeValue.timeValueMinutes(1));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<>(
            scrollId,
            logger,
            discoveryNodes,
            dummyListener(),
            request,
            null
        ) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                ActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                try {
                    assertNotEquals("node2 is not available", "node2", connection.getNode().getId());
                } catch (NullPointerException e) {
                    logger.warn(e);
                }
                new Thread(() -> {
                    SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult = new SearchAsyncActionTests.TestSearchPhaseResult(
                        internalRequest.contextId(),
                        connection.getNode()
                    );
                    testSearchPhaseResult.setSearchShardTarget(
                        new SearchShardTarget(connection.getNode().getId(), new ShardId("test", "_na_", 1), null)
                    );
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
                    protected void run() {
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
        assertThat(shardSearchFailures[0].reason(), startsWith("org.elasticsearch.action.search.SearchContextMissingNodesException"));

        SearchContextIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            if (context[i].getNode().equals("node2")) {
                assertNull(results.get(i));
            } else {
                assertNotNull(results.get(i));
                assertEquals(context[i].getSearchContextId(), results.get(i).getContextId());
                assertEquals(context[i].getNode(), results.get(i).node.getId());
            }
        }
    }

    public void testAllNodesUnavailableReturns404() throws InterruptedException {
        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId("", 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 17))
        );

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node4"))
            .add(DiscoveryNodeUtils.create("node5"))
            .build();

        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(TimeValue.timeValueMinutes(1));

        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<SearchResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse o) {
                try {
                    fail("Expected failure but got success");
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    assertTrue(
                        "Expected SearchContextMissingNodesException but got: " + e.getClass().getName(),
                        e instanceof SearchContextMissingNodesException
                    );
                    SearchContextMissingNodesException ex = (SearchContextMissingNodesException) e;
                    assertEquals(RestStatus.NOT_FOUND, ex.status());
                    assertEquals(SearchContextMissingNodesException.ContextType.SCROLL, ex.getContextType());
                    assertEquals(Set.of("node3", "node2", "node1"), ex.getMissingNodeIds());
                } finally {
                    latch.countDown();
                }
            }
        };

        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<>(
            scrollId,
            logger,
            discoveryNodes,
            listener,
            request,
            null
        ) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                ActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                new Thread(() -> {
                    SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult = new SearchAsyncActionTests.TestSearchPhaseResult(
                        internalRequest.contextId(),
                        connection.getNode()
                    );
                    testSearchPhaseResult.setSearchShardTarget(
                        new SearchShardTarget(connection.getNode().getId(), new ShardId("test", "_na_", 1), null)
                    );
                    searchActionListener.onResponse(testSearchPhaseResult);
                }).start();
            }

            @Override
            protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }

            @Override
            protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                fail("Should not move to next phase when all shards fail");
                return null;
            }

            @Override
            protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                fail("Should not have results when all nodes are unavailable");
            }
        };

        action.run();
        latch.await();
    }

    public void testSearchContextMissingNodesExceptionMetadata() throws InterruptedException {
        String expectedScrollId = "test-scroll-id-12345";
        ParsedScrollId scrollId = new ParsedScrollId(
            expectedScrollId,
            new SearchContextIdForNode[] { new SearchContextIdForNode(null, "missing-node", new ShardSearchContextId("", 1)) }
        );

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("other-node")).build();

        SearchScrollRequest request = new SearchScrollRequest(expectedScrollId);
        request.scroll(TimeValue.timeValueMinutes(1));
        CountDownLatch latch = new CountDownLatch(1);

        ActionListener<SearchResponse> listener = new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse o) {
                try {
                    fail("Expected failure");
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    assertTrue(
                        "Expected SearchContextMissingNodesException but got: " + e.getClass().getName(),
                        e instanceof SearchContextMissingNodesException
                    );
                    SearchContextMissingNodesException ex = (SearchContextMissingNodesException) e;
                    assertEquals(RestStatus.NOT_FOUND, ex.status());
                    assertEquals(SearchContextMissingNodesException.ContextType.SCROLL, ex.getContextType());
                    assertTrue(ex.getMissingNodeIds().contains("missing-node"));
                } finally {
                    latch.countDown();
                }
            }
        };

        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<>(
            scrollId,
            logger,
            discoveryNodes,
            listener,
            request,
            null
        ) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                ActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                fail("Should not execute when node is unavailable");
            }

            @Override
            protected Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }

            @Override
            protected SearchPhase moveToNextPhase(BiFunction<String, String, DiscoveryNode> clusterNodeLookup) {
                fail("Should not move to next phase");
                return null;
            }

            @Override
            protected void onFirstPhaseResult(int shardId, SearchAsyncActionTests.TestSearchPhaseResult result) {
                fail("Should not have results");
            }
        };

        action.run();
        latch.await();
    }

    public void testShardFailures() throws InterruptedException {
        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId("", 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 17)),
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 0)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 0))
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node1"))
            .add(DiscoveryNodeUtils.create("node2"))
            .add(DiscoveryNodeUtils.create("node3"))
            .build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(TimeValue.timeValueMinutes(1));
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger movedCounter = new AtomicInteger(0);
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<>(
            scrollId,
            logger,
            discoveryNodes,
            dummyListener(),
            request,
            null
        ) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                ActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
                new Thread(() -> {
                    if (internalRequest.contextId().getId() == 17) {
                        searchActionListener.onFailure(new IllegalArgumentException("BOOM on shard"));
                    } else {
                        SearchAsyncActionTests.TestSearchPhaseResult testSearchPhaseResult =
                            new SearchAsyncActionTests.TestSearchPhaseResult(internalRequest.contextId(), connection.getNode());
                        testSearchPhaseResult.setSearchShardTarget(
                            new SearchShardTarget(connection.getNode().getId(), new ShardId("test", "_na_", 1), null)
                        );
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
                    protected void run() {
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

        SearchContextIdForNode[] context = scrollId.getContext();
        for (int i = 0; i < results.length(); i++) {
            if (context[i].getSearchContextId().getId() == 17) {
                assertNull(results.get(i));
            } else {
                assertNotNull(results.get(i));
                assertEquals(context[i].getSearchContextId(), results.get(i).getContextId());
                assertEquals(context[i].getNode(), results.get(i).node.getId());
            }
        }
    }

    public void testAllShardsFailed() throws InterruptedException {
        ParsedScrollId scrollId = getParsedScrollId(
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 1)),
            new SearchContextIdForNode(null, "node2", new ShardSearchContextId("", 2)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 17)),
            new SearchContextIdForNode(null, "node1", new ShardSearchContextId("", 0)),
            new SearchContextIdForNode(null, "node3", new ShardSearchContextId("", 0))
        );
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node1"))
            .add(DiscoveryNodeUtils.create("node2"))
            .add(DiscoveryNodeUtils.create("node3"))
            .build();

        AtomicArray<SearchAsyncActionTests.TestSearchPhaseResult> results = new AtomicArray<>(scrollId.getContext().length);
        SearchScrollRequest request = new SearchScrollRequest();
        request.scroll(TimeValue.timeValueMinutes(1));
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<SearchResponse> listener = new ActionListener<>() {
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
        SearchScrollAsyncAction<SearchAsyncActionTests.TestSearchPhaseResult> action = new SearchScrollAsyncAction<>(
            scrollId,
            logger,
            discoveryNodes,
            listener,
            request,
            null
        ) {
            @Override
            protected void executeInitialPhase(
                Transport.Connection connection,
                InternalScrollSearchRequest internalRequest,
                ActionListener<SearchAsyncActionTests.TestSearchPhaseResult> searchActionListener
            ) {
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
        SearchContextIdForNode[] context = scrollId.getContext();

        ShardSearchFailure[] shardSearchFailures = action.buildShardFailures();
        assertEquals(context.length, shardSearchFailures.length);
        assertThat(shardSearchFailures[0].reason(), containsString("IllegalArgumentException: BOOM on shard"));

        for (int i = 0; i < results.length(); i++) {
            assertNull(results.get(i));
        }
    }

    private static ParsedScrollId getParsedScrollId(SearchContextIdForNode... idsForNodes) {
        List<SearchContextIdForNode> searchContextIdForNodes = Arrays.asList(idsForNodes);
        Collections.shuffle(searchContextIdForNodes, random());
        return new ParsedScrollId("test", searchContextIdForNodes.toArray(new SearchContextIdForNode[0]));
    }

    private ActionListener<SearchResponse> dummyListener() {
        return ActionTestUtils.assertNoFailureListener(response -> fail("dummy"));
    }
}
