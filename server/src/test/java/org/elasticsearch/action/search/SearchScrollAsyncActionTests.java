/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.Transport;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
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
        List<ShardSearchFailure> shardSearchFailures = action.buildShardFailures();
        assertEquals(0, shardSearchFailures.size());
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
        List<ShardSearchFailure> shardSearchFailures = action.buildShardFailures();
        assertEquals(0, shardSearchFailures.size());
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
        List<ShardSearchFailure> shardSearchFailures = action.buildShardFailures();
        assertEquals(1, shardSearchFailures.size());
        assertThat(
            shardSearchFailures.getFirst().reason(),
            startsWith("org.elasticsearch.action.search.SearchContextMissingNodesException")
        );

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
        List<ShardSearchFailure> shardSearchFailures = action.buildShardFailures();
        assertEquals(1, shardSearchFailures.size());
        assertThat(shardSearchFailures.getFirst().reason(), containsString("IllegalArgumentException: BOOM on shard"));

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

        List<ShardSearchFailure> shardSearchFailures = action.buildShardFailures();
        assertEquals(context.length, shardSearchFailures.size());
        assertThat(shardSearchFailures.getFirst().reason(), containsString("IllegalArgumentException: BOOM on shard"));

        for (int i = 0; i < results.length(); i++) {
            assertNull(results.get(i));
        }
    }

    /**
     * A node that leaves between the query and fetch phases of a scroll makes {@code getConnection} throw while the fetch phase is
     * still sending requests. Fetch results that already arrived for other shards must still be released.
     */
    public void testQueryThenFetchReleasesFetchResultsWhenConnectionFailsDuringFetchPhase() throws Exception {
        final DiscoveryNode node1 = DiscoveryNodeUtils.create("node1");
        final DiscoveryNode node2 = DiscoveryNodeUtils.create("node2");
        // The fetch phase visits shards in order, so node1's fetch completes before node2's connection fails
        final ParsedScrollId scrollId = new ParsedScrollId(
            "test",
            new SearchContextIdForNode[] {
                new SearchContextIdForNode(null, "node1", new ShardSearchContextId(UUIDs.randomBase64UUID(), 1)),
                new SearchContextIdForNode(null, "node2", new ShardSearchContextId(UUIDs.randomBase64UUID(), 2)) }
        );
        final AtomicBoolean fetchStarted = new AtomicBoolean();
        final List<FetchSearchResult> fetchResults = new CopyOnWriteArrayList<>();
        final SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {
            @Override
            public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                if (fetchStarted.get() && node.equals(node2)) {
                    throw new NodeNotConnectedException(node, "simulated disconnect");
                }
                return new SearchAsyncActionTests.MockConnection(node);
            }

            @Override
            public void sendExecuteScrollQuery(
                Transport.Connection connection,
                InternalScrollSearchRequest request,
                SearchTask task,
                ActionListener<ScrollQuerySearchResult> listener
            ) {
                final DiscoveryNode node = connection.getNode();
                final SearchShardTarget target = new SearchShardTarget(
                    node.getId(),
                    new ShardId("test", "_na_", node.equals(node1) ? 0 : 1),
                    null
                );
                final QuerySearchResult queryResult = new QuerySearchResult(request.contextId(), target, null);
                try {
                    queryResult.topDocs(
                        new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(42, 1.0F) }),
                            1.0F
                        ),
                        new DocValueFormat[0]
                    );
                    queryResult.size(10);
                    final ScrollQuerySearchResult result = new ScrollQuerySearchResult(queryResult, target);
                    try {
                        listener.onResponse(result);
                    } finally {
                        result.decRef();
                    }
                } finally {
                    queryResult.decRef();
                }
            }

            @Override
            public void sendExecuteFetchScroll(
                Transport.Connection connection,
                ShardFetchRequest request,
                SearchTask task,
                ActionListener<FetchSearchResult> listener
            ) {
                fetchStarted.set(true);
                final FetchSearchResult fetchResult = new FetchSearchResult(
                    request.contextId(),
                    new SearchShardTarget(connection.getNode().getId(), new ShardId("test", "_na_", 0), null)
                );
                fetchResult.shardResult(
                    new SearchHits(new SearchHit[] { new SearchHit(42) }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F),
                    null
                );
                fetchResults.add(fetchResult);
                try {
                    listener.onResponse(fetchResult);
                } finally {
                    // as the transport layer does once the response handler returns
                    fetchResult.decRef();
                }
            }
        };

        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool)) {
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(clusterService.state())
                    .nodes(DiscoveryNodes.builder(clusterService.state().nodes()).add(node1).add(node2))
            );
            final SearchScrollRequest request = new SearchScrollRequest();
            request.scroll(TimeValue.timeValueMinutes(1));
            final PlainActionFuture<int[]> future = new PlainActionFuture<>();
            new SearchScrollQueryThenFetchAsyncAction(
                logger,
                clusterService,
                searchTransportService,
                request,
                new SearchTask(1, "transport", TransportSearchScrollAction.TYPE.name(), () -> "", TaskId.EMPTY_TASK_ID, Map.of()),
                scrollId,
                future.map(response -> new int[] { response.getSuccessfulShards(), response.getFailedShards() })
            ).run();

            assertThat(fetchResults, hasSize(1));
            assertFalse("fetch result of node1 was not released", fetchResults.get(0).hasReferences());
            final int[] shards = future.actionGet(SAFE_AWAIT_TIMEOUT);
            assertEquals(1, shards[0]);
            assertEquals(1, shards[1]);
        } finally {
            terminate(threadPool);
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
