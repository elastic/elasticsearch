/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.Transport;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class AbstractSearchAsyncActionTests extends ESTestCase {

    private final List<Tuple<String, String>> resolvedNodes = new ArrayList<>();
    private final Set<ShardSearchContextId> releasedContexts = new CopyOnWriteArraySet<>();

    private AbstractSearchAsyncAction<SearchPhaseResult> createAction(
        SearchRequest request,
        ArraySearchPhaseResults<SearchPhaseResult> results,
        ActionListener<SearchResponse> listener,
        final boolean controlled,
        final AtomicLong expected
    ) {
        final Runnable runnable;
        final TransportSearchAction.SearchTimeProvider timeProvider;
        if (controlled) {
            runnable = () -> expected.set(randomNonNegativeLong());
            timeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, expected::get);
        } else {
            runnable = () -> {
                long elapsed = spinForAtLeastNMilliseconds(randomIntBetween(1, 10));
                expected.set(elapsed);
            };
            timeProvider = new TransportSearchAction.SearchTimeProvider(0, System.nanoTime(), System::nanoTime);
        }

        BiFunction<String, String, Transport.Connection> nodeIdToConnection = (cluster, node) -> {
            resolvedNodes.add(Tuple.tuple(cluster, node));
            return null;
        };
        OriginalIndices originalIndices = new OriginalIndices(request.indices(), request.indicesOptions());
        return new AbstractSearchAsyncAction<SearchPhaseResult>(
            "test",
            logger,
            null,
            null,
            nodeIdToConnection,
            Collections.singletonMap("foo", AliasFilter.of(new MatchAllQueryBuilder())),
            Collections.singletonMap("foo", 2.0f),
            null,
            request,
            listener,
            Collections.singletonList(
                new SearchShardIterator(null, new ShardId("index", "_na", 0), Collections.emptyList(), null, SplitShardCountSummary.UNSET)
            ),
            timeProvider,
            ClusterState.EMPTY_STATE,
            null,
            results,
            request.getMaxConcurrentShardRequests(),
            SearchResponse.Clusters.EMPTY,
            Mockito.mock(SearchResponseMetrics.class),
            Map.of()
        ) {
            @Override
            protected SearchPhase getNextPhase() {
                return null;
            }

            @Override
            protected void executePhaseOnShard(
                final SearchShardIterator shardIt,
                final Transport.Connection shard,
                final SearchActionListener<SearchPhaseResult> listener
            ) {}

            @Override
            long buildTookInMillis() {
                runnable.run();
                return super.buildTookInMillis();
            }

            @Override
            public void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection) {
                releasedContexts.add(contextId);
            }

            @Override
            public OriginalIndices getOriginalIndices(int shardIndex) {
                return originalIndices;
            }
        };
    }

    public void testTookWithControlledClock() {
        runTestTook(true);
    }

    public void testTookWithRealClock() {
        runTestTook(false);
    }

    private void runTestTook(final boolean controlled) {
        final AtomicLong expected = new AtomicLong();
        try (var result = new ArraySearchPhaseResults<>(10)) {
            AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(new SearchRequest(), result, null, controlled, expected);
            final long actual = action.buildTookInMillis();
            if (controlled) {
                // with a controlled clock, we can assert the exact took time
                assertThat(actual, equalTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
            } else {
                // with a real clock, the best we can say is that it took as long as we spun for
                assertThat(actual, greaterThanOrEqualTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
            }
        }
    }

    public void testBuildShardSearchTransportRequest() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        final AtomicLong expected = new AtomicLong();
        try (var result = new ArraySearchPhaseResults<>(10)) {
            AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, result, null, false, expected);
            String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            SearchShardIterator iterator = new SearchShardIterator(
                clusterAlias,
                new ShardId(new Index("name", "foo"), 1),
                Collections.emptyList(),
                new OriginalIndices(new String[] { "name", "name1" }, IndicesOptions.strictExpand()),
                SplitShardCountSummary.UNSET
            );
            ShardSearchRequest shardSearchTransportRequest = action.buildShardSearchRequest(iterator, 10);
            assertEquals(IndicesOptions.strictExpand(), shardSearchTransportRequest.indicesOptions());
            assertArrayEquals(new String[] { "name", "name1" }, shardSearchTransportRequest.indices());
            assertEquals(new MatchAllQueryBuilder(), shardSearchTransportRequest.getAliasFilter().getQueryBuilder());
            assertEquals(2.0f, shardSearchTransportRequest.indexBoost(), 0.0f);
            assertArrayEquals(new String[] { "name", "name1" }, shardSearchTransportRequest.indices());
            assertEquals(clusterAlias, shardSearchTransportRequest.getClusterAlias());
        }
    }

    public void testSendSearchResponseDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<ShardSearchContextId> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        int numFailures = randomIntBetween(1, 5);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, numFailures);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        for (int i = 0; i < numFailures; i++) {
            ShardId failureShardId = new ShardId("index", "index-uuid", i);
            String failureClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String failureNodeId = randomAlphaOfLengthBetween(5, 10);
            action.onShardFailure(
                i,
                new SearchShardTarget(failureNodeId, failureShardId, failureClusterAlias),
                new IllegalArgumentException()
            );
        }
        action.sendSearchResponse(SearchResponseSections.EMPTY_WITH_TOTAL_HITS, phaseResults.results);
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException) exception.get();
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
        assertEquals(numFailures, searchPhaseExecutionException.shardFailures().length);
        for (ShardSearchFailure shardSearchFailure : searchPhaseExecutionException.shardFailures()) {
            assertThat(shardSearchFailure.getCause(), instanceOf(IllegalArgumentException.class));
        }
        assertEquals(nodeLookups, resolvedNodes);
        assertEquals(requestIds, releasedContexts);
    }

    public void testOnPhaseFailure() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<ShardSearchContextId> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, 0);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        action.onPhaseFailure("test", "message", null);
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException) exception.get();
        assertEquals("message", searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
        assertEquals(nodeLookups, resolvedNodes);
        assertEquals(requestIds, releasedContexts);
    }

    public void testShardNotAvailableWithDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        int numShards = randomIntBetween(2, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(numShards);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        // skip one to avoid the "all shards failed" failure.
        action.onShardResult(new SearchPhaseResult() {
            @Override
            public int getShardIndex() {
                return 0;
            }

            @Override
            public SearchShardTarget getSearchShardTarget() {
                return new SearchShardTarget(null, null, null);
            }
        });
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException) exception.get();
        assertEquals("Partial shards failure (" + (numShards - 1) + " shards unavailable)", searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
    }

    public void testMaybeReEncode() {
        String[] nodeIds = new String[] { "node1", "node2", "node3" };
        AtomicInteger idGenerator = new AtomicInteger(0);
        Map<ShardId, SearchContextIdForNode> originalShardIdMap = randomMap(
            10,
            20,
            () -> new Tuple<>(
                new ShardId("index", "index-uuid", idGenerator.getAndIncrement()),
                new SearchContextIdForNode(
                    null,
                    randomFrom(nodeIds),
                    new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong(), randomUUID())
                )
            )
        );
        PointInTimeBuilder pointInTimeBuilder = new PointInTimeBuilder(
            SearchContextId.encode(originalShardIdMap, Collections.emptyMap(), TransportVersion.current(), ShardSearchFailure.EMPTY_ARRAY)
        );

        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        Arrays.stream(nodeIds).forEach(id -> nodesBuilder.add(DiscoveryNodeUtils.create(id)));
        DiscoveryNodes nodes = nodesBuilder.build();
        final Set<ShardSearchContextId> freedContexts = new CopyOnWriteArraySet<>();
        SearchTransportService searchTransportService = new SearchTransportService(null, null, null) {

            @Override
            public void sendFreeContext(
                Transport.Connection connection,
                ShardSearchContextId contextId,
                ActionListener<SearchFreeContextResponse> listener
            ) {
                freedContexts.add(contextId);
            }

            @Override
            public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
                return new SearchAsyncActionTests.MockConnection(node);
            }
        };

        {
            // case 1, result shard ids are identical to original PIT ids
            ArrayList<SearchPhaseResult> results = new ArrayList<>();
            for (ShardId shardId : originalShardIdMap.keySet()) {
                SearchContextIdForNode searchContextIdForNode = originalShardIdMap.get(shardId);
                results.add(
                    new PhaseResult(searchContextIdForNode.getSearchContextId()).withShardTarget(
                        new SearchShardTarget(searchContextIdForNode.getNode(), shardId, searchContextIdForNode.getClusterAlias())
                    )
                );
            }
            BytesReference reEncodedId = AbstractSearchAsyncAction.maybeReEncodeNodeIds(
                pointInTimeBuilder,
                results,
                new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
                TransportVersionUtils.randomCompatibleVersion(random()),
                searchTransportService,
                nodes,
                logger
            );
            assertEquals(0, freedContexts.size());
            assertSame(reEncodedId, pointInTimeBuilder.getEncodedId());
        }
        freedContexts.clear();
        {
            // case 2, some results are from different nodes but have same context Ids
            ArrayList<SearchPhaseResult> results = new ArrayList<>();
            Set<ShardId> shardsWithSwappedNodes = new HashSet<>();
            for (ShardId shardId : originalShardIdMap.keySet()) {
                SearchContextIdForNode searchContextIdForNode = originalShardIdMap.get(shardId);
                // only swap node for ids there have a non-null node id, i.e. those that didn't fail when opening a PIT
                if (randomBoolean() && searchContextIdForNode.getNode() != null) {
                    // swap to a different node
                    PhaseResult otherNode = new PhaseResult(searchContextIdForNode.getSearchContextId()).withShardTarget(
                        new SearchShardTarget("otherNode", shardId, searchContextIdForNode.getClusterAlias())
                    );
                    results.add(otherNode);
                    shardsWithSwappedNodes.add(shardId);
                } else {
                    results.add(
                        new PhaseResult(searchContextIdForNode.getSearchContextId()).withShardTarget(
                            new SearchShardTarget(searchContextIdForNode.getNode(), shardId, searchContextIdForNode.getClusterAlias())
                        )
                    );
                }
            }
            BytesReference reEncodedId = AbstractSearchAsyncAction.maybeReEncodeNodeIds(
                pointInTimeBuilder,
                results,
                new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
                TransportVersionUtils.randomCompatibleVersion(random()),
                searchTransportService,
                nodes,
                logger
            );
            assertNotSame(reEncodedId, pointInTimeBuilder.getEncodedId());
            SearchContextId reEncodedPit = SearchContextId.decode(
                new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
                reEncodedId
            );
            assertEquals(originalShardIdMap.size(), reEncodedPit.shards().size());
            for (ShardId shardId : originalShardIdMap.keySet()) {
                SearchContextIdForNode original = originalShardIdMap.get(shardId);
                SearchContextIdForNode reEncoded = reEncodedPit.shards().get(shardId);
                assertNotNull(reEncoded);
                if (shardsWithSwappedNodes.contains(shardId)) {
                    assertNotEquals(original.getNode(), reEncoded.getNode());
                    assertTrue(freedContexts.contains(reEncoded.getSearchContextId()));
                } else {
                    assertEquals(original.getNode(), reEncoded.getNode());
                    assertFalse(freedContexts.contains(reEncoded.getSearchContextId()));
                }
                assertEquals(original.getSearchContextId(), reEncoded.getSearchContextId());
            }
        }
        freedContexts.clear();
        {
            // case 3, result shard ids are identical to original PIT id but some are missing. Stay with original PIT id in this case
            ArrayList<SearchPhaseResult> results = new ArrayList<>();
            for (ShardId shardId : originalShardIdMap.keySet()) {
                SearchContextIdForNode searchContextIdForNode = originalShardIdMap.get(shardId);
                if (randomBoolean()) {
                    results.add(
                        new PhaseResult(searchContextIdForNode.getSearchContextId()).withShardTarget(
                            new SearchShardTarget(searchContextIdForNode.getNode(), shardId, searchContextIdForNode.getClusterAlias())
                        )
                    );
                }
            }
            BytesReference reEncodedId = AbstractSearchAsyncAction.maybeReEncodeNodeIds(
                pointInTimeBuilder,
                results,
                new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
                TransportVersionUtils.randomCompatibleVersion(random()),
                searchTransportService,
                nodes,
                logger
            );
            assertSame(reEncodedId, pointInTimeBuilder.getEncodedId());
        }
    }

    private ShardSearchContextId randomSearchShardId() {
        return new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
    }

    private static ArraySearchPhaseResults<SearchPhaseResult> phaseResults(
        Set<ShardSearchContextId> contextIds,
        List<Tuple<String, String>> nodeLookups,
        int numFailures
    ) {
        int numResults = randomIntBetween(1, 10);
        ArraySearchPhaseResults<SearchPhaseResult> phaseResults = new ArraySearchPhaseResults<>(numResults + numFailures);

        for (int i = 0; i < numResults; i++) {
            ShardSearchContextId contextId = new ShardSearchContextId(UUIDs.randomBase64UUID(), randomNonNegativeLong());
            contextIds.add(contextId);
            SearchPhaseResult phaseResult = new PhaseResult(contextId);
            String resultClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String resultNodeId = randomAlphaOfLengthBetween(5, 10);
            ShardId resultShardId = new ShardId("index", "index-uuid", i);
            nodeLookups.add(Tuple.tuple(resultClusterAlias, resultNodeId));
            phaseResult.setSearchShardTarget(new SearchShardTarget(resultNodeId, resultShardId, resultClusterAlias));
            phaseResult.setShardIndex(i);
            phaseResults.consumeResult(phaseResult, () -> {});
        }
        return phaseResults;
    }

    private static final class PhaseResult extends SearchPhaseResult {
        PhaseResult(ShardSearchContextId contextId) {
            this.contextId = contextId;
        }

        PhaseResult withShardTarget(SearchShardTarget shardTarget) {
            PhaseResult phaseResult = new PhaseResult(contextId);
            phaseResult.setSearchShardTarget(shardTarget);
            return phaseResult;
        }
    }
}
