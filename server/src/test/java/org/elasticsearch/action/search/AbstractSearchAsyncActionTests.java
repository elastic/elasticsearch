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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.Transport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class AbstractSearchAsyncActionTests extends ESTestCase {

    private final List<Tuple<String, String>> resolvedNodes = new ArrayList<>();
    private final Set<Long> releasedContexts = new CopyOnWriteArraySet<>();

    private AbstractSearchAsyncAction<SearchPhaseResult> createAction(SearchRequest request,
                                                                      InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> results,
                                                                      ActionListener<SearchResponse> listener,
                                                                      final boolean controlled,
                                                                      final AtomicLong expected) {
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
            timeProvider = new TransportSearchAction.SearchTimeProvider(
                    0,
                    System.nanoTime(),
                    System::nanoTime);
        }

        BiFunction<String, String, Transport.Connection> nodeIdToConnection = (cluster, node) -> {
            resolvedNodes.add(Tuple.tuple(cluster, node));
            return null;
        };

        return new AbstractSearchAsyncAction<SearchPhaseResult>("test", logger, null, nodeIdToConnection,
                Collections.singletonMap("foo", new AliasFilter(new MatchAllQueryBuilder())), Collections.singletonMap("foo", 2.0f),
                Collections.singletonMap("name", Sets.newHashSet("bar", "baz")), null, request, listener,
                new GroupShardsIterator<>(
                    Collections.singletonList(
                        new SearchShardIterator(null, null, Collections.emptyList(), null)
                    )
                ), timeProvider, 0, null,
                results, request.getMaxConcurrentShardRequests(),
                SearchResponse.Clusters.EMPTY) {
            @Override
            protected SearchPhase getNextPhase(final SearchPhaseResults<SearchPhaseResult> results, final SearchPhaseContext context) {
                return null;
            }

            @Override
            protected void executePhaseOnShard(final SearchShardIterator shardIt, final ShardRouting shard,
                                               final SearchActionListener<SearchPhaseResult> listener) {
            }

            @Override
            long buildTookInMillis() {
                runnable.run();
                return super.buildTookInMillis();
            }

            @Override
            public void sendReleaseSearchContext(long contextId, Transport.Connection connection, OriginalIndices originalIndices) {
                releasedContexts.add(contextId);
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
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(new SearchRequest(),
            new InitialSearchPhase.ArraySearchPhaseResults<>(10), null, controlled, expected);
        final long actual = action.buildTookInMillis();
        if (controlled) {
            // with a controlled clock, we can assert the exact took time
            assertThat(actual, equalTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        } else {
            // with a real clock, the best we can say is that it took as long as we spun for
            assertThat(actual, greaterThanOrEqualTo(TimeUnit.NANOSECONDS.toMillis(expected.get())));
        }
    }

    public void testBuildShardSearchTransportRequest() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean()).preference("_shards:1,3");
        final AtomicLong expected = new AtomicLong();
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            new InitialSearchPhase.ArraySearchPhaseResults<>(10), null, false, expected);
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        SearchShardIterator iterator = new SearchShardIterator(clusterAlias, new ShardId(new Index("name", "foo"), 1),
            Collections.emptyList(), new OriginalIndices(new String[] {"name", "name1"}, IndicesOptions.strictExpand()));
        ShardSearchTransportRequest shardSearchTransportRequest = action.buildShardSearchRequest(iterator);
        assertEquals(IndicesOptions.strictExpand(), shardSearchTransportRequest.indicesOptions());
        assertArrayEquals(new String[] {"name", "name1"}, shardSearchTransportRequest.indices());
        assertEquals(new MatchAllQueryBuilder(), shardSearchTransportRequest.getAliasFilter().getQueryBuilder());
        assertEquals(2.0f, shardSearchTransportRequest.indexBoost(), 0.0f);
        assertArrayEquals(new String[] {"name", "name1"}, shardSearchTransportRequest.indices());
        assertArrayEquals(new String[] {"bar", "baz"}, shardSearchTransportRequest.indexRoutings());
        assertEquals("_shards:1,3", shardSearchTransportRequest.preference());
        assertEquals(clusterAlias, shardSearchTransportRequest.getClusterAlias());
    }

    public void testBuildSearchResponse() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(randomBoolean());
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            new InitialSearchPhase.ArraySearchPhaseResults<>(10), null, false, new AtomicLong());
        String scrollId = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, scrollId);
        assertEquals(scrollId, searchResponse.getScrollId());
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
    }

    public void testBuildSearchResponseAllowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(true);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest,
            new InitialSearchPhase.ArraySearchPhaseResults<>(10), null, false, new AtomicLong());
        action.onShardFailure(0, new SearchShardTarget("node", new ShardId("index", "index-uuid", 0), null, OriginalIndices.NONE),
            new IllegalArgumentException());
        String scrollId = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchResponse searchResponse = action.buildSearchResponse(internalSearchResponse, scrollId);
        assertEquals(scrollId, searchResponse.getScrollId());
        assertSame(searchResponse.getAggregations(), internalSearchResponse.aggregations());
        assertSame(searchResponse.getSuggest(), internalSearchResponse.suggest());
        assertSame(searchResponse.getProfileResults(), internalSearchResponse.profile());
        assertSame(searchResponse.getHits(), internalSearchResponse.hits());
    }

    public void testBuildSearchResponseDisallowPartialFailures() {
        SearchRequest searchRequest = new SearchRequest().allowPartialSearchResults(false);
        AtomicReference<Exception> exception = new AtomicReference<>();
        ActionListener<SearchResponse> listener = ActionListener.wrap(response -> fail("onResponse should not be called"), exception::set);
        Set<Long> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        int numFailures = randomIntBetween(1, 5);
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, numFailures);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        for (int i = 0; i < numFailures; i++) {
            ShardId failureShardId = new ShardId("index", "index-uuid", i);
            String failureClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String failureNodeId = randomAlphaOfLengthBetween(5, 10);
            action.onShardFailure(i, new SearchShardTarget(failureNodeId, failureShardId, failureClusterAlias, OriginalIndices.NONE),
                new IllegalArgumentException());
        }
        action.buildSearchResponse(InternalSearchResponse.empty(), randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10));
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException)exception.get();
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
        Set<Long> requestIds = new HashSet<>();
        List<Tuple<String, String>> nodeLookups = new ArrayList<>();
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> phaseResults = phaseResults(requestIds, nodeLookups, 0);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        action.onPhaseFailure(new SearchPhase("test") {
            @Override
            public void run() {

            }
        }, "message", null);
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException)exception.get();
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
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> phaseResults =
            new InitialSearchPhase.ArraySearchPhaseResults<>(numShards);
        AbstractSearchAsyncAction<SearchPhaseResult> action = createAction(searchRequest, phaseResults, listener, false, new AtomicLong());
        // skip one to avoid the "all shards failed" failure.
        SearchShardIterator skipIterator = new SearchShardIterator(null, null, Collections.emptyList(), null);
        skipIterator.resetAndSkip();
        action.skipShard(skipIterator);
        // expect at least 2 shards, so onPhaseDone should report failure.
        action.onPhaseDone();
        assertThat(exception.get(), instanceOf(SearchPhaseExecutionException.class));
        SearchPhaseExecutionException searchPhaseExecutionException = (SearchPhaseExecutionException)exception.get();
        assertEquals("Partial shards failure (" + (numShards - 1) + " shards unavailable)",
            searchPhaseExecutionException.getMessage());
        assertEquals("test", searchPhaseExecutionException.getPhaseName());
        assertEquals(0, searchPhaseExecutionException.shardFailures().length);
        assertEquals(0, searchPhaseExecutionException.getSuppressed().length);
    }

    private static InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> phaseResults(Set<Long> requestIds,
                                                                                              List<Tuple<String, String>> nodeLookups,
                                                                                              int numFailures) {
        int numResults = randomIntBetween(1, 10);
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> phaseResults =
            new InitialSearchPhase.ArraySearchPhaseResults<>(numResults + numFailures);

        for (int i = 0; i < numResults; i++) {
            long requestId = randomLong();
            requestIds.add(requestId);
            SearchPhaseResult phaseResult = new PhaseResult(requestId);
            String resultClusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
            String resultNodeId = randomAlphaOfLengthBetween(5, 10);
            ShardId resultShardId = new ShardId("index", "index-uuid", i);
            nodeLookups.add(Tuple.tuple(resultClusterAlias, resultNodeId));
            phaseResult.setSearchShardTarget(new SearchShardTarget(resultNodeId, resultShardId, resultClusterAlias, OriginalIndices.NONE));
            phaseResult.setShardIndex(i);
            phaseResults.consumeResult(phaseResult);
        }
        return phaseResults;
    }

    private static final class PhaseResult extends SearchPhaseResult {
        PhaseResult(long requestId) {
            this.requestId = requestId;
        }
    }
}
