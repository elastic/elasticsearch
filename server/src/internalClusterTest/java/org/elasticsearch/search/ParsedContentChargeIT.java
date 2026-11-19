/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * The query inside a {@code wrapper} query is parsed where the query is rewritten, which is not always where the REST request was parsed.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3)
public class ParsedContentChargeIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        // The test cluster gives some nodes a noop request breaker, which never trips.
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    /**
     * Every item of an {@code _msearch} parses its wrapper query again, so the parses are checked against the request's breaker but not
     * held by the request, which would grow its charge with the items.
     */
    public void testMultiSearchItemsAreCheckedAgainstTheRequestButNotHeldByIt() throws Exception {
        assertAcked(prepareCreate("test").setSettings(indexSettings(6, 0)));
        indexRandom(true, prepareIndex("test").setSource("k", "v"));
        ensureGreen("test");
        String node = internalCluster().getRandomNodeName();
        int searches = between(2, 6);
        WrapperQueryBuilder small = new WrapperQueryBuilder(Strings.toString(new TermQueryBuilder("k", "v")));
        WrapperQueryBuilder large = new WrapperQueryBuilder(Strings.toString(overAChunk()));
        // With one at a time, searches past the first wait for one to complete, and are sent from its listener.
        for (int concurrent : new int[] { 1, searches }) {
            LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(100));
            long held = multiSearch(
                node,
                breaker,
                searches,
                concurrent,
                small,
                item -> assertFalse(String.valueOf(item.getFailure()), item.isFailure())
            );
            assertThat(held, equalTo(0L));
            assertThat(breaker.getUsed(), equalTo(0L));

            // Only the request's reservation reaches this breaker, so every item tripping it shows that every item checks the request.
            LimitedBreaker tight = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofKb(512));
            multiSearch(node, tight, searches, concurrent, large, item -> {
                assertTrue(item.isFailure());
                assertThat(ExceptionsHelper.status(item.getFailure()), equalTo(RestStatus.TOO_MANY_REQUESTS));
            });
            assertThat(tight.getTrippedCount(), equalTo((long) searches));
            assertThat(tight.getUsed(), equalTo(0L));
        }
    }

    /**
     * Field caps checks its index filter on every shard, and on the node that handles the REST request the shard requests keep its thread
     * context. Their parses are not held by the request either, as ES|QL resolves its indices with the request's {@code filter} this way.
     */
    public void testFieldCapsIndexFilterOnTheRestRequestsNodeIsNotHeldByIt() throws Exception {
        String node = internalCluster().getRandomDataNodeName();
        // Every shard on the node that handles the request, so each one parses the filter next to it.
        assertAcked(
            prepareCreate("test").setSettings(indexSettings(between(2, 6), 0).put("index.routing.allocation.require._name", node))
                .setMapping("k", "type=keyword")
        );
        indexRandom(true, prepareIndex("test").setSource("k", "v"));
        ensureGreen("test");
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(100));
        assertThat(fieldCapsHeld(node, breaker, new WrapperQueryBuilder(Strings.toString(new TermQueryBuilder("k", "v")))), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));

        // Only the request's reservation reaches this breaker, so a trip shows that the shards check the request.
        LimitedBreaker tight = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofKb(512));
        assertThat(fieldCapsHeld(node, tight, new WrapperQueryBuilder(Strings.toString(overAChunk()))), equalTo(0L));
        assertThat(tight.getTrippedCount(), greaterThan(0L));
        assertThat(tight.getUsed(), equalTo(0L));
    }

    /**
     * Runs field caps on {@code node} as if for a REST request whose reservation charges {@code breaker}, and returns what the reservation
     * holds once the response is in.
     */
    private long fieldCapsHeld(String node, CircuitBreaker breaker, QueryBuilder indexFilter) {
        ThreadContext threadContext = internalCluster().getInstance(ThreadPool.class, node).getThreadContext();
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putTransient(ParsedContentReservation.TRANSIENT_NAME, reservation);
                FieldCapabilitiesResponse response = client(node).prepareFieldCaps("test").setFields("k").setIndexFilter(indexFilter).get();
                // A trip while the filter is checked counts the shard as a possible match.
                assertTrue(response.get().containsKey("k"));
            }
            return reservation.reservedBytes();
        }
    }

    /** A query whose parse charges over a chunk, so the breaker is checked while it is parsed. */
    private static BoolQueryBuilder overAChunk() {
        BoolQueryBuilder query = new BoolQueryBuilder();
        for (int i = 0; i < 5_000; i++) {
            query.should(new TermQueryBuilder("k", "value-" + i));
        }
        return query;
    }

    public void testFieldCapsIndexFilterIsCheckedOnDataNodes() throws Exception {
        assertAcked(prepareCreate("test").setSettings(indexSettings(6, 0)).setMapping("k", "type=keyword"));
        indexRandom(true, prepareIndex("test").setSource("k", "v"));
        ensureGreen("test");
        long tripsBefore = requestBreakerTrips();
        String limit = HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey();
        updateClusterSettings(Settings.builder().put(limit, "512kb"));
        try {
            FieldCapabilitiesResponse response = client().prepareFieldCaps("test")
                .setFields("k")
                .setIndexFilter(new WrapperQueryBuilder(Strings.toString(overAChunk())))
                .get();
            // A trip while the filter is checked counts the shard as a possible match.
            assertTrue(response.get().containsKey("k"));
        } finally {
            updateClusterSettings(Settings.builder().putNull(limit));
        }
        assertThat(requestBreakerTrips(), greaterThan(tripsBefore));
    }

    /**
     * Runs an {@code _msearch} on {@code node} as if for a REST request whose reservation charges {@code breaker}, and returns what the
     * reservation holds once the response is in.
     */
    private long multiSearch(
        String node,
        CircuitBreaker breaker,
        int searches,
        int concurrent,
        QueryBuilder query,
        Consumer<MultiSearchResponse.Item> checkItem
    ) {
        MultiSearchRequest request = new MultiSearchRequest().maxConcurrentSearchRequests(concurrent);
        for (int i = 0; i < searches; i++) {
            request.add(new SearchRequest("test").source(new SearchSourceBuilder().query(query)));
        }
        ThreadContext threadContext = internalCluster().getInstance(ThreadPool.class, node).getThreadContext();
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            // As the REST layer publishes the reservation of the request it handles.
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putTransient(ParsedContentReservation.TRANSIENT_NAME, reservation);
                MultiSearchResponse response = client(node).multiSearch(request).actionGet();
                try {
                    for (MultiSearchResponse.Item item : response) {
                        checkItem.accept(item);
                    }
                } finally {
                    response.decRef();
                }
            }
            return reservation.reservedBytes();
        }
    }

    private long requestBreakerTrips() {
        return clusterAdmin().prepareNodesStats()
            .setBreaker(true)
            .get()
            .getNodes()
            .stream()
            .mapToLong(node -> node.getBreaker().getStats(CircuitBreaker.REQUEST).getTrippedCount())
            .sum();
    }
}
