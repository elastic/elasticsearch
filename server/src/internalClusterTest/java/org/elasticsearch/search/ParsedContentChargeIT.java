/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

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
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

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

    public void testQueuedMultiSearchItemsChargeTheRequest() throws Exception {
        assertAcked(prepareCreate("test").setSettings(indexSettings(6, 0)));
        indexRandom(true, prepareIndex("test").setSource("k", "v"));
        ensureGreen("test");
        WrapperQueryBuilder wrapper = new WrapperQueryBuilder(Strings.toString(new TermQueryBuilder("k", "v")));
        String node = internalCluster().getRandomNodeName();
        long one = charge(node, 1, 1, wrapper);
        assertThat(one, greaterThan(0L));
        int searches = between(2, 6);
        assertThat(charge(node, searches, searches, wrapper), equalTo(searches * one));
        // Searches past the first wait for one to complete, and are sent from its listener.
        assertThat(charge(node, searches, 1, wrapper), equalTo(searches * one));
    }

    public void testFieldCapsIndexFilterIsCheckedOnDataNodes() throws Exception {
        assertAcked(prepareCreate("test").setSettings(indexSettings(6, 0)).setMapping("k", "type=keyword"));
        indexRandom(true, prepareIndex("test").setSource("k", "v"));
        ensureGreen("test");
        BoolQueryBuilder inner = new BoolQueryBuilder();
        // Charged over a chunk while parsed, so the breaker is checked while the source is parsed.
        for (int i = 0; i < 5_000; i++) {
            inner.should(new TermQueryBuilder("k", "value-" + i));
        }
        long tripsBefore = requestBreakerTrips();
        String limit = HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey();
        updateClusterSettings(Settings.builder().put(limit, "512kb"));
        try {
            FieldCapabilitiesResponse response = client().prepareFieldCaps("test")
                .setFields("k")
                .setIndexFilter(new WrapperQueryBuilder(Strings.toString(inner)))
                .get();
            // A trip while the filter is checked counts the shard as a possible match.
            assertTrue(response.get().containsKey("k"));
        } finally {
            updateClusterSettings(Settings.builder().putNull(limit));
        }
        assertThat(requestBreakerTrips(), greaterThan(tripsBefore));
    }

    private long charge(String node, int searches, int concurrent, QueryBuilder query) {
        MultiSearchRequest request = new MultiSearchRequest().maxConcurrentSearchRequests(concurrent);
        for (int i = 0; i < searches; i++) {
            request.add(new SearchRequest("test").source(new SearchSourceBuilder().query(query)));
        }
        ThreadContext threadContext = internalCluster().getInstance(ThreadPool.class, node).getThreadContext();
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofMb(100));
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            // As the REST layer publishes the reservation of the request it handles.
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.putTransient(ParsedContentReservation.TRANSIENT_NAME, reservation);
                MultiSearchResponse response = client(node).multiSearch(request).actionGet();
                try {
                    for (MultiSearchResponse.Item item : response) {
                        assertFalse(String.valueOf(item.getFailure()), item.isFailure());
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
