/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ParsedContentReservation;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Parsing a search request must not be able to take a node down. What a request is parsed into can be several times larger than its
 * content, and the coordinating node parses all of it before the shards apply any limit on clauses or terms. So the parsers of a request
 * charge the request breaker for the heap they take as they read.
 *
 * <p>Each of these cases is a single request within {@code http.max_content_length} that does not fit in the heap of the node once
 * parsed, so without the charge the node runs out of memory while parsing it. The shapes differ in where the heap goes:
 * <ul>
 * <li>a {@code bool} query with many clauses, a query builder each;
 * <li>a {@code filters} aggregation with many filters, each of which is parsed as a query of its own;
 * <li>a single {@code terms} query with many values, a {@code BytesRef} each, in one query builder;
 * <li>a multi-search of many searches, each read by a parser of its own and too small to be checked alone, so that they have to be
 * checked together.
 * </ul>
 * {@link #testParsedRequestsHeldInFlightStayCharged} checks that the charge lasts until the response is sent, not just while parsing.
 */
public class RequestParsingHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "request-parsing";
    private static final int DOCS = 100;

    /**
     * About 27 bytes of content each, which retain 96 bytes of heap once parsed in a bool or 168 in a filters aggregation, so this is
     * about 54 MB of content.
     */
    private static final int MANY_CLAUSES = 2_000_000;
    /** About 11 bytes of content each and 64 bytes of heap while parsed, so this is about 44 MB of content. */
    private static final int MANY_TERMS = 4_000_000;
    /**
     * One-character values of the {@code terms} query in each search of a multi-search. They are charged about 960 KB while read, so no
     * search alone reaches the 1 MB chunk at which charges are checked.
     */
    private static final int TERMS_PER_SEARCH = 9_000;
    /**
     * About 36 KB of content each, which retains 480 KB of heap once parsed, so this is about 22 MB of content, which the in-flight
     * requests breaker lets in even when the parent breaker checks the real heap.
     */
    private static final int MANY_SEARCHES = 600;

    /** Pinned so that the number of searches it takes to fill the search threads is known. */
    private static final int SEARCH_THREADS = 4;
    /** Each retains about 8 MB of heap once parsed with {@link #FILTERS_PER_HELD_SEARCH} filters, so together more than the heap. */
    private static final int HELD_SEARCHES = 40;
    private static final int FILTERS_PER_HELD_SEARCH = 50_000;

    /** Parks a search thread on the pause field until it is unblocked. */
    private static final String PAUSED_SEARCH = """
        {
          "size": 0,
          "runtime_mappings": {"p": {"type": "long", "script": {"source": "", "lang": "pause"}}},
          "query": {"bool": {"filter": [{"term": {"p": 1}}]}}
        }
        """;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").module("test-pausable-field")
        .setting("thread_pool.search.size", Integer.toString(SEARCH_THREADS))
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    /** The client keeps at most 10 connections per route by default, which would cap the searches held in flight at 10. */
    @Override
    protected void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        super.configureClient(builder, settings);
        int connections = HELD_SEARCHES + SEARCH_THREADS;
        builder.setHttpClientConfigCallback(http -> http.setMaxConnPerRoute(connections).setMaxConnTotal(connections));
    }

    @Before
    public void indexDocuments() throws IOException {
        indexDocuments(INDEX, 1, """
            {"k": {"type": "keyword"}}""", DOCS, i -> Strings.format("""
            {"k":"%s"}""", term(i)));
    }

    public void testBoolQueryWithManyClausesTripsWhileParsing() throws IOException {
        assertTripsWhileParsing(n -> Strings.format("""
            {"size": 0, "query": {"bool": {"should": [%s]}}}""", repeat(n, RequestParsingHeapPressureIT::termClause)), 100, MANY_CLAUSES);
    }

    public void testFiltersAggregationWithManyFiltersTripsWhileParsing() throws IOException {
        assertTripsWhileParsing(RequestParsingHeapPressureIT::filtersAggregation, 100, MANY_CLAUSES);
    }

    public void testTermsQueryWithManyValuesTripsWhileParsing() throws IOException {
        assertTripsWhileParsing(n -> Strings.format("""
            {"size": 0, "query": {"terms": {"k": [%s]}}}""", repeat(n, i -> "\"" + term(i) + "\"")), 1000, MANY_TERMS);
    }

    public void testMultiSearchOfManySearchesTripsWhileParsing() throws IOException {
        Map<String, Object> small = entityAsMap(client().performRequest(multiSearch(2)));
        for (Object response : (List<?>) small.get("responses")) {
            assertNull("expected every search to succeed", ((Map<?, ?>) response).get("error"));
        }
        assertCircuitBreaks(multiSearch(MANY_SEARCHES), ParsedContentReservation.LABEL);
    }

    /**
     * Searches parked on the pause field take every search thread, so the held searches, each well under the limit, wait in the search
     * queue with their parsed requests and do no other work. Since the parse charge lasts until the response is sent, the breakers reject
     * the later searches, while they are parsed or, once the parent breaker is full, as they arrive. The searches are cancelled before the
     * pause field is unblocked, so none of them builds its aggregation.
     */
    public void testParsedRequestsHeldInFlightStayCharged() throws Exception {
        String filters = filtersAggregation(FILTERS_PER_HELD_SEARCH);
        List<Future<Map<String, Object>>> paused;
        List<Future<Map<String, Object>>> held;
        long heldCharge;
        blockPauseField();
        try {
            paused = submit(SEARCH_THREADS, () -> errorBodyOrNull(search(INDEX, PAUSED_SEARCH)));
            assertBusy(() -> assertThat(parkedCount(), equalTo(SEARCH_THREADS)));
            held = submit(HELD_SEARCHES, () -> errorBodyOrNull(search(INDEX, filters)));
            // Wait until each search is rejected or has its one shard request in the search queue.
            assertBusy(() -> assertThat(completed(held) + queuedSearches(), equalTo(HELD_SEARCHES)), 60, TimeUnit.SECONDS);
            heldCharge = requestBreakerUsedBytes();
        } finally {
            // Cancelled first, so that no held search goes on to build its aggregation, also if waiting for them failed.
            try {
                cancelSearches();
            } finally {
                unblockPauseField();
            }
        }
        int rejected = 0;
        int rejectedWhileParsing = 0;
        for (Future<Map<String, Object>> search : held) {
            Map<String, Object> error = search.get();
            if (error != null && containsCircuitBreakingException(error)) {
                rejected++;
                if (containsCircuitBreakingException(error, "data for [" + ParsedContentReservation.LABEL + "]")) {
                    rejectedWhileParsing++;
                }
            } else {
                // Each one waited in the search queue until it was cancelled, so none of them built its aggregation.
                assertTrue(
                    "expected the search to be cancelled, but got: " + error,
                    error != null && error.toString().contains("task_cancelled_exception")
                );
            }
        }
        // The paused searches were running when they were cancelled, so they may complete once unblocked.
        for (Future<Map<String, Object>> search : paused) {
            assertCompletedOrCancelled(search.get());
        }
        logger.info(
            "held [{}] bytes charged, rejected [{}] of [{}] searches, [{}] of them while parsing",
            heldCharge,
            rejected,
            HELD_SEARCHES,
            rejectedWhileParsing
        );
        // Each waiting search was parsed long before and is charged about 24 MB for its filters.
        assertThat(
            "the searches waiting for a thread keep their parsed requests charged",
            heldCharge,
            greaterThan((HELD_SEARCHES - rejected) * ByteSizeValue.ofMb(20).getBytes())
        );
        assertThat("expected the breakers to reject some of the searches", rejected, greaterThan(0));
        assertThat("expected some of the searches to get past the breakers and wait", rejected, lessThan(HELD_SEARCHES));
        assertNodeAlive();
    }

    private static void assertCompletedOrCancelled(Map<String, Object> error) {
        assertTrue(
            "expected the search to complete or be cancelled, but got: " + error,
            error == null || error.toString().contains("task_cancelled_exception")
        );
    }

    private static int completed(List<? extends Future<?>> futures) {
        return (int) futures.stream().filter(Future::isDone).count();
    }

    private static int queuedSearches() throws IOException {
        Map<?, ?> nodes = (Map<?, ?>) entityAsMap(adminClient().performRequest(new Request("GET", "/_nodes/stats/thread_pool"))).get(
            "nodes"
        );
        @SuppressWarnings("unchecked")
        Map<String, Object> node = (Map<String, Object>) nodes.values().iterator().next();
        return ((Number) XContentMapValues.extractValue("thread_pool.search.queue", node)).intValue();
    }

    private static void cancelSearches() throws IOException {
        Request cancel = new Request("POST", "/_tasks/_cancel");
        cancel.addParameter("actions", "indices:data/read/search");
        adminClient().performRequest(cancel);
    }

    /** A multi-search of {@code searches} searches, each a {@code terms} query of {@link #TERMS_PER_SEARCH} one-character values. */
    private static Request multiSearch(int searches) {
        String search = Strings.format("""
            {}
            {"size": 0, "query": {"terms": {"k": [%s]}}}
            """, repeat(TERMS_PER_SEARCH, i -> "\"a\""));
        Request multiSearch = new Request("POST", "/" + INDEX + "/_msearch");
        multiSearch.setJsonEntity(search.repeat(searches));
        return multiSearch;
    }

    private static String filtersAggregation(int filters) {
        return Strings.format("""
            {"size": 0, "aggs": {"f": {"filters": {"filters": [%s]}}}}""", repeat(filters, RequestParsingHeapPressureIT::termClause));
    }

    /**
     * Asserts that the search succeeds with {@code small} elements, and that with {@code large} it is rejected by the parse charge rather
     * than a later one, and the node survives.
     */
    private void assertTripsWhileParsing(IntFunction<String> body, int small, int large) throws IOException {
        assertTripsOnlyWhenLarge(n -> search(INDEX, body.apply(n)), small, large, ParsedContentReservation.LABEL);
    }

    private static String termClause(int i) {
        return Strings.format("""
            {"term": {"k": "%s"}}""", term(i % DOCS));
    }

    /** Eight characters, the same length for every {@code i}, so the size of a request depends only on how many it has. */
    private static String term(int i) {
        return Strings.format("t%07d", i);
    }

    private static String repeat(int count, IntFunction<String> element) {
        StringBuilder elements = new StringBuilder(count * (element.apply(0).length() + 1));
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                elements.append(',');
            }
            elements.append(element.apply(i));
        }
        return elements.toString();
    }
}
