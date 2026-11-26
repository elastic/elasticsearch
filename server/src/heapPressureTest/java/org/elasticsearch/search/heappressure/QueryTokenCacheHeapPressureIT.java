/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Match-family queries cache every token of their analyzed text before they build any clause or check the clause limit, on every search
 * thread that parses the query for a shard. The cache must be charged to the request breaker, and each cached token must cost only its
 * own term, not a copy of a long term that the thread's reused token stream encoded earlier.
 *
 * <p>Modeled on a search node that ran out of memory, where a {@code multi_match} over four fields with a text of 50,005 short tokens
 * reached four search threads at once, and each cached token held a copy of a term left over from an earlier analysis. This node also
 * has four search threads, and the same query exceeds its clause limit.
 */
public class QueryTokenCacheHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "query-tokens";
    private static final int SHARDS = 5;
    private static final int SEARCH_THREADS = 4;
    private static final List<String> FIELDS = List.of("text1", "text2", "text3", "text4");
    /** As many tokens as the query of the incident, about 210 MB per search thread with a {@link #LEFTOVER_TERM_CHARS} leftover term. */
    private static final int INCIDENT_TOKENS = 50_005;
    private static final int LEFTOVER_TERM_CHARS = 4000;
    /** About 330 MB per search thread, with or without a leftover term, so that only the breaker can keep the node alive. */
    private static final int ACCOUNTED_TOKENS = 2_000_000;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").module("test-pausable-field")
        .setting("thread_pool.search.size", Integer.toString(SEARCH_THREADS))
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    /**
     * Each field's analyzer keeps terms of up to {@link #LEFTOVER_TERM_CHARS} characters whole, as the ICU tokenizer of the incident kept
     * a term of more than a thousand characters.
     */
    @Before
    public void indexText() throws IOException {
        StringBuilder properties = new StringBuilder("{");
        for (String field : FIELDS) {
            properties.append(properties.length() > 1 ? ", " : "").append(Strings.format("""
                "%s": {"type": "text", "analyzer": "long_terms"}""", field));
        }
        properties.append(", \"tag\": {\"type\": \"keyword\"}}");
        indexDocuments(INDEX, SHARDS, """
            "analysis": {
              "tokenizer": {"long_terms": {"type": "standard", "max_token_length": 4096}},
              "analyzer": {"long_terms": {"type": "custom", "tokenizer": "long_terms"}}
            }""", properties.toString(), 100, i -> Strings.format("""
            {"text1": "doc %d", "tag": "tag-%d"}""", i, i));
    }

    /**
     * The query of the incident, on search threads whose token streams last encoded a long term. Every shard must fail on the clause
     * limit, which the query exceeds, rather than run the node out of memory while it caches the tokens.
     */
    public void testIncidentQueryFailsOnClauseLimit() throws Exception {
        leaveLongTermOnEverySearchThread();

        Map<String, Object> body = errorBodyOrNull(search(INDEX, multiMatch("ab ".repeat(INCIDENT_TOKENS))));
        assertNotNull("expected the query to exceed the clause limit", body);
        assertThat(body.toString(), containsString("too many nested clauses"));
        assertFalse("no breaker should be needed once the leftover term is not copied: " + body, containsCircuitBreakingException(body));
        assertNodeAlive();
    }

    /**
     * A text far longer than the incident's, which no shard can cache within the request breaker's limit, must be rejected by the breaker
     * while the tokens are cached.
     */
    public void testLongTextTripsBreakerWhileTokensAreCached() throws IOException {
        // A thousand tokens stay under the clause limit, so the small search succeeds.
        assertTripsOnlyWhenLarge(
            tokens -> search(INDEX, Strings.format("""
                {"query": {"match": {"%s": "%s"}}}""", FIELDS.get(0), "ab ".repeat(tokens))),
            1000,
            ACCOUNTED_TOKENS,
            "query_tokens:" + FIELDS.get(0)
        );
    }

    /**
     * Parks one search on every search thread after it has parsed a query with a single long term, so that each thread's reused token
     * streams last encoded that term, as an earlier query or a highlighter could have left them.
     */
    private void leaveLongTermOnEverySearchThread() throws Exception {
        // The filter on the pause field drives the search, so every shard runs its script, while the long term matches nothing.
        String prime = Strings.format("""
            {
              "size": 0,
              "runtime_mappings": {"p": {"type": "long", "script": {"source": "", "lang": "pause"}}},
              "query": {"bool": {"should": [%s], "filter": [{"term": {"p": 1}}]}}
            }
            """, multiMatchClause("a".repeat(LEFTOVER_TERM_CHARS)));
        List<Future<Map<String, Object>>> primes;
        blockPauseField();
        try {
            primes = submit(1, () -> errorBodyOrNull(search(INDEX, prime)));
            assertBusy(() -> assertThat(parkedCount(), equalTo(SEARCH_THREADS)), 30, TimeUnit.SECONDS);
        } finally {
            unblockPauseField();
        }
        assertNull(primes.get(0).get(1, TimeUnit.MINUTES));
    }

    private static String multiMatch(String text) {
        return Strings.format("""
            {"query": {"bool": {"must": [%s]}}}""", multiMatchClause(text));
    }

    private static String multiMatchClause(String text) {
        String fields = String.join("\", \"", FIELDS);
        return Strings.format("""
            {"multi_match": {"query": "%s", "type": "best_fields", "operator": "or", "fields": ["%s"]}}""", text, fields);
    }
}
