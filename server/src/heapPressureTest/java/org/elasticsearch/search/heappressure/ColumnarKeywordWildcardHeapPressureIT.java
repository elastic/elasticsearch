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
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.HeapPressureTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Wildcard queries on a keyword field of a {@code columnar} index, which has no inverted index and so answers them by running an
 * automaton over the column, must be accounted by the request circuit breaker.
 *
 * <p>A wildcard pattern is bounded by the fixed determinization work limit (10,000), so one automaton allocates tens of MB and retains
 * about 10 MB per shard. That cannot exhaust a normal heap, so the cluster runs with a deliberately small one, where the automatons of
 * a few overlapping requests do. Charged to the breaker, the excess requests are rejected; uncharged, the node runs out of memory.
 *
 * <p>Here the pausable field comes from {@code runtime_mappings} of the request, because a columnar index does not accept
 * mapping-level runtime fields.
 */
public class ColumnarKeywordWildcardHeapPressureIT extends HeapPressureTestCase {

    private static final String INDEX = "columnar-heap-pressure";
    private static final int SHARDS = 5;
    // More than the REST client's default of 10 connections per route, so no more than that many are parked at once and the rest wait
    // for one to be freed. Five shards each, even ten are enough to occupy the whole search pool.
    private static final int THREAD_COUNT = 8;
    // '*a' followed by N '?' determinizes to 2^N states: 8192 for N=11, the most the default 10,000 work limit accepts. The inner '?'
    // keeps the pattern from being rewritten to a term, prefix or contains query, which need no automaton.
    private static final String HEAVY_WILDCARD = "*a???????????*";
    private static final String SMALL_WILDCARD = "*a????*";
    // Set this to true to disable the circuit breaker and validate that the OOM actually happens instead of being caught by the breaker.
    private static final boolean VALIDATE_OOM = false;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .module("test-pausable-field")
        // Parked requests hold one search thread each, so the pool has to be large enough for their automatons to add up to more than
        // the heap.
        .setting("thread_pool.search.size", "32")
        .jvmArg("-Xms256m")
        .jvmArg("-Xmx256m")
        .apply(c -> {
            if (VALIDATE_OOM) {
                c.setting("indices.breaker.request.limit", "-1");
                c.setting("indices.breaker.total.use_real_memory", "false");
                c.setting("indices.breaker.total.limit", "100gb");
            }
        })
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void createTestIndex() throws IOException {
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(Strings.format("""
            {
              "settings": {"index.mode": "columnar", "number_of_shards": %d, "number_of_replicas": 0},
              "mappings": {"properties": {"k": {"type": "keyword", "index": false}}}
            }
            """, SHARDS));
        client().performRequest(create);

        // Automatons are built per shard however few documents it holds, so a few per shard is enough.
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < SHARDS * 10; i++) {
            bulk.append("{\"index\":{}}\n{\"k\":\"value-").append(i).append("-abcabcabc\"}\n");
        }
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);
    }

    /** Case-sensitive: a general pattern, which needs an automaton where a literal, prefix or contains pattern would not. */
    public void testPausedHeavyWildcardsTripBreaker() throws Exception {
        runPausedHeavyWildcards(false);
    }

    /** Case-insensitive: a pattern that is never rewritten to a plain term, so the automaton is always built. */
    public void testPausedHeavyCaseInsensitiveWildcardsTripBreaker() throws Exception {
        runPausedHeavyWildcards(true);
    }

    /**
     * Heavy requests held in flight at the same time either complete or are rejected by the breaker, never anything else, and the node
     * survives them. The pausable field blocks each request after its automaton is built, so the overlap is guaranteed rather than a
     * matter of timing: every thread of the search pool ends up holding one.
     */
    private void runPausedHeavyWildcards(boolean caseInsensitive) throws Exception {
        /* A pattern that fits comfortably under the breaker is unaffected. */
        assertNull(errorBodyOrNull(wildcardSearch(SMALL_WILDCARD, caseInsensitive, false)));

        List<Future<Map<String, Object>>> futures;
        blockPauseField();
        try {
            futures = submit(THREAD_COUNT, () -> errorBodyOrNull(wildcardSearch(HEAVY_WILDCARD, caseInsensitive, true)));
            waitForOverlappingRequests(2);
        } finally {
            unblockPauseField();
        }
        int rejected = 0;
        for (Future<Map<String, Object>> future : futures) {
            Map<String, Object> body = future.get();
            if (body != null) {
                assertTrue("expected only circuit_breaking_exception failures, but got: " + body, containsCircuitBreakingException(body));
                rejected++;
            }
        }
        assertThat("expected the request breaker to reject some of the overlapping requests", rejected, greaterThan(0));
        // Otherwise no automaton is ever retained, and the test only covers the cost of building one.
        assertThat("expected some of the requests to get past the breaker and hold their automaton", rejected, lessThan(THREAD_COUNT));
        // Check that the node didn't OOM and is still alive.
        assertNodeAlive();
    }

    /**
     * A wildcard search on {@code k}. With {@code pausable} it also filters on the runtime field {@code p}: the automaton is built with
     * the query, and the script only runs later, when documents are scored, so while the field is blocked every such request keeps its
     * automaton alive and parks a search thread.
     */
    private static Request wildcardSearch(String pattern, boolean caseInsensitive, boolean pausable) {
        String wildcard = Strings.format("""
            {"wildcard": {"k": {"value": "%s", "case_insensitive": %s}}}""", pattern, caseInsensitive);
        Request search = new Request("POST", "/" + INDEX + "/_search");
        // Each request has to build its automaton on every shard rather than be answered from the shard request cache.
        search.addParameter("request_cache", "false");
        search.setJsonEntity(pausable ? Strings.format("""
            {
              "size": 0,
              "runtime_mappings": {"p": {"type": "long", "script": {"source": "", "lang": "pause"}}},
              "query": {"bool": {"must": [%s], "filter": [{"term": {"p": 1}}]}}
            }
            """, wildcard) : Strings.format("""
            {"size": 0, "query": %s}
            """, wildcard));
        return search;
    }
}
