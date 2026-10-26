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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * The automatons the unified highlighter builds in the fetch phase for fuzzy clauses must be accounted by the request circuit breaker.
 *
 * <p>For every highlighted field the highlighter rebuilds one Levenshtein automaton per fuzzy clause it collects, and with
 * {@code require_field_match: false} it collects the clauses of every field. The automaton grows with the length of the term: a
 * 250-character term at two edits retains about 6 MB. With {@code FIELDS} fields that is {@code FIELDS}² automatons per shard fetch,
 * about 100 MB, so the fetches of a few overlapping requests exhaust a small heap. Charged to the breaker, the excess requests are
 * rejected; uncharged, the node runs out of memory.
 *
 * <p>The fuzzy clauses are only in {@code highlight_query}, so the query phase, which accounts its own automatons, builds none and
 * cannot reject the request before the fetch phase. The pausable field {@code p} is highlighted last: the highlighters of the text
 * fields, and their automatons, are built and cached for the shard before its script runs and parks the search thread.
 */
public class FuzzyHighlightHeapPressureIT extends HeapPressureTestCase {

    private static final String INDEX = "fuzzy-highlight-heap-pressure";
    private static final int SHARDS = 5;
    private static final int FIELDS = 4;
    // Create 8 parallel requests, to create enough heap pressure.
    private static final int THREAD_COUNT = 8;
    // Under the standard tokenizer's maximum token length of 255, so the term is indexed whole and the documents match it.
    private static final int HEAVY_TERM_LENGTH = 250;
    private static final int SMALL_TERM_LENGTH = 10;
    // Set this to true to disable the circuit breaker and validate that the OOM actually happens instead of being caught by the breaker.
    private static final boolean VALIDATE_OOM = false;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .module("test-pausable-field")
        // Parked requests hold one search thread per shard, so the pool has to be large enough for their automatons to add up to more
        // than the heap.
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

    private String heavyTerm;
    private String smallTerm;

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void createTestIndex() throws IOException {
        heavyTerm = randomAlphaOfLength(HEAVY_TERM_LENGTH).toLowerCase(Locale.ROOT);
        smallTerm = randomAlphaOfLength(SMALL_TERM_LENGTH).toLowerCase(Locale.ROOT);

        String properties = fieldNames().map(f -> "\"" + f + "\": {\"type\": \"text\"}").collect(Collectors.joining(", "));
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(Strings.format("""
            {
              "settings": {"number_of_shards": %d, "number_of_replicas": 0},
              "mappings": {
                "runtime": {"p": {"type": "long", "script": {"source": "", "lang": "pause"}}},
                "properties": {%s}
              }
            }
            """, SHARDS, properties));
        client().performRequest(create);

        // Highlighters are built per shard at its first hit however many hits it has, so a few documents per shard are enough.
        String source = fieldNames().map(f -> "\"" + f + "\": \"" + heavyTerm + " " + smallTerm + "\"")
            .collect(Collectors.joining(", ", "{", "}"));
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < SHARDS * 10; i++) {
            bulk.append("{\"index\":{}}\n").append(source).append('\n');
        }
        Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
        bulkRequest.addParameter("refresh", "true");
        bulkRequest.setJsonEntity(bulk.toString());
        client().performRequest(bulkRequest);
    }

    /**
     * Heavy requests held in flight at the same time either complete or are rejected by the breaker, never anything else, and the node
     * survives them. The pausable field blocks each shard fetch after its highlighters are built, so the overlap is guaranteed rather
     * than a matter of timing.
     */
    public void testPausedFuzzyHighlightsTripBreaker() throws Exception {
        /* A short term, whose automatons are small, is unaffected. */
        assertNull(errorBodyOrNull(highlightSearch(smallTerm, false)));

        List<Future<Map<String, Object>>> futures;
        blockPauseField();
        try {
            futures = submit(THREAD_COUNT, () -> errorBodyOrNull(highlightSearch(heavyTerm, true)));
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
        assertThat("expected some of the requests to get past the breaker and hold their automatons", rejected, lessThan(THREAD_COUNT));
        // Check that the node didn't OOM and is still alive.
        assertNodeAlive();
    }

    /**
     * Matches {@code term} exactly, and highlights every text field with a fuzzy {@code highlight_query} on all of them. With
     * {@code pausable}, {@code p} is highlighted last, so its script parks the shard fetch while the highlighters of the text fields
     * hold their automatons.
     */
    private static Request highlightSearch(String term, boolean pausable) {
        String fuzzyClauses = fieldNames().map(f -> Strings.format("""
            {"match": {"%s": {"query": "%s", "fuzziness": 2}}}""", f, term)).collect(Collectors.joining(", "));
        String highlightFields = Stream.concat(fieldNames(), pausable ? Stream.of("p") : Stream.empty())
            .map(f -> "\"" + f + "\": {}")
            .collect(Collectors.joining(", "));
        Request search = new Request("POST", "/" + INDEX + "/_search");
        // Each request has to run its fetch phase on every shard rather than be answered from the shard request cache.
        search.addParameter("request_cache", "false");
        search.setJsonEntity(Strings.format("""
            {
              "query": {"match": {"f0": "%s"}},
              "highlight": {
                "require_field_match": false,
                "highlight_query": {"bool": {"should": [%s]}},
                "fields": {%s}
              }
            }
            """, term, fuzzyClauses, highlightFields));
        return search;
    }

    private static Stream<String> fieldNames() {
        return IntStream.range(0, FIELDS).mapToObj(i -> "f" + i);
    }
}
