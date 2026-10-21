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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A {@code terms} filter just under {@code index.max_terms_count} must be accounted for, or bounded, in every form it takes on a node.
 *
 * <p>The filter is parsed into a {@code TermsQueryBuilder} holding a {@code BytesRef} and a {@code byte[]} per term, which every
 * {@code ShardSearchRequest} of the search carries, including the ones still waiting in the search queue. When the shard runs it, the
 * builder is rewritten into a {@code TermInSetQuery} that holds the terms again, as a pre-filter of the kNN query if there is one. Neither
 * is charged to a circuit breaker, and {@code index.max_terms_count} limits the terms of one query rather than the memory of a node, so a
 * few overlapping multi-searches whose sub-searches each stay under it exhaust a small heap.
 *
 * <p>This is the shape of a search-tier out-of-memory seen in production: a multi-search whose sub-searches each carry a filter of about
 * 62k ids, as a plain filter or as the filter of a kNN query on a nested vector field. The vector index type does not matter, since the
 * filter is held the same way whichever kNN query it is handed to, so {@code int8_hnsw}, which needs no licensed plugin, is used.
 * <p>See <a href="https://github.com/elastic/elasticsearch/issues/162788">#162788</a> for issue details.</p>
 */
public class LargeTermsFilterHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "large-terms-filter";
    private static final int SHARDS = 4;
    private static final int DOCS = 2000;
    private static final int CHUNKS_PER_DOC = 2;
    private static final int DIMS = 64;
    // Just under the default index.max_terms_count of 65536.
    private static final int TERMS = 62_000;
    private static final int SEARCHES_PER_MULTI_SEARCH = 5;
    // A sub-search holds about 7 MB per shard and so 28 MB over SHARDS shards; THREADS * SEARCHES_PER_MULTI_SEARCH of them hold more than
    // the heap.
    private static final int THREADS = 8;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m")
        // A few search threads, so that shard requests wait in the queue with their parsed source, as they do on a busy node.
        .setting("thread_pool.search.size", "4")
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    /** A request that a node with no other load handles, so that the cases below cannot pass on a node that rejects every such request. */
    public void testSingleTermsFilterSucceeds() throws IOException {
        createIndex();
        assertSucceeds(search(INDEX, termsSearch()));
    }

    /** As {@link #testSingleTermsFilterSucceeds}, with the filter under a kNN query. */
    public void testSingleKnnWithTermsFilterSucceeds() throws IOException {
        createIndex();
        assertSucceeds(search(INDEX, knnSearch()));
    }

    /**
     * Overlapping multi-searches whose sub-searches each filter on a large {@code terms} list either complete or are rejected by a circuit
     * breaker, never anything else, and the node survives them.
     */
    public void testConcurrentTermsFilterMultiSearchesDoNotKillTheNode() throws Exception {
        createIndex();
        assertOverlappingMultiSearchesSurvive(termsSearch());
    }

    /** As {@link #testConcurrentTermsFilterMultiSearchesDoNotKillTheNode}, with the filter as the pre-filter of a nested kNN query. */
    public void testConcurrentKnnWithTermsFilterMultiSearchesDoNotKillTheNode() throws Exception {
        createIndex();
        assertOverlappingMultiSearchesSurvive(knnSearch());
    }

    private void assertOverlappingMultiSearchesSurvive(String searchBody) throws Exception {
        // A request's entity is not safe to share between threads, so each builds its own.
        List<Future<Map<String, Object>>> futures = submit(THREADS, () -> multiSearchFailureOrNull(multiSearch(searchBody)));
        for (Future<Map<String, Object>> future : futures) {
            Map<String, Object> failure = future.get();
            if (failure != null) {
                assertTrue(
                    "expected only circuit_breaking_exception failures, but got: " + failure,
                    containsCircuitBreakingException(failure)
                );
            }
        }
        // Check that the node didn't OOM and is still alive.
        assertNodeAlive();
    }

    /**
     * Performs the multi-search and returns the first failure, whether of the whole request or of one of its searches, which multi-search
     * reports in its response, or {@code null} if every search succeeded.
     */
    private Map<String, Object> multiSearchFailureOrNull(Request multiSearch) throws IOException {
        Map<String, Object> body;
        try {
            body = entityAsMap(client().performRequest(multiSearch));
        } catch (ResponseException e) {
            return entityAsMap(e.getResponse());
        }
        for (Object response : (List<?>) body.get("responses")) {
            Map<?, ?> search = (Map<?, ?>) response;
            if (search.containsKey("error")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> failure = (Map<String, Object>) search;
                return failure;
            }
        }
        return null;
    }

    private static Request multiSearch(String searchBody) {
        String compactBody = searchBody.replace("\n", " ");
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < SEARCHES_PER_MULTI_SEARCH; i++) {
            body.append(Strings.format("{\"index\": \"%s\", \"request_cache\": false}\n%s\n", INDEX, compactBody));
        }
        Request request = new Request("POST", "/_msearch");
        // Hits are not what the cases look at.
        request.addParameter("filter_path", "responses.status,responses.error");
        request.setJsonEntity(body.toString());
        return request;
    }

    private static String termsSearch() {
        return Strings.format("""
            {"size": 0, "query": {"bool": {"filter": [{"terms": {"id": [%s]}}]}}}""", termsList());
    }

    private static String knnSearch() {
        return Strings.format("""
            {
              "size": 10,
              "query": {
                "knn": {
                  "field": "chunks.vector",
                  "query_vector": [%s],
                  "k": 10,
                  "num_candidates": 50,
                  "filter": {"terms": {"id": [%s]}}
                }
              }
            }""", vector(0), termsList());
    }

    /** The ids of {@link #DOCS} documents are the first of the {@link #TERMS} values, so the filter matches them all. */
    private static String termsList() {
        return IntStream.range(0, TERMS).mapToObj(i -> "\"" + id(i) + "\"").collect(Collectors.joining(","));
    }

    /** Distinct and long enough that the term bytes, not the per-term overhead alone, make up the filter. */
    private static String id(int i) {
        return Strings.format("doc-%016d", i);
    }

    private static String vector(int seed) {
        return IntStream.range(0, DIMS)
            .mapToObj(d -> Float.toString((((seed + 1) * (d + 1) % 17) + 1) / 18f))
            .collect(Collectors.joining(","));
    }

    private void createIndex() throws IOException {
        indexDocuments(INDEX, SHARDS, Strings.format("""
            {
              "id": {"type": "keyword"},
              "chunks": {
                "type": "nested",
                "properties": {
                  "vector": {"type": "dense_vector", "dims": %d, "index": true,
                        "similarity": "cosine", "index_options": {"type": "int8_hnsw"}}
                }
              }
            }""", DIMS), DOCS, i -> {
            String chunks = IntStream.range(0, CHUNKS_PER_DOC)
                .mapToObj(c -> Strings.format("{\"vector\": [%s]}", vector(i * CHUNKS_PER_DOC + c)))
                .collect(Collectors.joining(","));
            return Strings.format("{\"id\": \"%s\", \"chunks\": [%s]}", id(i), chunks);
        });
    }
}
