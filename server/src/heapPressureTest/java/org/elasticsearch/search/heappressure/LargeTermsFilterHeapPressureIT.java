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
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A {@code terms} filter must be accounted for, or bounded, in every form it takes on a node.
 *
 * <p>The filter is parsed into a {@code TermsQueryBuilder} holding a {@code BytesRef} and a {@code byte[]} per term, which every
 * {@code ShardSearchRequest} of the search carries, including the ones still waiting in the search queue. When the shard runs it, the
 * builder is turned into a {@code TermInSetQuery} that holds the terms again, as a pre-filter of the kNN query if there is one. That
 * query is charged to the request breaker when it is built, but the builder, which every shard request and every queued request keeps
 * until it completes, is not. {@code index.max_terms_count} limits the terms of one query rather than the memory of a node, so a few
 * overlapping multi-searches whose sub-searches each stay under it exhaust a small heap.
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
    private static final int THREADS = 8;
    // Each sub-search parses its own TermsQueryBuilder. A parsed JSON string becomes a BytesRef whose byte[] is sized for the worst-case
    // UTF-8 length, 3 bytes per char, so a 20-char id takes about 108 bytes and TERMS ids about 6.7 MB, on top of about 1.4 MB of request
    // JSON. On one node the shard requests share the builder, and each shard adds only its prefix-coded TermInSetQuery.
    // THREADS * SEARCHES_PER_MULTI_SEARCH sub-searches in flight hold about 320 MB, more than the heap.

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
        List<Future<List<Object>>> futures = submit(THREADS, () -> nonBreakerFailures(multiSearch(searchBody)));
        for (Future<List<Object>> future : futures) {
            List<Object> failures = future.get();
            assertTrue("expected only circuit_breaking_exception failures, but got: " + failures, failures.isEmpty());
        }
        // Check that the node didn't OOM and is still alive.
        assertNodeAlive();
    }

    /**
     * Performs the multi-search and returns every failure that is not a circuit breaker rejection: of the whole request, of one of its
     * searches, which multi-search reports in its response, or of some shards of a search that otherwise returned results. Empty if every
     * search succeeded or was rejected only by a circuit breaker.
     */
    private List<Object> nonBreakerFailures(Request multiSearch) throws IOException {
        List<Object> failures = new ArrayList<>();
        Map<String, Object> body;
        try {
            body = entityAsMap(client().performRequest(multiSearch));
        } catch (ResponseException e) {
            Map<String, Object> response = entityAsMap(e.getResponse());
            addNonBreakerFailures(response.getOrDefault("error", response), failures);
            return failures;
        }
        for (Object response : (List<?>) body.get("responses")) {
            Map<?, ?> search = (Map<?, ?>) response;
            Object error = search.get("error");
            if (error != null) {
                addNonBreakerFailures(error, failures);
            }
            List<?> shardFailures = (List<?>) XContentMapValues.extractValue("_shards.failures", search);
            if (shardFailures != null) {
                addNonBreakerShardFailures(shardFailures, failures);
            }
        }
        return failures;
    }

    /**
     * Adds {@code error} to {@code failures} unless it is a circuit breaker rejection. A search that failed on every shard reports each
     * shard under {@code failed_shards}, and those are checked one by one, so that a breaker rejection on one shard cannot hide an
     * unrelated failure on another.
     */
    private static void addNonBreakerFailures(Object error, List<Object> failures) {
        if (error instanceof Map<?, ?> map && map.get("failed_shards") instanceof List<?> failedShards && failedShards.isEmpty() == false) {
            addNonBreakerShardFailures(failedShards, failures);
        } else if (containsCircuitBreakingException(error) == false) {
            failures.add(error);
        }
    }

    private static void addNonBreakerShardFailures(List<?> shardFailures, List<Object> failures) {
        for (Object shardFailure : shardFailures) {
            if (containsCircuitBreakingException(shardFailure) == false) {
                failures.add(shardFailure);
            }
        }
    }

    private static Request multiSearch(String searchBody) {
        String compactBody = searchBody.replace("\n", " ");
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < SEARCHES_PER_MULTI_SEARCH; i++) {
            body.append(Strings.format("{\"index\": \"%s\", \"request_cache\": false}\n%s\n", INDEX, compactBody));
        }
        Request request = new Request("POST", "/_msearch");
        // Hits are not what the cases look at.
        request.addParameter("filter_path", "responses.status,responses.error,responses._shards.failures");
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
