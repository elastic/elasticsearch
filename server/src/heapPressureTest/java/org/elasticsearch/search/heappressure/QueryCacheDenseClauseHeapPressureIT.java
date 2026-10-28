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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The bitsets the query cache builds for the clauses of a filter must be accounted for, or bounded, before they are allocated.
 *
 * <p>When a filter clause is admitted to the query cache, {@code LRUQueryCache#cacheIntoBitSet} allocates a {@code FixedBitSet} of
 * {@code maxDoc} bits for it, whatever the clause matches, and only counts it against the cache after it is built. A clause over a
 * doc-values-only field reports a cost of about {@code maxDoc}, so every such clause gets the dense bitset. A {@code bool} filter of
 * {@code CLAUSES} such clauses holds {@code CLAUSES} bitsets at once while its disjunction is built, and concurrent searches over
 * the same segment each build their own. Neither the request breaker nor the cache size limit sees any of it, so a few overlapping
 * searches exhaust a small heap.
 */
public class QueryCacheDenseClauseHeapPressureIT extends HeapPressureTestCase {

    private static final String INDEX = "query-cache-dense-clause-heap-pressure";
    private static final int DOCS = 500_000;
    // Under the default maximum bool clause count.
    private static final int CLAUSES = 900;
    private static final int BULK_SIZE = 10_000;
    // A search holds CLAUSES * DOCS / 8 bytes, about 56 MB, and 8 of them hold more than the heap.
    private static final int THREAD_COUNT = 8;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .setting("thread_pool.search.size", "16")
        .jvmArg("-Xms256m")
        .jvmArg("-Xmx256m")
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void createTestIndex() throws IOException {
        Request create = new Request("PUT", "/" + INDEX);
        // Doc values only, so the clauses are doc values range queries whose cost is the number of docs that have the field.
        create.setJsonEntity("""
            {
              "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": -1
              },
              "mappings": {
                "properties": {
                  "tag": {"type": "keyword", "index": false, "doc_values": true},
                  "n": {"type": "long", "index": false, "doc_values": true}
                }
              }
            }
            """);
        client().performRequest(create);

        for (int from = 0; from < DOCS; from += BULK_SIZE) {
            StringBuilder bulk = new StringBuilder();
            for (int i = from; i < Math.min(DOCS, from + BULK_SIZE); i++) {
                bulk.append("{\"index\":{}}\n");
                bulk.append(Strings.format("{\"tag\": \"%s\", \"n\": %d}\n", tag(i % CLAUSES), i + 1));
            }
            Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
            bulkRequest.setJsonEntity(bulk.toString());
            client().performRequest(bulkRequest);
        }
        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
        Request forceMerge = new Request("POST", "/" + INDEX + "/_forcemerge");
        forceMerge.addParameter("max_num_segments", "1");
        client().performRequest(forceMerge);
    }

    /**
     * Overlapping searches whose cached clauses each hold a full-segment bitset either complete or are rejected by a circuit breaker,
     * never anything else, and the node survives them.
     */
    public void testConcurrentDenseClauseCachingDoesNotKillTheNode() throws Exception {
        List<Future<Map<String, Object>>> futures = submit(THREAD_COUNT, () -> errorBodyOrNull(denseClauseSearch()));
        for (Future<Map<String, Object>> future : futures) {
            Map<String, Object> body = future.get();
            if (body != null) {
                assertTrue("expected only circuit_breaking_exception failures, but got: " + body, containsCircuitBreakingException(body));
            }
        }
        // Check that the node didn't OOM and is still alive.
        assertNodeAlive();
    }

    /**
     * Filters on a range over {@code n}, as the time range of a dashboard query is, and on one of {@code CLAUSES} values of
     * {@code tag}, each a clause of its own.
     */
    private static Request denseClauseSearch() {
        String tags = IntStream.range(0, CLAUSES)
            .mapToObj(i -> Strings.format("{\"term\": {\"tag\": \"%s\"}}", tag(i)))
            .collect(Collectors.joining(", "));
        Request search = new Request("POST", "/" + INDEX + "/_search");
        // Each request has to run on the shard rather than be answered from the shard request cache.
        search.addParameter("request_cache", "false");
        search.setJsonEntity(Strings.format("""
            {
              "size": 0,
              "query": {
                "bool": {
                  "filter": [
                    {"range": {"n": {"gte": 1}}},
                    {"bool": {"should": [%s], "minimum_should_match": 1}}
                  ]
                }
              }
            }
            """, tags));
        return search;
    }

    private static String tag(int i) {
        return "tag-" + i;
    }
}
