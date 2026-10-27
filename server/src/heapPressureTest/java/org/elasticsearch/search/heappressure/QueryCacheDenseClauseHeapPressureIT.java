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
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/// The bitsets the query cache builds for the clauses of a filter must be accounted for, or bounded, before they are allocated.
///
/// When a filter clause is admitted to the query cache, `LRUQueryCache#cacheIntoBitSet` allocates a `FixedBitSet` of `maxDoc` bits for
/// it, whatever the clause matches, and only counts it against the cache after it is built. A clause over a doc-values-only field
/// reports a cost of about `maxDoc`, so every such clause gets the dense bitset. A `bool` filter of `CLAUSES` such clauses holds
/// `CLAUSES` bitsets at once while it is built, and concurrent searches over the same segment each build their own. The request breaker
/// does not charge them, and the cache size limit applies only after they are built, so a few overlapping searches exhaust a small heap.
///
/// This is the shape of an out-of-memory seen in production: a few concurrent searches with about 30 `OR`ed clauses over one segment of
/// tens of millions of documents, each search holding about 330 MB of bitsets on a 2 GB heap. Reproducing it takes four conditions:
///
/// - The caching policy of the shard admits a clause after it has been seen 5 times among the last 256 filters used, and in this test
///   almost every lookup is a miss that needs that again. A search can therefore have at most about 50 clauses: `CLAUSES` of them, which
///   `WARMUP_SEARCHES` warm-up searches first get admitted.
/// - The cache refuses a segment unless it has room for 5 bitsets of it, about 2.5 MB here, so the cache size has to be more than that.
/// - The cache is not much larger than that, so concurrent searches cannot be answered from one another's bitsets, which a cache hit
///   would not allocate.
/// - One shard, so that one search task builds the bitsets of all the clauses at once. With the documents spread over many shards each
///   task holds only its share, and the number of tasks that run at once is limited by the size of the search thread pool.
///
/// For the test to pass, searches may fail only with a circuit breaker rejection, and the node must survive.
///
/// See [#139398](https://github.com/elastic/elasticsearch/issues/139398) for the details.
public class QueryCacheDenseClauseHeapPressureIT extends HeapPressureTestCase {

    private static final String INDEX = "query-cache-dense-clause-heap-pressure";
    // A bitset takes DOCS / 8 bytes, about 500 KB.
    private static final int DOCS = 4_000_000;
    // A clause is admitted to the cache after being seen 5 times among the last 256 filters used, so a search can have at most about 50
    // clauses.
    private static final int CLAUSES = 40;
    // Searches run one after another to get every clause admitted.
    private static final int WARMUP_SEARCHES = 8;
    private static final int BULK_SIZE = 10_000;
    private static final int BULKS_PER_FLUSH = 3;
    // A search holds CLAUSES * DOCS / 8 bytes, about 20 MB, and 16 of them hold more than the heap.
    private static final int THREAD_COUNT = 16;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .setting("thread_pool.search.size", "16")
        // Room for 6 bitsets, just over the 5 that the cache needs to admit the segment, instead of the default tenth of the heap, so that
        // the clauses of concurrent searches cannot be answered from one another's bitsets.
        .setting("indices.queries.cache.size", "3mb")
        .jvmArg("-Xms128m")
        .jvmArg("-Xmx128m")
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
        RestClient client = client();
        client.performRequest(create);

        for (int from = 0; from < DOCS; from += BULK_SIZE) {
            StringBuilder bulk = new StringBuilder();
            for (int i = from; i < Math.min(DOCS, from + BULK_SIZE); i++) {
                bulk.append("{\"index\":{}}\n");
                bulk.append(Strings.format("{\"tag\": \"%s\", \"n\": %d}\n", tag(i % CLAUSES), i + 1));
            }
            Request bulkRequest = new Request("POST", "/" + INDEX + "/_bulk");
            bulkRequest.setJsonEntity(bulk.toString());
            client.performRequest(bulkRequest);
            // Test clusters run with assertions, and the translog then keeps every operation until it is rolled, which a node this small
            // cannot hold for this many documents.
            if ((from / BULK_SIZE + 1) % BULKS_PER_FLUSH == 0) {
                client.performRequest(new Request("POST", "/" + INDEX + "/_flush"));
                logger.info("{}: flushed", from);
            }
        }
        client.performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
        Request forceMerge = new Request("POST", "/" + INDEX + "/_forcemerge");
        forceMerge.addParameter("max_num_segments", "1");
        // We need to have one segment because all caching is happening per segment.
        client.performRequest(forceMerge);
    }

    /**
     * Overlapping searches whose cached clauses each hold a full-segment bitset either complete or are rejected by a circuit breaker,
     * never anything else, and the node survives them.
     */
    public void testConcurrentDenseClauseCachingDoesNotKillTheNode() throws Exception {
        logger.info("Warming up");
        // Get every clause admitted to the cache. Without this the cases below allocate next to nothing and pass on any node.
        for (int i = 0; i < WARMUP_SEARCHES; i++) {
            assertNull("the warm-up search must succeed on an idle node", errorBodyOrNull(denseClauseSearch()));
        }
        assertThat("the clauses must be admitted to the query cache", cachedEntryCount(), greaterThanOrEqualTo((long) CLAUSES));
        logger.info("Running search");
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

    /** The number of entries ever admitted to the query cache of the node. */
    private static long cachedEntryCount() throws IOException {
        Map<?, ?> nodes = (Map<?, ?>) entityAsMap(adminClient().performRequest(new Request("GET", "/_nodes/stats/indices/query_cache")))
            .get("nodes");
        Map<?, ?> node = (Map<?, ?>) nodes.values().iterator().next();
        return ((Number) XContentMapValues.extractValue("indices.query_cache.cache_count", node)).longValue();
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
