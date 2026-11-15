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
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.HeapPressureTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
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
/// - The caching policy of the shard has to admit the clauses. In production it admits a clause after it has been seen 5 times among
///   the last 256 filters used, which a dashboard gets by being refreshed; here the index has `index.queries.cache.everything` set, so
///   every clause is admitted on first sight. Without it a search could have at most about 50 clauses, and the second case below, whose
///   clauses are all different, could not be cached at all.
/// - The cache refuses a segment unless it has room for 5 bitsets of it, about 600 KB here, which the default cache size has.
/// - The searches must miss the cache, since a hit allocates nothing: the first case clears the query cache right before the burst, so
///   that every search misses on every clause at the same time, and the clauses of the searches of the second case are all different.
/// - One shard, so that one search task builds the bitsets of all the clauses at once. With the documents spread over many shards each
///   task holds only its share, and the number of tasks that run at once is limited by the size of the search thread pool.
///
/// The cache builds an entry only once at a time: concurrent searches that miss on the same clause run it uncached while the first
/// builds its bitset, and are then answered from the cache. That bounds identical searches to `CLAUSES` bitsets between them, so the
/// second case sends searches whose clauses are all different, which that cannot help with.
///
/// Indexing takes most of the running time, so the index is built once and shared by both cases.
///
/// For the test to pass, searches may fail only with a circuit breaker rejection, and the node must survive.
///
/// See [#139398](https://github.com/elastic/elasticsearch/issues/139398) for the details.
public class QueryCacheDenseClauseHeapPressureIT extends HeapPressureTestCase {

    private static final String INDEX = "query-cache-dense-clause-heap-pressure";
    // A bitset takes DOCS / 8 bytes, about 125 KB.
    private static final int DOCS = 1_000_000;
    // The number of OR-ed clauses of a search, as in the production case.
    private static final int CLAUSES = 40;
    private static final int BULK_SIZE = 10_000;
    private static final int BULKS_PER_FLUSH = 3;
    // A search holds CLAUSES * DOCS / 8 bytes, about 5 MB, and 64 of them hold more than the heap.
    private static final int THREAD_COUNT = 64;
    // Enough distinct tags for every concurrent search to have CLAUSES of its own.
    private static final int TAGS = THREAD_COUNT * CLAUSES;

    /** Whether the index has been built on the cluster yet. The cluster lives for the whole class, and so does the index. */
    private static boolean indexCreated;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        // Registers index.queries.cache.everything.
        .module("test-cache-everything")
        .setting("thread_pool.search.size", Integer.toString(THREAD_COUNT))
        .jvmArg("-Xms128m")
        .jvmArg("-Xmx128m")
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    /** The client keeps at most 10 connections per route by default, which would cap the searches that reach the node at 10. */
    @Override
    protected void configureClient(RestClientBuilder builder, Settings settings) throws IOException {
        super.configureClient(builder, settings);
        builder.setHttpClientConfigCallback(http -> http.setMaxConnPerRoute(THREAD_COUNT).setMaxConnTotal(THREAD_COUNT));
    }

    /** The index is shared by the cases of this class, so the test framework must not delete it after each one. */
    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    /** Creates and fills the index, once per class. */
    @Before
    public void createTestIndex() throws IOException {
        if (indexCreated) {
            return;
        }
        Request create = new Request("PUT", "/" + INDEX);
        // Doc values only, so the clauses are doc values range queries whose cost is the number of docs that have the field.
        create.setJsonEntity("""
            {
              "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "refresh_interval": -1,
                "index.queries.cache.everything": true
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
                bulk.append(Strings.format("{\"tag\": \"%s\", \"n\": %d}\n", tag(i % TAGS), i + 1));
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
        indexCreated = true;
    }

    /**
     * Overlapping identical searches whose cached clauses each hold a full-segment bitset either complete or are rejected by a circuit
     * breaker, never anything else, and the node survives them.
     */
    public void testConcurrentDenseClauseCachingDoesNotKillTheNode() throws Exception {
        assertClausesAreCached();
        // So that every search misses on every clause at once, rather than being answered from the bitsets the check above built.
        clearQueryCache();
        logger.info("Running search");
        assertSearchesOnlyTrippedBreakers(submit(THREAD_COUNT, () -> errorBodyOrNull(denseClauseSearch(0))));
    }

    /**
     * Like {@link #testConcurrentDenseClauseCachingDoesNotKillTheNode}, but every search has clauses of its own, so none of them can be
     * answered from, or wait for, the bitsets another search is building: each builds all of its own.
     */
    public void testConcurrentDistinctDenseClauseCachingDoesNotKillTheNode() throws Exception {
        assertClausesAreCached();
        logger.info("Running search");
        AtomicInteger nextSearch = new AtomicInteger();
        assertSearchesOnlyTrippedBreakers(
            submit(THREAD_COUNT, () -> errorBodyOrNull(denseClauseSearch(nextSearch.getAndIncrement() * CLAUSES)))
        );
    }

    /**
     * Checks the premise of the cases: a search on an idle node succeeds and gets its clauses cached. Every clause is cached on first
     * sight, so one search is enough. Without this the cases allocate next to nothing and pass on any node.
     */
    private void assertClausesAreCached() throws IOException {
        // The index is shared, so an earlier case may have left the clauses in the cache, and a hit admits nothing.
        clearQueryCache();
        long cachedBefore = cachedEntryCount();
        assertNull("a search must succeed on an idle node", errorBodyOrNull(denseClauseSearch(0)));
        assertThat(
            "the clauses must be admitted to the query cache",
            cachedEntryCount() - cachedBefore,
            greaterThanOrEqualTo((long) CLAUSES)
        );
    }

    /** Every search either succeeded or was rejected by a circuit breaker, and the node is still there afterwards. */
    private void assertSearchesOnlyTrippedBreakers(List<Future<Map<String, Object>>> futures) throws Exception {
        for (Future<Map<String, Object>> future : futures) {
            Map<String, Object> body = future.get();
            if (body != null) {
                assertTrue("expected only circuit_breaking_exception failures, but got: " + body, containsCircuitBreakingException(body));
            }
        }
        // Check that the node didn't OOM and is still alive.
        assertNodeAlive();
    }

    /** Drops every entry of the query cache of the index, so that the next search misses on every clause. */
    private static void clearQueryCache() throws IOException {
        Request clear = new Request("POST", "/" + INDEX + "/_cache/clear");
        clear.addParameter("query", "true");
        client().performRequest(clear);
    }

    /** The number of entries ever admitted to the query cache of the node. */
    private static long cachedEntryCount() throws IOException {
        Map<?, ?> nodes = (Map<?, ?>) entityAsMap(adminClient().performRequest(new Request("GET", "/_nodes/stats/indices/query_cache")))
            .get("nodes");
        Map<?, ?> node = (Map<?, ?>) nodes.values().iterator().next();
        return ((Number) XContentMapValues.extractValue("indices.query_cache.cache_count", node)).longValue();
    }

    /**
     * Filters on a range over {@code n}, as the time range of a dashboard query is, and on one of the {@code CLAUSES} values of
     * {@code tag} starting at {@code firstTag}, each a clause of its own.
     */
    private static Request denseClauseSearch(int firstTag) {
        String tags = IntStream.range(firstTag, firstTag + CLAUSES)
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
