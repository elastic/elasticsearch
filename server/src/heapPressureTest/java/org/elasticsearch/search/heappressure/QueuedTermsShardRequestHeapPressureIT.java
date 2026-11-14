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
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A data node that receives shard requests over the transport layer holds the {@code terms} values it decodes from them while the
 * requests wait in its search queue. Decoded, a numeric value is a boxed number and a reference, several times its serialized size, so a
 * few overlapping searches whose clauses each stay under {@code index.max_terms_count} can fill the node's heap before a shard runs any
 * of them. The decoded values must be charged as the requests are read, so that the breaker rejects the requests instead.
 *
 * <p>The searches carry many {@code terms} clauses of about 49,000 numbers each and go through a coordinating-only node, so every shard
 * request reaches the data node over the transport layer, as it does from another node of the cluster. The data node's search threads
 * are parked, so the requests it reads wait in its queue, and only the charge made while they are read can protect it.
 */
public class QueuedTermsShardRequestHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "queued-terms";
    private static final String PAUSE_INDEX = "queued-terms-pause";
    private static final int SHARDS = 4;
    private static final int DOCS = 1000;
    private static final int SEARCH_THREADS = 2;
    private static final int VALUES_PER_CLAUSE = 49_000;
    // About 78 MB decoded per search on the data node, and about 31 MB of JSON, under the default http.max_content_length of 100 MB.
    private static final int CLAUSES = 80;
    // With the paused searches, the connections the test client opens to a node, or the searches would not overlap. Waiting together,
    // they decode to more than the data node's heap.
    private static final int HEAVY_SEARCHES = RestClientBuilder.DEFAULT_MAX_CONN_PER_ROUTE - SEARCH_THREADS;
    // Less than what one heavy search decodes to beyond its serialized size: with compressed references, a boxed Integer and its reference
    // take about 20 bytes, for 5 on the wire. Far more than the parked searches charge, so only a heavy search read by the data node can
    // account for it.
    private static final long ONE_SEARCH_DECODED_BYTES = (long) CLAUSES * VALUES_PER_CLAUSE * 10;
    // Above the Integer cache, so that every decoded value is its own object, as in production.
    private static final int FIRST_VALUE = 1_000_000;

    /** Parks a search thread of the data node on the pause field until it is unblocked. */
    private static final String PAUSED_SEARCH = """
        {
          "size": 0,
          "runtime_mappings": {"p": {"type": "long", "script": {"source": "", "lang": "pause"}}},
          "query": {"bool": {"filter": [{"term": {"p": 1}}]}}
        }
        """;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedCluster().nodes(2)
        .module("test-pausable-field")
        // Coordinating only, with a heap large enough that its own charge for the parsed searches does not reject them first. It charges
        // about 126 MB to parse each heavy search, so about 1 GB for all of them, which a 2 GB heap's request breaker sometimes rejects.
        .node(0, n -> n.setting("node.roles", "[]").jvmArg("-Xms3g").jvmArg("-Xmx3g"))
        // Holds every shard, with a few search threads, so that shard requests wait in the queue with their decoded values.
        .node(1, n -> n.setting("thread_pool.search.size", Integer.toString(SEARCH_THREADS)).jvmArg("-Xms512m").jvmArg("-Xmx512m"))
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Override
    protected String getTestRestCluster() {
        // Every search goes through the coordinating node, which holds no shards.
        return cluster.getHttpAddress(0);
    }

    /**
     * Searches parked on the pause field take every search thread of the data node, so the heavy searches wait in its search queue with
     * their decoded {@code terms} values, which together are more than its heap. The breaker must reject some of them as they are read,
     * the rest either complete or are rejected by a circuit breaker once unblocked, and the data node survives.
     */
    public void testQueuedSearchesWithLargeNumericTermsDoNotKillTheDataNode() throws Exception {
        createIndices();
        String heavy = heavySearch();
        // A search that a node with no other load handles, so that the test cannot pass on a node that rejects every such search.
        assertThat(circuitBreakerRejectedShards(search(INDEX, heavy), SHARDS), equalTo(0));

        long trippedBefore = breakerTrips(true);
        long coordinatorTrippedBefore = breakerTrips(false);
        List<Future<Map<String, Object>>> paused;
        List<Future<Integer>> held;
        blockPauseField();
        try {
            paused = submit(SEARCH_THREADS, () -> errorBodyOrNull(search(PAUSE_INDEX, PAUSED_SEARCH)));
            assertBusy(() -> assertThat(parkedCount(), equalTo(SEARCH_THREADS)));
            held = submit(HEAVY_SEARCHES, () -> circuitBreakerRejectedShards(search(INDEX, heavy), SHARDS));
            // Each heavy search is rejected, or its node request has forked one shard task per search thread into the queue: the batched
            // query phase runs min(max_concurrent_shard_requests, shards, search threads) of them at a time, and SHARDS is at least
            // SEARCH_THREADS, which is under the default max_concurrent_shard_requests of 5.
            assertBusy(
                () -> assertThat(completed(held) + dataNodeQueuedTasks() / SEARCH_THREADS, equalTo(HEAVY_SEARCHES)),
                120,
                TimeUnit.SECONDS
            );
            // Every search thread is parked, so only reading the heavy searches can have charged or tripped the data node's breakers.
            long charged = dataNodeRequestBreakerUsed();
            logger.info("[{}] bytes charged to the data node's request breaker while waiting", charged);
            assertThat("decoded terms are charged while the searches wait", charged, greaterThan(ONE_SEARCH_DECODED_BYTES));
            assertThat("a breaker rejected a search before any shard ran", breakerTrips(true), greaterThan(trippedBefore));
        } finally {
            unblockPauseField();
        }
        for (Future<Integer> search : held) {
            search.get(5, TimeUnit.MINUTES);
        }
        for (Future<Map<String, Object>> search : paused) {
            assertNull(search.get(5, TimeUnit.MINUTES));
        }
        assertNodeAlive();
        assertThat(
            "the coordinating node rejected nothing, so every rejection was the data node's",
            breakerTrips(false),
            equalTo(coordinatorTrippedBefore)
        );
    }

    private static String heavySearch() {
        String clause = Strings.format(
            "{\"terms\": {\"n\": [%s]}}",
            IntStream.range(0, VALUES_PER_CLAUSE).mapToObj(i -> Integer.toString(FIRST_VALUE + i)).collect(Collectors.joining(","))
        );
        String clauses = IntStream.range(0, CLAUSES).mapToObj(i -> clause).collect(Collectors.joining(","));
        return Strings.format("""
            {"size": 0, "query": {"bool": {"should": [%s]}}}""", clauses);
    }

    private static int completed(List<? extends Future<?>> futures) {
        return (int) futures.stream().filter(Future::isDone).count();
    }

    private static int dataNodeQueuedTasks() throws IOException {
        return ((Number) XContentMapValues.extractValue("thread_pool.search.queue", nodeStats(true, "thread_pool"))).intValue();
    }

    /**
     * Trips of any breaker of the data node, or of the coordinating node: the request breaker, or the parent once the charges of every
     * breaker reach its limit.
     */
    private static long breakerTrips(boolean dataNode) throws IOException {
        long tripped = 0;
        for (Object breaker : ((Map<?, ?>) nodeStats(dataNode, "breaker").get("breakers")).values()) {
            tripped += ((Number) ((Map<?, ?>) breaker).get("tripped")).longValue();
        }
        return tripped;
    }

    private static long dataNodeRequestBreakerUsed() throws IOException {
        return ((Number) XContentMapValues.extractValue("breakers.request.estimated_size_in_bytes", nodeStats(true, "breaker")))
            .longValue();
    }

    /** The {@code metric} stats of the data node, the only node with data roles, or of the coordinating node, the only one without. */
    private static Map<?, ?> nodeStats(boolean dataNode, String metric) throws IOException {
        // A filter that only removes nodes starts from none, so the coordinating node is all the nodes but the data ones.
        Request request = new Request("GET", "/_nodes/" + (dataNode ? "data:true" : "_all,data:false") + "/stats/" + metric);
        Map<?, ?> nodes = (Map<?, ?>) entityAsMap(adminClient().performRequest(request)).get("nodes");
        assertThat(nodes.size(), equalTo(1));
        return (Map<?, ?>) nodes.values().iterator().next();
    }

    private void createIndices() throws IOException {
        indexDocuments(INDEX, SHARDS, """
            {"n": {"type": "integer"}}""", DOCS, i -> Strings.format("{\"n\": %d}", FIRST_VALUE + i));
        // One shard, so each paused search parks exactly one search thread.
        indexDocuments(PAUSE_INDEX, 1, """
            {"n": {"type": "integer"}}""", 1, i -> "{\"n\": 1}");
    }
}
