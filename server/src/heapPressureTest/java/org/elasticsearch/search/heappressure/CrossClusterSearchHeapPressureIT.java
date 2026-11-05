/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;

/**
 * With {@code ccs_minimize_roundtrips}, each cluster returns a full response with its hits, and the coordinating node holds all of them
 * until the slowest cluster answers. Requests go to a small local node in front of a remote cluster big enough to serve them.
 *
 * <p>Each search covers the remote index and a one-document local index whose pausable field {@code p} is fetched. With the field blocked,
 * the local part of every search parks, and the local node holds each remote response until its search completes. Together they hold
 * more than the local node's heap. Charged to the breaker, the excess searches are rejected; uncharged, the node runs out of memory.
 *
 * <p>{@link #testHeldRemoteResponsesTripBreaker} sends each search once the previous response has started to be charged, or was
 * rejected. Only the merge holds a response until its search completes, so the test checks that every response the merge holds is
 * charged. {@link #testRemoteResponsesArrivingAtOnceTripBreaker} sends them all at once, so many responses wait to be read at the same
 * time, and checks that the node survives them.
 */
public class CrossClusterSearchHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String REMOTE_INDEX = "ccs-remote";
    private static final String PAUSE_INDEX = "ccs-pause";
    // 30 hits of 256 KB, about 7.5 MB per remote response: about 20 of them fill the 153 MB limit of the local node, and all of them
    // together are more than its heap.
    private static final int HITS_PER_SEARCH = 30;
    private static final int SEARCHES = 40;
    private static final long RESPONSE_BYTES = (long) HITS_PER_SEARCH * DOC_BYTES;

    private static final ElasticsearchCluster REMOTE = ElasticsearchCluster.local()
        .name("remote")
        .nodes(1)
        .jvmArg("-Xms1g")
        .jvmArg("-Xmx1g")
        .build();

    private static final ElasticsearchCluster LOCAL = heapConstrainedNode("256m").name("local")
        .module("test-pausable-field")
        // Parks one search thread per search.
        .setting("thread_pool.search.size", "64")
        .setting("cluster.remote.remote.seeds", () -> "\"" + REMOTE.getTransportEndpoint(0) + "\"")
        // A trip fails the search even though the cluster can be skipped.
        .setting("cluster.remote.remote.skip_unavailable", "true")
        .build();

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(REMOTE).around(LOCAL);

    @Override
    protected ElasticsearchCluster cluster() {
        return LOCAL;
    }

    @Before
    public void createIndices() throws IOException {
        try (RestClient remote = restClient(REMOTE, 1)) {
            indexLargeDocuments(remote, REMOTE_INDEX, 1, SOURCE_ONLY_KEYWORD, "x".repeat(DOC_BYTES));
        }
        if (indexExists(PAUSE_INDEX) == false) {
            Request create = new Request("PUT", "/" + PAUSE_INDEX);
            create.setJsonEntity("""
                {
                  "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                  "mappings": {"runtime": {"p": {"type": "long", "script": {"source": "", "lang": "pause"}}}}
                }
                """);
            client().performRequest(create);
            Request doc = new Request("POST", "/" + PAUSE_INDEX + "/_doc");
            doc.addParameter("refresh", "true");
            doc.setJsonEntity("{}");
            client().performRequest(doc);
        }
    }

    /** One remote response fits, as do the hits it keeps until the response is written. */
    public void testSearchUnderLimitSucceeds() throws IOException {
        assertSucceeds(search());
    }

    /**
     * Searches that each hold a remote response on the local node at the same time either complete or are rejected by the breaker. At
     * least one is rejected, which shows that they overlapped, and at least one completes. The local node survives them.
     */
    public void testHeldRemoteResponsesTripBreaker() throws Exception {
        List<Future<Map<String, Object>>> futures = new ArrayList<>(SEARCHES);
        ExecutorService pool = Executors.newFixedThreadPool(SEARCHES);
        // The default client allows 10 connections per node, too few to hold all the searches in flight.
        try (RestClient searchClient = restClient(LOCAL, SEARCHES)) {
            blockPauseField();
            try {
                for (int i = 0; i < SEARCHES; i++) {
                    long charged = requestBreakerUsedBytes();
                    long tripped = trippedCount();
                    Future<Map<String, Object>> future = pool.submit(() -> errorBodyOrNull(searchClient, search()));
                    futures.add(future);
                    // Charged or rejected. A search that ends before the field is unblocked failed some other way, which is checked below.
                    // Half a response: more than any unrelated charge, less than the response's own.
                    assertBusy(
                        () -> assertTrue(
                            "the remote response was neither charged nor rejected",
                            requestBreakerUsedBytes() >= charged + RESPONSE_BYTES / 2 || trippedCount() > tripped || future.isDone()
                        ),
                        60,
                        TimeUnit.SECONDS
                    );
                }
            } finally {
                unblockPauseField();
            }
            assertCompletedOrRejected(futures);
        } finally {
            pool.shutdown();
        }
        assertNodeAlive();
    }

    /**
     * Sent all at once, many remote responses arrive together and wait to be read. Each search either completes or is rejected by the
     * breaker, and the local node survives.
     */
    public void testRemoteResponsesArrivingAtOnceTripBreaker() throws Exception {
        List<Future<Map<String, Object>>> futures;
        try (RestClient searchClient = restClient(LOCAL, SEARCHES)) {
            // The node's trip count only grows, so wait for a trip from these searches.
            long trippedBefore = trippedCount();
            blockPauseField();
            try {
                futures = submit(SEARCHES, () -> errorBodyOrNull(searchClient, search()));
                assertBusy(() -> assertThat(trippedCount(), greaterThan(trippedBefore)), 60, TimeUnit.SECONDS);
            } finally {
                unblockPauseField();
            }
            assertCompletedOrRejected(futures);
        }
        assertNodeAlive();
    }

    /**
     * Asserts that each search either completed or was rejected under {@code fetch[ccs]}, and that some did each.
     */
    private void assertCompletedOrRejected(List<Future<Map<String, Object>>> futures) throws Exception {
        int completed = 0;
        int rejected = 0;
        for (Future<Map<String, Object>> future : futures) {
            Map<String, Object> body = future.get();
            if (body == null) {
                completed++;
            } else {
                assertTrue(
                    "expected only circuit_breaking_exception failures, but got: " + body,
                    containsCircuitBreakingException(body, "data for [fetch[ccs]]")
                );
                rejected++;
            }
        }
        logger.info("{} of {} searches rejected by the breaker", rejected, futures.size());
        // Some fit, so the breaker isn't rejecting every search, as a leaked or inflated charge would.
        assertThat(completed, greaterThan(0));
        assertThat(rejected, greaterThan(0));
    }

    private static Request search() {
        Request search = search(PAUSE_INDEX + ",remote:" + REMOTE_INDEX, Strings.format("""
            {"size": %d, "fields": ["p"]}
            """, HITS_PER_SEARCH));
        // Each cluster sends back a full response, which is what the local node holds.
        search.addParameter("ccs_minimize_roundtrips", "true");
        // The test client does not need the hits, but the local node still holds every one of them until it renders the response.
        search.addParameter("filter_path", "took,hits.total,error,status");
        return search;
    }

    private static RestClient restClient(ElasticsearchCluster cluster, int connections) {
        return RestClient.builder(HttpHost.create(cluster.getHttpAddress(0)))
            .setHttpClientConfigCallback(b -> b.setMaxConnPerRoute(connections).setMaxConnTotal(connections))
            .build();
    }

    private static Map<String, Object> errorBodyOrNull(RestClient restClient, Request request) throws IOException {
        try {
            restClient.performRequest(request);
            return null;
        } catch (ResponseException e) {
            return entityAsMap(e.getResponse());
        }
    }
}
