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
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The coordinating node gathers the results of every shard before it builds the response, so it can hold far more than any shard and must
 * charge it to its own request breaker. Requests go to a small coordinating-only node in front of a data node big enough that no
 * shard-level charge trips first. Each of the {@link #SHARDS} shards fits the coordinator alone, so only their sum trips it.
 */
public class CoordinatorHeapPressureIT extends FetchHeapPressureTestCase {

    private static final String INDEX = "coordinator";
    private static final int SHARDS = 4;
    private static final int COORDINATOR = 0;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedCluster().withNode(
        node -> node.name("coordinator").setting("node.roles", "[]").jvmArg("-Xms256m").jvmArg("-Xmx256m")
    ).withNode(node -> node.name("data").setting("node.roles", "[master, data]").jvmArg("-Xms1g").jvmArg("-Xmx1g")).build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddress(COORDINATOR);
    }

    @Before
    public void indexLargeDocuments() throws IOException {
        indexLargeDocuments(INDEX, SHARDS, SOURCE_ONLY_KEYWORD, "x".repeat(DOC_BYTES));
    }

    /** The fetch results of all shards together do not fit. The request fails as a whole rather than return the hits that did. */
    public void testFetchedHitsTripBreaker() throws IOException {
        assertFetchedHitsTripBreaker(INDEX);
    }

    /**
     * About 100 MB of hits fit under the 153 MB limit if they are charged once, not on arrival and again when merged into the response.
     */
    public void testFetchedHitsUnderLimitSucceed() throws IOException {
        assertFetchedHitsUnderLimitSucceed(INDEX);
    }

    /** Every bucket comes from a different shard, so the reduce keeps all of their hits. */
    public void testTopHitsReduceTripsBreaker() throws IOException {
        assertTripsOnlyWhenLarge(tags -> search(INDEX, topHitsPerTag(tags, "true")), 10, LARGE_DOCS, "<reduce_aggs>");
    }

    /**
     * Each search fits on its own, but multi-search holds every response until the last one completes. Buffering a response fails the whole
     * multi-search, while fetching the hits of a search still running fails only that search, so either label can trip.
     */
    public void testMultiSearchResponsesTripBreaker() throws IOException {
        assertMultiSearchSucceeds(multiSearch(2, 50));
        assertCircuitBreaksInPart(multiSearch(20, 50), "msearch[response]", "fetch[chunk]");
    }

    /**
     * About 105 MB of hits across the searches fit if each buffered hit is charged once. Running one search at a time keeps the hits in
     * flight out of the sum. A buffered response already holds the fetch charge for its hits, so multi-search must not charge them again.
     */
    public void testMultiSearchResponsesUnderLimitSucceed() throws IOException {
        Request multiSearch = multiSearch(8, 50);
        multiSearch.addParameter("max_concurrent_searches", "1");
        assertMultiSearchSucceeds(multiSearch);
    }

    /** Multi-search reports a failed search in its response rather than failing the request. */
    private void assertMultiSearchSucceeds(Request multiSearch) throws IOException {
        List<?> responses = (List<?>) entityAsMap(client().performRequest(multiSearch)).get("responses");
        for (Object response : responses) {
            assertEquals("expected every search to succeed, but got: " + response, 200, ((Map<?, ?>) response).get("status"));
        }
        assertNodeAlive();
    }

    private static Request multiSearch(int searches, int size) {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < searches; i++) {
            body.append(Strings.format("""
                {"index": "%s", "request_cache": false}
                {"size": %d, "from": %d}
                """, INDEX, size, i * size % (LARGE_DOCS - size)));
        }
        Request request = new Request("POST", "/_msearch");
        // The test client cannot buffer them all.
        request.addParameter("filter_path", "responses.status,responses.error");
        request.setJsonEntity(body.toString());
        return request;
    }
}
