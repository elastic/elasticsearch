/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.join.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.HeapPressureTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * A {@code bool} of many {@code has_child} clauses must not be able to take a node down.
 *
 * <p>{@code has_child} is rewritten into a Lucene global ordinals join, which runs the child query on a plain {@code IndexSearcher}, so
 * nothing it allocates is charged to the request circuit breaker, and keeps what it collected until the request ends:
 * <ul>
 * <li>with {@code score_mode: none}, a {@code LongBitSet} of one bit per parent, about 62kb for {@link #PARENTS} parents;
 * <li>with a scoring {@code score_mode} such as {@code avg}, a float and an int per parent the child query reaches, about 4mb.
 * </ul>
 * Every clause is rewritten before the search starts, so a request holds what all of them collected at once, and with enough clauses more
 * than the heap. A case passes if the request either succeeds or is rejected by a circuit breaker and the node survives. Each case should
 * be run on its own, since a case that kills the node leaves the rest without one.
 */
public class HasChildHeapPressureIT extends HeapPressureTestCase {

    private static final String INDEX = "has-child-heap-pressure";
    private static final int PARENTS = 500_000;
    private static final int DOCS_PER_BULK = 10_000;

    private static boolean indexCreated;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .module("parent-join")
        .setting("xpack.security.enabled", "false")
        // Pins the clause limit at 4096: 256mb of heap times 64 over the size of the search pool.
        .setting("thread_pool.search.size", "4")
        .setting("indices.queries.cache.size", "0%")
        // Only charged bytes count towards the parent breaker, see SearchHeapPressureTestCase.
        .setting("indices.breaker.total.use_real_memory", "false")
        .jvmArg("-XX:+ExitOnOutOfMemoryError")
        .jvmArg("-XX:-HeapDumpOnOutOfMemoryError")
        .jvmArg("-Xms256m")
        .jvmArg("-Xmx256m")
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    /** {@link #PARENTS} parents with one child each, in one segment. */
    @Before
    public void indexDocuments() throws IOException {
        if (indexCreated) {
            return;
        }
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("""
            {
              "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": -1},
              "mappings": {
                "properties": {
                  "join": {"type": "join", "relations": {"parent": "child"}},
                  "n": {"type": "long"}
                }
              }
            }""");
        client().performRequest(create);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < PARENTS; i++) {
            bulk.append(Strings.format("""
                {"index":{"_id":"p%d"}}
                {"join":"parent"}
                {"index":{"routing":"p%d"}}
                {"join":{"name":"child","parent":"p%d"},"n":%d}
                """, i, i, i, i));
            if ((i + 1) % DOCS_PER_BULK == 0 || i == PARENTS - 1) {
                Request request = new Request("POST", "/" + INDEX + "/_bulk");
                request.setJsonEntity(bulk.toString());
                assertFalse((Boolean) entityAsMap(client().performRequest(request)).get("errors"));
                bulk.setLength(0);
                // Test clusters run with assertions, and the translog then keeps every operation until it is rolled.
                client().performRequest(new Request("POST", "/" + INDEX + "/_flush"));
            }
        }
        Request forceMerge = new Request("POST", "/" + INDEX + "/_forcemerge");
        forceMerge.addParameter("max_num_segments", "1");
        client().performRequest(forceMerge);
        client().performRequest(new Request("POST", "/" + INDEX + "/_refresh"));
        indexCreated = true;
        logger.info("Indexed {} parents and {} children", PARENTS, PARENTS);
    }

    /** Each clause reaches nearly every parent and keeps a bit per parent. */
    public void testHasChildClausesWithoutScores() throws IOException {
        assertOnlyTripsOrSucceeds(n -> hasChildSearch(n, "none"), 20, 4000);
    }

    /** Each clause reaches nearly every parent and keeps a float and an int per parent. */
    public void testHasChildClausesWithAvgScores() throws IOException {
        assertOnlyTripsOrSucceeds(n -> hasChildSearch(n, "avg"), 2, 100);
    }

    private void assertOnlyTripsOrSucceeds(IntFunction<Request> request, int small, int large) throws IOException {
        assertNull("expected the request to succeed", errorBodyOrNull(request.apply(small)));
        assertNodeAlive();
        logger.info("Searching for {} clauses", large);
        Map<String, Object> body = errorBodyOrNull(request.apply(large));
        if (body != null) {
            assertTrue("expected only a circuit_breaking_exception, but got: " + body, containsCircuitBreakingException(body));
            logger.info("rejected by a circuit breaker: {}", body);
        } else {
            logger.info("succeeded");
        }
        assertNodeAlive();
    }

    /** A {@code bool} of {@code count} {@code has_child} clauses, each with a different child query so that they are not merged. */
    private static Request hasChildSearch(int count, String scoreMode) {
        StringBuilder clauses = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                clauses.append(',');
            }
            clauses.append(Strings.format("""
                {"has_child": {"type": "child", "score_mode": "%s", "query": {"range": {"n": {"gte": %d}}}}}""", scoreMode, i));
        }
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.addParameter("request_cache", "false");
        search.setJsonEntity(Strings.format("""
            {"size": 0, "track_total_hits": true, "query": {"bool": {"should": [%s]}}}
            """, clauses));
        return search;
    }
}
