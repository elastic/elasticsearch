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
import org.elasticsearch.test.rest.SearchHeapPressureTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

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
public class HasChildHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "has-child-heap-pressure";
    private static final int PARENTS = 500_000;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").module("parent-join")
        // Pins the clause limit at 4096: 256mb of heap times 64 over the size of the search pool.
        .setting("thread_pool.search.size", "4")
        .setting("indices.queries.cache.size", "0%")
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    /** {@link #PARENTS} parents with one child each, in one segment: parent {@code i} and then its child. */
    @Before
    public void indexDocuments() throws IOException {
        indexBulkEntries(INDEX, 1, """
            {
              "join": {"type": "join", "relations": {"parent": "child"}},
              "n": {"type": "long"}
            }""", 2 * PARENTS, i -> {
            int parent = i / 2;
            return i % 2 == 0 ? Strings.format("""
                {"index":{"_id":"p%d"}}
                {"join":"parent"}""", parent) : Strings.format("""
                {"index":{"routing":"p%d"}}
                {"join":{"name":"child","parent":"p%d"},"n":%d}""", parent, parent, parent);
        });
    }

    /** Each clause reaches nearly every parent and keeps a bit per parent. */
    public void testHasChildClausesWithoutScores() throws IOException {
        assertSucceedsOrTripsWhenLarge(n -> hasChildSearch(n, "none"), 20, 4000);
    }

    /** Each clause reaches nearly every parent and keeps a float and an int per parent. */
    public void testHasChildClausesWithAvgScores() throws IOException {
        assertSucceedsOrTripsWhenLarge(n -> hasChildSearch(n, "avg"), 2, 100);
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
        return search(INDEX, Strings.format("""
            {"size": 0, "track_total_hits": true, "query": {"bool": {"should": [%s]}}}
            """, clauses));
    }
}
