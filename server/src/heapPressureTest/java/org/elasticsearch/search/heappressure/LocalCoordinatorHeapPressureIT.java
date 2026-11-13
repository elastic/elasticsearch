/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

/**
 * Like {@link CoordinatorHeapPressureIT}, but the coordinating node holds the shards itself. It delivers their fetch results to itself
 * without the network, so the hits it keeps point into the pages they were written to, and it must charge them to its request breaker
 * all the same. Each of the {@link #SHARDS} shards fits the node alone, so only their sum trips it.
 */
public class LocalCoordinatorHeapPressureIT extends FetchHeapPressureTestCase {

    private static final String INDEX = "local-coordinator";
    // More shards run the 256 MB node out of memory while the documents are indexed.
    private static final int SHARDS = 2;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
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
     * About 100 MB of hits fit under the 153 MB limit if they are charged once, although they sit both in the pages the node wrote them to
     * and in the hits it keeps for the response.
     */
    public void testFetchedHitsUnderLimitSucceed() throws IOException {
        assertFetchedHitsUnderLimitSucceed(INDEX);
    }
}
