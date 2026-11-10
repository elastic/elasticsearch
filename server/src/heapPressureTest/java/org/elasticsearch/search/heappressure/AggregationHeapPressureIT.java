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
 * A {@code top_hits} under a bucket per document holds a hit per document until the shard result is built. The index has a single shard, so
 * one aggregator collects every document.
 */
public class AggregationHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "aggregations";

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void indexLargeDocuments() throws IOException {
        indexLargeDocuments(INDEX, 1, SOURCE_ONLY_KEYWORD, "x".repeat(DOC_BYTES));
    }

    /** {@code top_hits} charges the hits its buckets keep, so a bucket per document returning its source is rejected. */
    public void testTopHitsSourcesTripBreaker() throws IOException {
        assertTripsOnlyWhenLarge(tags -> search(INDEX, topHitsPerTag(tags, "true")), 10, LARGE_DOCS, "<agg [top]>");
    }

    /**
     * Each bucket loads the whole {@code _source} to return {@code tag}, so the hits are small and fit the breaker. The source of the last
     * fetch must not stay on the heap for every bucket, uncharged, until the request ends (see #159231).
     */
    public void testTopHitsDoesNotRetainSourcePerBucket() throws IOException {
        assertSucceeds(search(INDEX, topHitsPerTag(LARGE_DOCS, """
            {"includes": ["tag"]}""")));
    }
}
