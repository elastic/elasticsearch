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

import java.io.IOException;

/**
 * Base class for search tests of the fetch phase, with assertions that several of them share. Each subclass picks the ones that fit its
 * cluster.
 */
public abstract class FetchHeapPressureTestCase extends SearchHeapPressureTestCase {

    /**
     * Asserts that the coordinating node charges the fetched hits it keeps for the response: a search for every document of
     * {@link #indexLargeDocuments}, whose shards each fit the node alone, is rejected as a whole.
     */
    protected void assertFetchedHitsTripBreaker(String index) throws IOException {
        assertTripsOnlyWhenLarge(size -> search(index, Strings.format("""
            {"size": %d}
            """, size)), 10, LARGE_DOCS, "fetch[chunk]");
    }

    /**
     * Asserts that about 100 MB of fetched hits fit under the 153 MB limit of a 256 MB coordinating node, which they do only if each hit is
     * charged once. Sorting by {@code tag} spreads the hits over every shard, where tied scores would take them from the first shard.
     */
    protected void assertFetchedHitsUnderLimitSucceed(String index) throws IOException {
        Request search = search(index, """
            {"size": 400, "sort": ["tag"]}
            """);
        // The test client cannot buffer a response that big.
        search.addParameter("filter_path", "took,hits.total,error,status");
        assertSucceeds(search);
    }
}
