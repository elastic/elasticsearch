/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.SearchHeapPressureTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;

/**
 * A {@code bool} of many {@code bitmap_terms} clauses must not be able to take a node down.
 *
 * <p>On a field indexed with points, in a segment that is not sorted on it, {@code bitmap_terms} collects the matching documents of a
 * segment into a {@code DocIdSetBuilder}, which is a bit set of {@code maxDoc} bits once a clause matches more than 1/128 of the segment.
 * The scorers of all the clauses of a {@code bool} are created together, so a request holds the bit sets of all of them, about 62kb each
 * for {@link #DOCS} documents and more than the heap for {@link #MANY_CLAUSES} clauses, and the request circuit breaker sees none of it.
 * The case passes if the request either succeeds or is rejected by a circuit breaker and the node survives.
 */
public class BitmapTermsHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "bitmap-terms-heap-pressure";
    private static final int DOCS = 500_000;
    // Under the clause limit of 4096 that a 256mb node with 4 search threads has.
    private static final int MANY_CLAUSES = 4000;
    private static final int FEW_CLAUSES = 50;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").module("bitmap")
        .setting("thread_pool.search.size", "4")
        .setting("indices.queries.cache.size", "0%")
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    /** {@link #DOCS} documents in one segment, half with {@code v} 0 and half with 1. */
    @Before
    public void indexDocuments() throws IOException {
        indexDocuments(INDEX, 1, """
            {"v": {"type": "integer"}}""", DOCS, i -> Strings.format("""
            {"v":%d}""", i % 2));
    }

    /** Each clause matches half the documents. */
    public void testBitmapTermsClauses() throws IOException {
        assertSucceedsOrTripsWhenLarge(BitmapTermsHeapPressureIT::bitmapTermsSearch, FEW_CLAUSES, MANY_CLAUSES);
    }

    /** A {@code bool} of {@code count} clauses, each with a different bitmap so that they are not merged. */
    private static Request bitmapTermsSearch(int count) {
        StringBuilder clauses = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                clauses.append(',');
            }
            clauses.append(Strings.format("""
                {"bitmap_terms": {"field": "v", "value": "%s"}}""", bitmap(0, 1000 + i)));
        }
        return search(INDEX, Strings.format("""
            {"size": 0, "track_total_hits": true, "query": {"bool": {"should": [%s]}}}
            """, clauses));
    }

    private static String bitmap(int... values) {
        RoaringBitmap bitmap = RoaringBitmap.bitmapOf(values);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            bitmap.serialize(out);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Base64.getEncoder().encodeToString(bytes.toByteArray());
    }
}
