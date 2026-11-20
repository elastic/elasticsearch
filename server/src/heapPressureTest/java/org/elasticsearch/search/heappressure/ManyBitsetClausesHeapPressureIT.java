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
import org.elasticsearch.test.rest.SearchHeapPressureTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.function.IntFunction;

/**
 * Clauses other than point ranges and multi-term queries that build a full-segment bit set per clause while the scorers of a
 * {@code bool} are created.
 *
 * <p>{@link BoolQueryHeapPressureIT#testRangeClausesTripBreaker} covers {@code PointRangeQuery}. These cases use the same shape, one
 * {@code bool} of {@link #MANY_CLAUSES} clauses that each match most of a single segment of {@link #DOCS} documents, about 62kb of bit
 * set per clause and more than the heap in total, with queries that build their bit set from points through a different Lucene query:
 * <ul>
 * <li>{@code terms} on {@code ip} and {@code long}, which is a bare {@code PointInSetQuery};
 * <li>{@code geo_distance} on {@code geo_point}, a {@code LatLonPointDistanceQuery};
 * <li>{@code geo_shape} with a polygon on {@code geo_point}, a {@code LatLonPointQuery};
 * <li>{@code range} on a {@code long_range} field, a {@code RangeFieldQuery}.
 * </ul>
 * The query cache is off so that every clause is built. Every case must be rejected under {@code points-execution}. Each case should
 * be run on its own, since a case that kills the node skips the rest.
 */
public class ManyBitsetClausesHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "many-bitset-clauses-heap-pressure";
    private static final int DOCS = 500_000;
    // Under the clause limit of 4096 that a 256mb node with 4 search threads has, see BoolQueryHeapPressureIT.
    private static final int MANY_CLAUSES = 4000;
    private static final int FEW_CLAUSES = 50;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").setting("thread_pool.search.size", "4")
        .setting("indices.queries.cache.size", "0%")
        .build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void indexDocuments() throws IOException {
        indexDocuments(
            INDEX,
            1,
            """
                {
                  "ip": {"type": "ip"},
                  "n": {"type": "long"},
                  "loc": {"type": "geo_point"},
                  "r": {"type": "long_range"}
                }""",
            DOCS,
            i -> Strings.format(
                """
                    {"ip":"10.0.0.%d","n":%d,"loc":{"lat":%s,"lon":%s},"r":{"gte":%d,"lte":%d}}""",
                1 + i % 2,
                i % 2,
                coordinate(i * 7919L),
                coordinate(i * 104729L),
                i,
                i + 10
            )
        );
    }

    /** {@code terms} on {@code ip}: each clause matches half the documents. */
    public void testIpTermsClauses() throws IOException {
        assertTripsUnderPointsLabel(n -> search(INDEX, searchBody(should(n, i -> Strings.format("""
            {"terms": {"ip": ["10.0.0.1", "172.16.%d.%d"]}}""", i / 256, i % 256)))));
    }

    /** {@code terms} on {@code long}: each clause matches half the documents. */
    public void testLongTermsClauses() throws IOException {
        assertTripsUnderPointsLabel(n -> search(INDEX, searchBody(should(n, i -> Strings.format("""
            {"terms": {"n": [0, %d]}}""", 1000 + i)))));
    }

    /** {@code geo_distance}: each clause matches about two thirds of the documents. */
    public void testGeoDistanceClauses() throws IOException {
        assertTripsUnderPointsLabel(n -> search(INDEX, searchBody(should(n, i -> Strings.format("""
            {"geo_distance": {"distance": "%dm", "loc": {"lat": 0, "lon": 0}}}""", 1_000_000 + i)))));
    }

    /**
     * {@code geo_shape} with a polygon on {@code geo_point}: each clause matches nearly every document, but not the bounding box of the
     * segment, which Lucene would answer without a bit set.
     */
    public void testGeoShapeOnGeoPointClauses() throws IOException {
        assertTripsUnderPointsLabel(n -> search(INDEX, searchBody(should(n, i -> {
            String top = Double.toString(9.0 - i * 0.0001);
            return Strings.format("""
                {"geo_shape": {"loc": {"relation": "intersects", "shape": {"type": "polygon", "coordinates": [[
                  [-10.5, -10.5], [10.5, -10.5], [10.5, %s], [-10.5, %s], [-10.5, -10.5]
                ]]}}}}""", top, top);
        }))));
    }

    /** {@code range} on {@code long_range}: each clause intersects nearly every document. */
    public void testLongRangeFieldClauses() throws IOException {
        assertTripsUnderPointsLabel(n -> search(INDEX, searchBody(should(n, i -> Strings.format("""
            {"range": {"r": {"gte": %d, "lte": 10000000}}}""", 1000 + i)))));
    }

    private void assertTripsUnderPointsLabel(IntFunction<Request> request) throws IOException {
        assertTripsOnlyWhenLarge(request, FEW_CLAUSES, MANY_CLAUSES, "points-execution");
    }

    /** A coordinate in [-10, 10) spread over the documents by {@code seed}. */
    private static String coordinate(long seed) {
        return Double.toString((seed % 2000) / 100.0 - 10.0);
    }

    private static String searchBody(String boolQuery) {
        return Strings.format("""
            {"size": 0, "track_total_hits": true, "query": %s}
            """, boolQuery);
    }

    private static String should(int count, IntFunction<String> clause) {
        StringBuilder clauses = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                clauses.append(',');
            }
            clauses.append(clause.apply(i));
        }
        return Strings.format("""
            {"bool": {"should": [%s]}}""", clauses);
    }
}
