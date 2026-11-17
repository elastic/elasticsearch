/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.heappressure;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.HeapPressureTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * A {@code bool} of many {@code geo_shape} clauses on a {@code geo_shape} field must not be able to take a node down, however few documents
 * each clause matches.
 *
 * <p>Lucene's {@code SpatialQuery} answers some relations with a bit set of {@code maxDoc} bits whatever the clause matches: always for
 * {@code contains}, and for {@code intersects} when the shapes of the field have more than four triangles per document on average.
 * The scorers of all the clauses of a {@code bool} are created together, so a request holds the bit sets of all of them, about 62kb each
 * for {@link #DOCS} documents, and the request circuit breaker sees none of it. The clauses here are selective, so that only the size of
 * the segment, not what they match, makes the request too large for the heap. A case passes if the request either succeeds or is rejected
 * by a circuit breaker and the node survives. Each case should be run on its own, since a case that kills the node leaves the rest
 * without one.
 */
public class GeoShapeHeapPressureIT extends HeapPressureTestCase {

    private static final String INDEX = "geo-shape-heap-pressure";
    private static final int DOCS = 500_000;
    private static final int DOCS_PER_BULK = 10_000;
    /** Half the width of the octagon of each document, in degrees. */
    private static final double RADIUS = 0.005;

    private static boolean indexCreated;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .module("spatial")
        .module("legacy-geo")
        .module("lang-painless")
        .setting("xpack.security.enabled", "false")
        // Pins the clause limit at 4096: 256mb of heap times 64 over the size of the search pool.
        .setting("thread_pool.search.size", "4")
        .setting("indices.queries.cache.size", "0%")
        // Only charged bytes count towards the parent breaker, so that the test sees the accounting gap.
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

    /** {@link #DOCS} small disjoint octagons, six triangles each, over 20 by 20 degrees, in one segment. */
    @Before
    public void indexDocuments() throws IOException {
        if (indexCreated) {
            return;
        }
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("""
            {
              "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": -1},
              "mappings": {"properties": {"shape": {"type": "geo_shape"}}}
            }""");
        client().performRequest(create);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < DOCS; i++) {
            bulk.append("{\"index\":{}}\n").append(Strings.format("{\"shape\":\"%s\"}\n", octagon(lat(i), lon(i))));
            if ((i + 1) % DOCS_PER_BULK == 0 || i == DOCS - 1) {
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
        logger.info("Indexed {} documents", DOCS);
    }

    /** Each clause is a point that the octagon of one document contains, and builds two bit sets of which it keeps one. */
    public void testContainsPointClauses() throws IOException {
        assertOnlyTripsOrSucceeds(n -> search(n, i -> Strings.format("""
            {"geo_shape": {"shape": {"relation": "contains", "shape": "POINT (%s %s)"}}}""", fmt(lon(i)), fmt(lat(i)))), 20, 4000);
    }

    /** Each clause is a small square around the octagon of one document, and costs one bit set. */
    public void testIntersectsPolygonClauses() throws IOException {
        assertOnlyTripsOrSucceeds(n -> search(n, i -> {
            double lat = lat(i);
            double lon = lon(i);
            double d = RADIUS / 2;
            return Strings.format(
                """
                    {"geo_shape": {"shape": {"relation": "intersects", "shape": "POLYGON ((%s %s, %s %s, %s %s, %s %s, %s %s))"}}}""",
                fmt(lon - d),
                fmt(lat - d),
                fmt(lon + d),
                fmt(lat - d),
                fmt(lon + d),
                fmt(lat + d),
                fmt(lon - d),
                fmt(lat + d),
                fmt(lon - d),
                fmt(lat - d)
            );
        }), 20, 4000);
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

    /** A {@code bool} of {@code count} clauses, each about a different document so that they are not merged. */
    private static Request search(int count, IntFunction<String> clause) {
        StringBuilder clauses = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                clauses.append(',');
            }
            // Spread the clauses over the documents.
            clauses.append(clause.apply(i * (DOCS / count)));
        }
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.addParameter("request_cache", "false");
        search.setJsonEntity(Strings.format("""
            {"size": 0, "track_total_hits": true, "query": {"bool": {"should": [%s]}}}
            """, clauses));
        return search;
    }

    /** Documents sit on a grid of 500 rows by 1000 columns, so that every clause is about a different one and none are merged. */
    private static double lat(int i) {
        return (i / 1000) * 0.04 - 10.0;
    }

    private static double lon(int i) {
        return (i % 1000) * 0.02 - 10.0;
    }

    private static String octagon(double lat, double lon) {
        StringBuilder wkt = new StringBuilder("POLYGON ((");
        for (int k = 0; k <= 8; k++) {
            double angle = Math.PI / 4 * (k % 8);
            if (k > 0) {
                wkt.append(", ");
            }
            wkt.append(fmt(lon + RADIUS * Math.cos(angle))).append(' ').append(fmt(lat + RADIUS * Math.sin(angle)));
        }
        return wkt.append("))").toString();
    }

    private static String fmt(double value) {
        return String.format(Locale.ROOT, "%.6f", value);
    }
}
