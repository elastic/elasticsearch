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
import org.elasticsearch.test.rest.HeapPressureTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Map;

/**
 * A {@code bool} of many {@code bitmap_terms} clauses must not be able to take a node down.
 *
 * <p>On a field indexed with points, in a segment that is not sorted on it, {@code bitmap_terms} collects the matching documents of a
 * segment into a {@code DocIdSetBuilder}, which is a bit set of {@code maxDoc} bits once a clause matches more than 1/128 of the segment.
 * The scorers of all the clauses of a {@code bool} are created together, so a request holds the bit sets of all of them, about 62kb each
 * for {@link #DOCS} documents and more than the heap for {@link #MANY_CLAUSES} clauses, and the request circuit breaker sees none of it.
 * The case passes if the request either succeeds or is rejected by a circuit breaker and the node survives.
 */
public class BitmapTermsHeapPressureIT extends HeapPressureTestCase {

    private static final String INDEX = "bitmap-terms-heap-pressure";
    private static final int DOCS = 500_000;
    private static final int DOCS_PER_BULK = 10_000;
    // Under the clause limit of 4096 that a 256mb node with 4 search threads has.
    private static final int MANY_CLAUSES = 4000;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(1)
        .module("bitmap")
        .setting("xpack.security.enabled", "false")
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

    /** {@link #DOCS} documents in one segment, half with {@code v} 0 and half with 1. */
    @Before
    public void indexDocuments() throws IOException {
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("""
            {
              "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": -1},
              "mappings": {"properties": {"v": {"type": "integer"}}}
            }""");
        client().performRequest(create);
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < DOCS; i++) {
            bulk.append("{\"index\":{}}\n").append(Strings.format("{\"v\":%d}\n", i % 2));
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
        logger.info("Indexed {} documents", DOCS);
    }

    /** Each clause matches half the documents. */
    public void testBitmapTermsClauses() throws IOException {
        assertNull("expected the request to succeed", errorBodyOrNull(bitmapTermsSearch(50)));
        assertNodeAlive();
        logger.info("Searching for {} clauses", MANY_CLAUSES);
        Map<String, Object> body = errorBodyOrNull(bitmapTermsSearch(MANY_CLAUSES));
        if (body != null) {
            assertTrue("expected only a circuit_breaking_exception, but got: " + body, containsCircuitBreakingException(body));
            logger.info("rejected by a circuit breaker: {}", body);
        } else {
            logger.info("succeeded");
        }
        assertNodeAlive();
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
        Request search = new Request("POST", "/" + INDEX + "/_search");
        search.addParameter("request_cache", "false");
        search.setJsonEntity(Strings.format("""
            {"size": 0, "track_total_hits": true, "query": {"bool": {"should": [%s]}}}
            """, clauses));
        return search;
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
