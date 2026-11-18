/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

/**
 * A {@code top_hits} keeps a Lucene collector per bucket, and while it collects a segment, a leaf collector per bucket too. Sorted by a
 * field, each leaf collector's comparator also opens its own doc values reader for the sort field, with its input clones and decode
 * buffers; on a stateless search node that came to about 5 KB per bucket. Several {@code top_hits} under one large {@code terms} multiply
 * that. A sort on one numeric field usually uses a {@code BucketedSort} instead, and the Lucene collectors of the other sorts are
 * charged to the request breaker with estimates. Unsorted {@code top_hits} hold no reader, so each case has its own test.
 *
 * <p>The hits themselves are small: the {@code top_hits} return no {@code _source}, so the bytes the fetch charges stay far below the
 * request breaker. The index uses the time series doc values format, as logs indices do, and is merged into one segment, so every bucket
 * has a leaf collector at the same time.
 */
public class TopHitsLeafCollectorHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "top-hits-leaf-collectors";
    private static final int TAGS = 10_000;
    private static final int TOP_HITS = 12;

    /**
     * Assertions are disabled on the node because they enable leak tracking, which keeps a stack trace for every reference count
     * change of the shard results and hits that each bucket creates. That came to about 190 MB of the 256 MB heap, so the node ran
     * out of heap on leak tracking that production does not have.
     */
    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").jvmArg("-da").build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void indexTaggedDocuments() throws IOException {
        indexDocuments(INDEX, 1, """
            "index.use_time_series_doc_values_format": true""", """
            {"tag": {"type": "keyword"}, "ts": {"type": "long"}}""", TAGS, i -> Strings.format("""
            {"tag":"tag-%05d","ts":%d}""", i, i));
    }

    /**
     * Sorted by a long field, the top hits of every bucket are collected with a {@code BucketedSort}, which holds no leaf collector per
     * bucket and is charged to the request breaker.
     */
    public void testManyLongSortedTopHitsPerBucketDoNotKillTheNode() throws IOException {
        assertFitsOrTripsBreaker("\"sort\": [{\"ts\": \"desc\"}], ");
    }

    /**
     * Sorted by a keyword field, which a {@code BucketedSort} does not support, each leaf collector holds a doc values reader for the
     * sort field. {@value #TOP_HITS} {@code top_hits} over {@value #TAGS} buckets need more of them than the heap holds.
     */
    public void testManyKeywordSortedTopHitsPerBucketDoNotKillTheNode() throws IOException {
        assertFitsOrTripsBreaker("\"sort\": [{\"tag\": \"desc\"}], ");
    }

    /**
     * Sorted by score, a leaf collector holds no doc values reader, but {@value #TOP_HITS} {@code top_hits} over {@value #TAGS} buckets
     * still hold a collector each.
     */
    public void testManyScoreSortedTopHitsPerBucketDoNotKillTheNode() throws IOException {
        assertFitsOrTripsBreaker("");
    }

    /**
     * A few buckets fit; all of them must either fit or be rejected by a circuit breaker, and the node must survive. {@code sort} is
     * the sort of every {@code top_hits}, followed by a comma, or empty to sort by score.
     */
    private void assertFitsOrTripsBreaker(String sort) throws IOException {
        assertSucceeds(search(INDEX, manyTopHitsPerTag(100, sort)));
        Map<String, Object> body = errorBodyOrNull(search(INDEX, manyTopHitsPerTag(TAGS, sort)));
        assertTrue(
            "expected success or a circuit_breaking_exception, but got: " + body,
            body == null || containsCircuitBreakingException(body)
        );
        assertNodeAlive();
    }

    /**
     * One {@code terms} bucket per tag, each with {@value #TOP_HITS} {@code top_hits} returning one document without its source, the
     * first by {@code sort}, or the best scoring if it is empty.
     */
    private static String manyTopHitsPerTag(int tags, String sort) {
        StringBuilder topHits = new StringBuilder();
        for (int i = 0; i < TOP_HITS; i++) {
            if (i > 0) {
                topHits.append(',');
            }
            topHits.append(Strings.format("""
                "top_%d": {"top_hits": {"size": 1, %s"_source": false}}""", i, sort));
        }
        return Strings.format("""
            {
              "size": 0,
              "aggs": {
                "tags": {
                  "terms": {"field": "tag", "size": %d},
                  "aggs": {%s}
                }
              }
            }
            """, tags, topHits);
    }
}
