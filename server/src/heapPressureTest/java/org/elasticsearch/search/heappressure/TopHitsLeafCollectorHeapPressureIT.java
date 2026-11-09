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
 * buffers; on a stateless search node that came to about 5 KB per bucket. None of it is charged to a circuit breaker. Several
 * {@code top_hits} under one large {@code terms} multiply that. Unsorted {@code top_hits} hold no reader, but their per-bucket collectors
 * and fetch results are not charged either, so each case has its own test.
 *
 * <p>The hits themselves are small: the {@code top_hits} return no {@code _source}, so the bytes the fetch charges stay far below the
 * request breaker. The index uses the time series doc values format, as logs indices do, and is merged into one segment, so every bucket
 * has a leaf collector at the same time.
 */
public class TopHitsLeafCollectorHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "top-hits-leaf-collectors";
    private static final int TAGS = 10_000;
    private static final int TOP_HITS = 12;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").build();

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
     * Sorted by a field, each leaf collector also holds a doc values reader. {@value #TOP_HITS} {@code top_hits} over {@value #TAGS}
     * buckets keep more uncharged per-bucket state than the heap holds.
     */
    public void testManySortedTopHitsPerBucketDoNotKillTheNode() throws IOException {
        assertFitsOrTripsBreaker(true);
    }

    /**
     * Sorted by score, a leaf collector holds no doc values reader, but the per-bucket collectors and fetch results of
     * {@value #TOP_HITS} {@code top_hits} over {@value #TAGS} buckets are still not charged.
     */
    public void testManyUnsortedTopHitsPerBucketDoNotKillTheNode() throws IOException {
        assertFitsOrTripsBreaker(false);
    }

    /** A few buckets fit; all of them must either fit or be rejected by a circuit breaker, and the node must survive. */
    private void assertFitsOrTripsBreaker(boolean sorted) throws IOException {
        assertSucceeds(search(INDEX, topHitsPerTag(100, sorted)));
        Map<String, Object> body = errorBodyOrNull(search(INDEX, topHitsPerTag(TAGS, sorted)));
        assertTrue(
            "expected success or a circuit_breaking_exception, but got: " + body,
            body == null || containsCircuitBreakingException(body)
        );
        assertNodeAlive();
    }

    /**
     * One {@code terms} bucket per tag, each with {@value #TOP_HITS} {@code top_hits} returning one document without its source, the
     * latest by {@code ts} if {@code sorted}, otherwise the best scoring.
     */
    private static String topHitsPerTag(int tags, boolean sorted) {
        StringBuilder topHits = new StringBuilder();
        for (int i = 0; i < TOP_HITS; i++) {
            if (i > 0) {
                topHits.append(',');
            }
            topHits.append(Strings.format("""
                "top_%d": {"top_hits": {"size": 1, %s"_source": false}}""", i, sorted ? "\"sort\": [{\"ts\": \"desc\"}], " : ""));
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
