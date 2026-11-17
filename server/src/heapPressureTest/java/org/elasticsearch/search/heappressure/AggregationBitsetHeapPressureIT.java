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
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;
import java.util.function.IntFunction;

/**
 * Aggregations that keep a bit set as large as the shard for as long as the request runs, and that the request circuit breaker does not
 * see, so that a request with many of them holds more than the heap:
 * <ul>
 * <li>{@code significant_terms} with a {@code background_filter}: {@code FilterableTermsEnum} runs the filter on a plain
 * {@code IndexSearcher} and keeps a bit set of {@code maxDoc} bits per segment for the aggregation's lifetime;
 * <li>{@code terms} with {@code include}: the accepted global ordinals are a {@code LongBitSet} of one bit per distinct value of the field,
 * kept by the aggregator.
 * </ul>
 * Each case is one request of {@link #AGGS} aggregations over one segment of {@link #DOCS} documents, about 62kb of bit set each and more
 * than the heap in total. The query selects few documents, so that collecting them costs next to nothing. A case passes if the request
 * either succeeds or is rejected by a circuit breaker and the node survives. Each case should be run on its own, since a case that kills
 * the node skips the rest. The controls run the same number of aggregations with no or tiny bit sets, and must succeed.
 */
public class AggregationBitsetHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "aggregation-bitset-heap-pressure";
    private static final int DOCS = 500_000;
    private static final int TAGS = 10;
    // One document in SELECTED matches the query.
    private static final int SELECTED = 1000;
    private static final int AGGS = 4000;
    private static final int FEW_AGGS = 20;

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
        indexDocuments(INDEX, 1, """
            {
              "tag": {"type": "keyword"},
              "k": {"type": "keyword"},
              "sel": {"type": "boolean"},
              "n": {"type": "long"}
            }""", DOCS, i -> Strings.format("""
            {"tag":"tag-%d","k":"k-%06d","sel":%s,"n":%d}""", i % TAGS, i, i % SELECTED == 0, i));
    }

    /** Each aggregation has its own background filter, which matches nearly every document. */
    public void testSignificantTermsBackgroundFilters() throws IOException {
        assertOnlyTripsOrSucceeds(n -> search(INDEX, searchBody(aggs(n, i -> Strings.format("""
            {"significant_terms": {"field": "tag", "size": 1, "min_doc_count": 1,
              "background_filter": {"range": {"n": {"gte": %d}}}}}""", i)))));
    }

    /** Each aggregation includes one value of {@code k}, which has a distinct value per document. */
    public void testTermsIncludeLists() throws IOException {
        assertOnlyTripsOrSucceeds(n -> search(INDEX, searchBody(aggs(n, i -> Strings.format("""
            {"terms": {"field": "k", "size": 1, "include": ["k-%06d"]}}""", i * SELECTED)))));
    }

    /**
     * Control for {@link #testSignificantTermsBackgroundFilters}: the same aggregations without a background filter, which then keep no
     * bit set, fit in the heap.
     */
    public void testSignificantTermsWithoutBackgroundFilter() throws IOException {
        assertSucceeds(search(INDEX, searchBody(aggs(AGGS, i -> """
            {"significant_terms": {"field": "tag", "size": 1, "min_doc_count": 1}}"""))));
    }

    /**
     * Control for {@link #testTermsIncludeLists}: the same aggregations on {@code tag}, which has {@link #TAGS} values, so that their bit
     * sets are tiny, fit. Without {@code include} the aggregation would collect into arrays of one entry per value instead, which are
     * charged and trip the breaker for {@code k}, so the control keeps it.
     */
    public void testTermsIncludeListsOnFewValues() throws IOException {
        assertSucceeds(search(INDEX, searchBody(aggs(AGGS, i -> Strings.format("""
            {"terms": {"field": "tag", "size": 1, "include": ["tag-%d"]}}""", i % TAGS)))));
    }

    private void assertOnlyTripsOrSucceeds(IntFunction<Request> request) throws IOException {
        assertSucceeds(request.apply(FEW_AGGS));
        logger.info("Using {} aggregations", AGGS);
        Map<String, Object> body = errorBodyOrNull(request.apply(AGGS));
        if (body != null) {
            assertTrue("expected only a circuit_breaking_exception, but got: " + body, containsCircuitBreakingException(body));
            logger.info("rejected by a circuit breaker: {}", body);
        } else {
            logger.info("succeeded");
        }
        assertNodeAlive();
    }

    private static String searchBody(String aggs) {
        return Strings.format("""
            {"size": 0, "query": {"term": {"sel": true}}, "aggs": {%s}}
            """, aggs);
    }

    private static String aggs(int count, IntFunction<String> agg) {
        StringBuilder aggs = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                aggs.append(',');
            }
            aggs.append("\"a").append(i).append("\":").append(agg.apply(i));
        }
        return aggs.toString();
    }
}
