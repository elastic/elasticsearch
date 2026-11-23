/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * The coordinating node holds the aggregations it reduced until the response is released, such as after a REST response is sent, so the
 * response holds their charge on the request breaker until then.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class AggregationResponseCircuitBreakerIT extends ESIntegTestCase {

    private static final String INDEX = "aggregation_response";

    public void testResponseHoldsAggregationChargeUntilReleased() throws Exception {
        assertResponseHoldsAggregationChargeUntilReleased(SearchType.QUERY_THEN_FETCH);
    }

    /** The DFS search keeps its query phase results apart from the results of its DFS phase, so it hands the charge over too. */
    public void testDfsResponseHoldsAggregationChargeUntilReleased() throws Exception {
        assertResponseHoldsAggregationChargeUntilReleased(SearchType.DFS_QUERY_THEN_FETCH);
    }

    private void assertResponseHoldsAggregationChargeUntilReleased(SearchType searchType) throws Exception {
        // The test cluster can make a node's request breaker a noop one, which charges nothing.
        String node = internalCluster().startNode(Settings.builder().put("indices.breaker.request.type", "memory"));
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("tag", "type=keyword", "n", "type=long")
        );
        int tags = randomIntBetween(100, 1000);
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < tags; i++) {
            bulk.add(prepareIndex(INDEX).setSource("tag", "tag-" + i, "n", i));
        }
        assertNoFailures(bulk.get());

        long breakerBefore = requestBreakerUsed(node);
        SearchResponse response = client(node).prepareSearch(INDEX)
            .setSearchType(searchType)
            .setSize(0)
            .setRequestCache(false)
            .addAggregation(terms("tags").field("tag").size(tags).subAggregation(max("max_n").field("n")))
            .get();
        try {
            assertNoFailures(response);
            assertThat(
                "the response must hold the charge for its aggregations after the search completes",
                requestBreakerUsed(node),
                greaterThanOrEqualTo(breakerBefore + response.getAggregations().estimateHeapBytes())
            );
        } finally {
            response.decRef();
        }
        assertBusy(() -> assertThat(requestBreakerUsed(node), equalTo(breakerBefore)));
    }

    private static long requestBreakerUsed(String node) {
        return internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST).getUsed();
    }
}
