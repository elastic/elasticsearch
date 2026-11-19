/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import static org.elasticsearch.search.aggregations.AggregationBuilders.max;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests that the charge for the aggregations of an async search is held while its response is, and released once the task has closed.
 */
public class AsyncSearchAggregationChargeIT extends AsyncSearchIntegTestCase {

    private static final String INDEX = "aggregation_charge";
    private static final int TAGS = 500;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // The test cluster can make a node's request breaker a noop one, which charges nothing.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    public void testChargeIsHeldByTheResponseThatIsNotStored() throws Exception {
        createIndexWithTags();
        long baseline = requestBreakerUsed();
        SubmitAsyncSearchRequest request = request();
        request.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(30));
        request.setKeepOnCompletion(false);
        AsyncSearchResponse response = submitAsyncSearch(request);
        try {
            assertNull("the response is not stored", response.getId());
            assertFalse(response.isRunning());
            assertThat(requestBreakerUsed(), greaterThan(baseline));
        } finally {
            response.decRef();
        }
        assertBusy(() -> assertThat(requestBreakerUsed(), equalTo(baseline)));
    }

    public void testChargeIsReleasedOnceTheTaskStoredItsResponse() throws Exception {
        createIndexWithTags();
        long baseline = requestBreakerUsed();
        SubmitAsyncSearchRequest request = request();
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(1));
        request.setKeepOnCompletion(true);
        AsyncSearchResponse submitted = submitAsyncSearch(request);
        String id = submitted.getId();
        submitted.decRef();
        try {
            ensureTaskCompletion(id);
            // Without deleting the search, so only the task closing can release what its response held.
            assertBusy(() -> assertThat(requestBreakerUsed(), equalTo(baseline)));

            AsyncSearchResponse stored = getAsyncSearch(id);
            try {
                assertFalse(stored.isRunning());
                assertNotNull(stored.getSearchResponse().getAggregations());
            } finally {
                stored.decRef();
            }
            assertBusy(() -> assertThat(requestBreakerUsed(), equalTo(baseline)));
        } finally {
            deleteAsyncSearch(id);
        }
    }

    private static SubmitAsyncSearchRequest request() {
        return new SubmitAsyncSearchRequest(
            new SearchSourceBuilder().size(0).aggregation(terms("tags").field("tag").size(TAGS).subAggregation(max("max_n").field("n"))),
            INDEX
        );
    }

    private void createIndexWithTags() {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            ).setMapping("tag", "type=keyword", "n", "type=long")
        );
        BulkRequestBuilder bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < TAGS; i++) {
            bulk.add(prepareIndex(INDEX).setSource("tag", "tag-" + i, "n", i));
        }
        assertNoFailures(bulk.get());
    }

    /** The bytes the request breakers of all nodes hold. */
    private static long requestBreakerUsed() {
        long used = 0;
        for (String node : internalCluster().getNodeNames()) {
            used += internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST).getUsed();
        }
        return used;
    }
}
