/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.AllocatingQueryPlugin.AllocatingQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * The memory a search allocates on a node is its query's there.
 */
public class SearchQueryMemoryIT extends ESIntegTestCase {

    private static final long BYTES = ByteSizeValue.ofKb(64).getBytes();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AllocatingQueryPlugin.class);
    }

    /**
     * A search context charges the query its known allocation.
     */
    public void testSearchContextChargesTheQuery() {
        createIndex();
        long bytes = memoryWhilePaused(prepareSearch("test").setQuery(new AllocatingQueryBuilder(BYTES)));
        assertThat(bytes, greaterThanOrEqualTo(BYTES));
    }

    /**
     * The aggregations of a search charge the query what they preallocate, at least.
     */
    public void testAggregationsChargeTheQuery() {
        createIndex();
        TermsAggregationBuilder terms = AggregationBuilders.terms("k").field("k");
        long preallocated = new AggregatorFactories.Builder().addAggregator(terms).bytesToPreallocate();
        assertThat(preallocated, greaterThan(0L));

        long without = memoryWhilePaused(prepareSearch("test").setSize(0).setQuery(new AllocatingQueryBuilder(BYTES)));
        long with = memoryWhilePaused(prepareSearch("test").setSize(0).setQuery(new AllocatingQueryBuilder(BYTES)).addAggregation(terms));
        assertThat(without, greaterThanOrEqualTo(BYTES));
        assertThat(with - without, greaterThanOrEqualTo(preallocated));
    }

    /**
     * A multi-search keeps the aggregations it buffers charged on the node's breaker after its searches complete, so no
     * query is left holding them.
     */
    public void testMultiSearchLeavesNoQueryTracked() throws Exception {
        createIndex();
        assertResponse(
            client().prepareMultiSearch().add(prepareSearch("test").setSize(0).addAggregation(AggregationBuilders.terms("k").field("k"))),
            response -> {
                for (MultiSearchResponse.Item item : response) {
                    assertNoFailures(item.getResponse());
                }
            }
        );
        // a search forgets its query once its listener returns, which can be just after the response is sent
        assertBusy(() -> {
            for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
                assertThat(searchService.queryScheduler().queries(), equalTo(0));
            }
        });
    }

    private void createIndex() {
        assertAcked(prepareCreate("test").setSettings(indexSettings(1, 0)).setMapping("k", "type=keyword"));
        prepareIndex("test").setSource("k", "a").get();
        indicesAdmin().prepareRefresh("test").get();
    }

    /**
     * Runs {@code search} until its query waits after allocating, and returns the bytes the query holds on all nodes then.
     */
    private long memoryWhilePaused(SearchRequestBuilder search) {
        long bytes;
        ActionFuture<SearchResponse> response;
        try (var pause = AllocatingQueryPlugin.pause()) {
            response = search.execute();
            pause.awaitReached();
            bytes = memoryBytes(queryId());
        }
        SearchResponse searchResponse = response.actionGet();
        try {
            assertNoFailures(searchResponse);
        } finally {
            searchResponse.decRef();
        }
        return bytes;
    }

    /**
     * The id of the running search, its task on the coordinator.
     */
    private static TaskId queryId() {
        List<TaskInfo> tasks = clusterAdmin().prepareListTasks().setActions(TransportSearchAction.NAME).get().getTasks();
        assertThat(tasks, hasSize(1));
        return tasks.get(0).taskId();
    }

    private static long memoryBytes(TaskId queryId) {
        long bytes = 0;
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            bytes += searchService.queryScheduler().memoryBytes(queryId);
        }
        return bytes;
    }
}
