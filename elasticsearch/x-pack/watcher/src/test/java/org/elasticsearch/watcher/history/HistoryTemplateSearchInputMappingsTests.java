/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.history;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.watcher.execution.ExecutionState;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test makes sure that the http host and path fields in the watch_record action result are
 * not analyzed so they can be used in aggregations
 */
public class HistoryTemplateSearchInputMappingsTests extends AbstractWatcherIntegrationTestCase {
    
    @Override
    protected boolean timeWarped() {
        return true; // just to have better control over the triggers
    }

    @Override
    protected boolean enableShield() {
        return false; // remove shield noise from this test
    }

    public void testHttpFields() throws Exception {
        String index = "the-index";
        String type = "the-type";
        createIndex(index);
        index(index, type, "{}");
        flush();
        refresh();

        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(searchInput(new SearchRequest().indices(index).types(type).searchType(SearchType.QUERY_AND_FETCH)
                        .source(searchSource().query(matchAllQuery()))))
                .condition(alwaysCondition())
                .addAction("logger", loggingAction("indexed")))
                .get();

        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().scheduler().trigger("_id");
        flush();
        refresh();

        // the action should fail as no email server is available
        assertWatchWithMinimumActionsCount("_id", ExecutionState.EXECUTED, 1);

        SearchResponse response = client().prepareSearch(HistoryStore.INDEX_PREFIX + "*").setSource(searchSource()
                .aggregation(terms("input_search_type").field("result.input.search.request.search_type"))
                .aggregation(terms("input_indices").field("result.input.search.request.indices"))
                .aggregation(terms("input_types").field("result.input.search.request.types"))
                .aggregation(terms("input_body").field("result.input.search.request.body")))
                .get();

        assertThat(response, notNullValue());
        assertThat(response.getHits().getTotalHits(), is(1L));
        Aggregations aggs = response.getAggregations();
        assertThat(aggs, notNullValue());

        Terms terms = aggs.get("input_search_type");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(1));
        assertThat(terms.getBucketByKey("query_and_fetch"), notNullValue());
        assertThat(terms.getBucketByKey("query_and_fetch").getDocCount(), is(1L));

        terms = aggs.get("input_indices");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(1));
        assertThat(terms.getBucketByKey(index), notNullValue());
        assertThat(terms.getBucketByKey(index).getDocCount(), is(1L));

        terms = aggs.get("input_types");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(1));
        assertThat(terms.getBucketByKey(type), notNullValue());
        assertThat(terms.getBucketByKey(type).getDocCount(), is(1L));

        terms = aggs.get("input_body");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(0));
    }
}
