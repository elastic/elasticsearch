/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.indexAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

/**
 * This test makes sure that the index action response `index` field in the watch_record action result is
 * not analyzed so it can be used in aggregations
 */
public class HistoryTemplateIndexActionMappingsTests extends AbstractWatcherIntegrationTestCase {

    public void testIndexActionFields() throws Exception {
        String index = "the-index";

        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), "_id").setSource(
            watchBuilder().trigger(schedule(interval("5m"))).addAction("index", indexAction(index))
        ).get();

        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().trigger("_id");
        flush();
        refresh();

        // the action should fail as no email server is available
        assertWatchWithMinimumActionsCount("_id", ExecutionState.EXECUTED, 1);
        flush();
        refresh();

        assertResponse(
            prepareSearch(HistoryStoreField.DATA_STREAM + "*").setSource(
                searchSource().aggregation(terms("index_action_indices").field("result.actions.index.response.index"))
            ),
            response -> {
                assertThat(response, notNullValue());
                assertThat(response.getHits().getTotalHits().value(), is(oneOf(1L, 2L)));
                InternalAggregations aggs = response.getAggregations();
                assertThat(aggs, notNullValue());

                Terms terms = aggs.get("index_action_indices");
                assertThat(terms, notNullValue());
                assertThat(terms.getBuckets().size(), is(1));
                assertThat(terms.getBucketByKey(index), notNullValue());
                assertThat(terms.getBucketByKey(index).getDocCount(), is(1L));
            }
        );
    }
}
