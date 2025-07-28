/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.protocol.xpack.watcher.PutWatchResponse;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.xpack.core.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.core.watcher.history.HistoryStoreField;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.support.search.WatcherSearchTemplateRequest;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.searchInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

/**
 * This test makes sure that the http host and path fields in the watch_record action result are
 * not analyzed so they can be used in aggregations
 */
public class HistoryTemplateSearchInputMappingsTests extends AbstractWatcherIntegrationTestCase {

    public void testHttpFields() throws Exception {
        String index = "the-index";
        createIndex(index);
        indexDoc(index, "{}");
        flush();
        refresh();

        WatcherSearchTemplateRequest request = new WatcherSearchTemplateRequest(
            new String[] { index },
            SearchType.QUERY_THEN_FETCH,
            WatcherSearchTemplateRequest.DEFAULT_INDICES_OPTIONS,
            new BytesArray("{}")
        );
        PutWatchResponse putWatchResponse = new PutWatchRequestBuilder(client(), "_id").setSource(
            watchBuilder().trigger(schedule(interval("5s")))
                .input(searchInput(request))
                .condition(InternalAlwaysCondition.INSTANCE)
                .addAction("logger", loggingAction("indexed"))
        ).get();

        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().trigger("_id");
        flush();
        refresh();

        // the action should fail as no email server is available
        assertWatchWithMinimumActionsCount("_id", ExecutionState.EXECUTED, 1);

        assertResponse(
            prepareSearch(HistoryStoreField.DATA_STREAM + "*").setSource(
                searchSource().aggregation(terms("input_search_type").field("result.input.search.request.search_type"))
                    .aggregation(terms("input_indices").field("result.input.search.request.indices"))
                    .aggregation(terms("input_body").field("result.input.search.request.body"))
            ),
            response -> {
                assertThat(response, notNullValue());
                assertThat(response.getHits().getTotalHits().value(), is(oneOf(1L, 2L)));
                InternalAggregations aggs = response.getAggregations();
                assertThat(aggs, notNullValue());

                Terms terms = aggs.get("input_search_type");
                assertThat(terms, notNullValue());
                assertThat(terms.getBuckets().size(), is(1));
                assertThat(terms.getBucketByKey("query_then_fetch"), notNullValue());
                assertThat(terms.getBucketByKey("query_then_fetch").getDocCount(), is(oneOf(1L, 2L)));

                terms = aggs.get("input_indices");
                assertThat(terms, notNullValue());
                assertThat(terms.getBuckets().size(), is(1));
                assertThat(terms.getBucketByKey(index), notNullValue());
                assertThat(terms.getBucketByKey(index).getDocCount(), is(oneOf(1L, 2L)));

                terms = aggs.get("input_body");
                assertThat(terms, notNullValue());
                assertThat(terms.getBuckets().size(), is(0));
            }
        );
    }
}
