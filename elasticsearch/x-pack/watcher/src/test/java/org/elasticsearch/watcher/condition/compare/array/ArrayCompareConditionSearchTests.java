/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.compare.array;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

public class ArrayCompareConditionSearchTests extends AbstractWatcherIntegrationTestCase {
    public void testExecuteWithAggs() throws Exception {
        String index = "test-index";
        String type = "test-type";
        client().admin().indices().prepareCreate(index)
                .addMapping(type)
                .get();

        ArrayCompareCondition.Op op = randomFrom(ArrayCompareCondition.Op.values());
        ArrayCompareCondition.Quantifier quantifier = randomFrom(ArrayCompareCondition.Quantifier.values());
        int numberOfDocuments = randomIntBetween(1, 100);
        int numberOfDocumentsWatchingFor = 1 + numberOfDocuments;
        for (int i = 0; i < numberOfDocuments; i++) {
            client().prepareIndex(index, type).setSource(source("elastic", "you know, for search", i)).get();
            client().prepareIndex(index, type).setSource(source("fights_for_the_users", "you know, for the users", i)).get();
        }

        refresh();

        SearchResponse response = client().prepareSearch(index)
                .addAggregation(AggregationBuilders.terms("top_tweeters").field("user.screen_name.keyword").size(3)).get();


        ExecutableArrayCompareCondition condition = new ExecutableArrayCompareCondition(
                new ArrayCompareCondition("ctx.payload.aggregations.top_tweeters.buckets" , "doc_count", op,
                        numberOfDocumentsWatchingFor, quantifier),
                logger,
                SystemClock.INSTANCE
        );

        WatchExecutionContext ctx = mockExecutionContext("_name", new Payload.XContent(response));
        ArrayCompareCondition.Result result = condition.execute(ctx);

        boolean met = quantifier.eval(Arrays.<Object>asList(numberOfDocuments, numberOfDocuments), numberOfDocumentsWatchingFor, op);
        assertEquals(met, result.met());

        Map<String, Object> resolvedValues = result.getResolvedValues();
        assertThat(resolvedValues, notNullValue());
        assertThat(resolvedValues.size(), is(1));
        Map<String, Object> elastic = new HashMap<>();
        elastic.put("doc_count", numberOfDocuments);
        elastic.put("key", "elastic");
        Map<String, Object> fightsForTheUsers = new HashMap<>();
        fightsForTheUsers.put("doc_count", numberOfDocuments);
        fightsForTheUsers.put("key", "fights_for_the_users");
        assertThat(resolvedValues, hasEntry("ctx.payload.aggregations.top_tweeters.buckets",
                (Object) Arrays.asList(elastic, fightsForTheUsers)));

        client().prepareIndex(index, type).setSource(source("fights_for_the_users", "you know, for the users", numberOfDocuments)).get();
        refresh();

        response = client().prepareSearch(index)
                .addAggregation(AggregationBuilders.terms("top_tweeters").field("user.screen_name.keyword").size(3)).get();

        ctx = mockExecutionContext("_name", new Payload.XContent(response));
        result = condition.execute(ctx);

        met = quantifier.eval(Arrays.<Object>asList(numberOfDocumentsWatchingFor, numberOfDocuments), numberOfDocumentsWatchingFor, op);
        assertEquals(met, result.met());

        resolvedValues = result.getResolvedValues();
        assertThat(resolvedValues, notNullValue());
        assertThat(resolvedValues.size(), is(1));
        fightsForTheUsers.put("doc_count", numberOfDocumentsWatchingFor);
        assertThat(resolvedValues, hasEntry("ctx.payload.aggregations.top_tweeters.buckets",
                (Object) Arrays.asList(fightsForTheUsers, elastic)));
    }

    private XContentBuilder source(String screenName, String tweet, int i) throws IOException {
        return jsonBuilder().startObject()
                    .startObject("user")
                        .field("screen_name", screenName)
                    .endObject()
                    .field("tweet", tweet + " " + i)
                .endObject();
    }
}
