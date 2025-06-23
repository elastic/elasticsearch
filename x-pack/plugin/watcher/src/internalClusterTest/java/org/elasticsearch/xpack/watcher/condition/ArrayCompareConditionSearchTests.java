/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.condition.Condition;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

public class ArrayCompareConditionSearchTests extends AbstractWatcherIntegrationTestCase {

    public void testExecuteWithAggs() throws Exception {
        String index = "test-index";
        indicesAdmin().prepareCreate(index).get();

        ArrayCompareCondition.Op op = randomFrom(ArrayCompareCondition.Op.values());
        ArrayCompareCondition.Quantifier quantifier = randomFrom(ArrayCompareCondition.Quantifier.values());
        int numberOfDocuments = randomIntBetween(1, 100);
        int numberOfDocumentsWatchingFor = 1 + numberOfDocuments;
        for (int i = 0; i < numberOfDocuments; i++) {
            prepareIndex(index).setSource(source("elastic", "you know, for search", i)).get();
            prepareIndex(index).setSource(source("fights_for_the_users", "you know, for the users", i)).get();
        }

        refresh();

        ArrayCompareCondition condition = new ArrayCompareCondition(
            "ctx.payload.aggregations.top_tweeters.buckets",
            "doc_count",
            op,
            numberOfDocumentsWatchingFor,
            quantifier,
            Clock.systemUTC()
        );

        Map<String, Object> fightsForTheUsers = new HashMap<>();
        Map<String, Object> elastic = new HashMap<>();

        assertResponse(
            prepareSearch(index).addAggregation(AggregationBuilders.terms("top_tweeters").field("user.screen_name.keyword").size(3)),
            response -> {
                WatchExecutionContext ctx = null;
                try {
                    ctx = mockExecutionContext("_name", new Payload.XContent(response, ToXContent.EMPTY_PARAMS));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                Condition.Result result = condition.execute(ctx);

                boolean met = quantifier.eval(
                    Arrays.<Object>asList(numberOfDocuments, numberOfDocuments),
                    numberOfDocumentsWatchingFor,
                    op
                );
                assertEquals(met, result.met());

                Map<String, Object> resolvedValues = result.getResolvedValues();
                assertThat(resolvedValues, notNullValue());
                assertThat(resolvedValues.size(), is(1));
                elastic.put("doc_count", numberOfDocuments);
                elastic.put("key", "elastic");

                fightsForTheUsers.put("doc_count", numberOfDocuments);
                fightsForTheUsers.put("key", "fights_for_the_users");
                assertThat(
                    resolvedValues,
                    hasEntry("ctx.payload.aggregations.top_tweeters.buckets", (Object) Arrays.asList(elastic, fightsForTheUsers))
                );
            }
        );

        prepareIndex(index).setSource(source("fights_for_the_users", "you know, for the users", numberOfDocuments)).get();
        refresh();

        assertResponse(
            prepareSearch(index).addAggregation(AggregationBuilders.terms("top_tweeters").field("user.screen_name.keyword").size(3)),
            response -> {
                WatchExecutionContext ctx = null;
                try {
                    ctx = mockExecutionContext("_name", new Payload.XContent(response, ToXContent.EMPTY_PARAMS));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                Condition.Result result = condition.execute(ctx);

                boolean met = quantifier.eval(
                    Arrays.<Object>asList(numberOfDocumentsWatchingFor, numberOfDocuments),
                    numberOfDocumentsWatchingFor,
                    op
                );
                assertEquals(met, result.met());

                Map<String, Object> resolvedValues = result.getResolvedValues();
                assertThat(resolvedValues, notNullValue());
                assertThat(resolvedValues.size(), is(1));
                fightsForTheUsers.put("doc_count", numberOfDocumentsWatchingFor);
                assertThat(
                    resolvedValues,
                    hasEntry("ctx.payload.aggregations.top_tweeters.buckets", (Object) Arrays.asList(fightsForTheUsers, elastic))
                );
            }
        );

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
