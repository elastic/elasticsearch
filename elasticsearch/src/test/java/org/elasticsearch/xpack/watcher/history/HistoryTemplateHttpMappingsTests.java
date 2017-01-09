/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.common.http.HttpMethod;
import org.elasticsearch.xpack.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.webhookAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test makes sure that the http host and path fields in the watch_record action result are
 * not analyzed so they can be used in aggregations
 */
public class HistoryTemplateHttpMappingsTests extends AbstractWatcherIntegrationTestCase {

    private MockWebServer webServer = new MockWebServer();

    @Before
    public void init() throws Exception {
        webServer.start();
    }

    @After
    public void cleanup() throws Exception {
        webServer.close();
    }

    @Override
    protected boolean timeWarped() {
        return true; // just to have better control over the triggers
    }

    @Override
    protected boolean enableSecurity() {
        return false; // remove security noise from this test
    }

    @TestLogging("org.elasticsearch.test.http:TRACE")
    public void testHttpFields() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(httpInput(HttpRequestTemplate.builder("localhost", webServer.getPort()).path("/input/path")))
                .condition(AlwaysCondition.INSTANCE)
                .addAction("_webhook", webhookAction(HttpRequestTemplate.builder("localhost", webServer.getPort())
                        .path("/webhook/path")
                        .method(HttpMethod.POST)
                        .body("_body"))))
                .get();


        // one for the input, one for the webhook
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));

        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().scheduler().trigger("_id");
        flush();
        refresh();

        // the action should fail as no email server is available
        assertWatchWithMinimumActionsCount("_id", ExecutionState.EXECUTED, 1);

        SearchResponse response = client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*").setSource(searchSource()
                .aggregation(terms("input_result_path").field("result.input.http.request.path"))
                .aggregation(terms("input_result_host").field("result.input.http.request.host"))
                .aggregation(terms("webhook_path").field("result.actions.webhook.request.path")))
                .get();

        assertThat(response, notNullValue());
        assertThat(response.getHits().getTotalHits(), is(1L));
        Aggregations aggs = response.getAggregations();
        assertThat(aggs, notNullValue());

        Terms terms = aggs.get("input_result_path");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(1));
        assertThat(terms.getBucketByKey("/input/path"), notNullValue());
        assertThat(terms.getBucketByKey("/input/path").getDocCount(), is(1L));

        terms = aggs.get("webhook_path");
        assertThat(terms, notNullValue());
        assertThat(terms.getBuckets().size(), is(1));
        assertThat(terms.getBucketByKey("/webhook/path"), notNullValue());
        assertThat(terms.getBucketByKey("/webhook/path").getDocCount(), is(1L));

        assertThat(webServer.requests(), hasSize(2));
        assertThat(webServer.requests().get(0).getUri().getPath(), is("/input/path"));
        assertThat(webServer.requests().get(1).getUri().getPath(), is("/webhook/path"));
    }
}
