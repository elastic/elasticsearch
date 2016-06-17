/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.history;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.QueueDispatcher;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.xpack.watcher.execution.ExecutionState;
import org.elasticsearch.xpack.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.junit.After;
import org.junit.Before;

import java.net.BindException;

import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.webhookAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This test makes sure that the http host and path fields in the watch_record action result are
 * not analyzed so they can be used in aggregations
 */
public class HistoryTemplateHttpMappingsTests extends AbstractWatcherIntegrationTestCase {
    private int webPort;
    private MockWebServer webServer;

    @Before
    public void init() throws Exception {
        for (webPort = 9200; webPort < 9300; webPort++) {
            try {
                webServer = new MockWebServer();
                QueueDispatcher dispatcher = new QueueDispatcher();
                dispatcher.setFailFast(true);
                webServer.setDispatcher(dispatcher);
                webServer.start(webPort);
                return;
            } catch (BindException be) {
                logger.warn("port [{}] was already in use trying next port", webPort);
            }
        }
        throw new ElasticsearchException("unable to find open port between 9200 and 9300");
    }

    @After
    public void cleanup() throws Exception {
        webServer.shutdown();
    }

    @Override
    protected boolean timeWarped() {
        return true; // just to have better control over the triggers
    }

    @Override
    protected boolean enableSecurity() {
        return false; // remove security noise from this test
    }

    public void testHttpFields() throws Exception {
        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("_id").setSource(watchBuilder()
                .trigger(schedule(interval("5s")))
                .input(httpInput(HttpRequestTemplate.builder("localhost", webPort).path("/input/path")))
                .condition(alwaysCondition())
                .addAction("_webhook", webhookAction(HttpRequestTemplate.builder("localhost", webPort)
                        .path("/webhook/path")
                        .body("_body"))))
                .get();


        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));

        assertThat(putWatchResponse.isCreated(), is(true));
        timeWarp().scheduler().trigger("_id");
        flush();
        refresh();

        // the action should fail as no email server is available
        assertWatchWithMinimumActionsCount("_id", ExecutionState.EXECUTED, 1);

        SearchResponse response = client().prepareSearch(HistoryStore.INDEX_PREFIX + "*").setSource(searchSource()
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
    }
}
