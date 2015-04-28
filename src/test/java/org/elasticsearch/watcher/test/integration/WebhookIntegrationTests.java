/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.QueueDispatcher;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.watcher.actions.ActionBuilders;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuth;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.BindException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class WebhookIntegrationTests extends AbstractWatcherIntegrationTests {

    private int webPort;
    private MockWebServer webServer;

    @Before
    public void startWebservice() throws Exception {
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
    public void stopWebservice() throws Exception {
        webServer.shutdown();
    }

    @Test
    public void testWebhook() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("localhost", webPort)
                .path(Template.builder("/test/{{ctx.watch_id}}").build())
                .putParam("param1", Template.builder("value1").build())
                .putParam("watch_id", Template.builder("{{ctx.watch_id}}").build())
                .body(Template.builder("{{ctx.payload}}").build());

        watcherClient().preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(simpleInput("key", "value"))
                        .condition(alwaysCondition())
                        .addAction("_id", ActionBuilders.webhookAction(builder)))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_id");
            refresh();
        }

        assertWatchWithMinimumPerformedActionsCount("_id", 1, false);
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), anyOf(equalTo("/test/_id?watch_id=_id&param1=value1"), equalTo("/test/_id?param1=value1&watch_id=_id")));
        assertThat(recordedRequest.getBody().readUtf8Line(), equalTo("{key=value}"));

        SearchResponse response = client().prepareSearch(HistoryStore.INDEX_PREFIX + "*")
                .setQuery(QueryBuilders.termQuery(WatchRecord.Parser.STATE_FIELD.getPreferredName(), "executed"))
                .get();
        assertNoFailures(response);
        assertThat(XContentMapValues.extractValue("watch_execution.actions_results._id.webhook.response.body", response.getHits().getAt(0).sourceAsMap()).toString(), equalTo("body"));
        assertThat(XContentMapValues.extractValue("watch_execution.actions_results._id.webhook.response.status", response.getHits().getAt(0).sourceAsMap()).toString(), equalTo("200"));
    }

    @Test
    public void testWebhookWithBasicAuth() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("localhost", webPort)
                .auth(new BasicAuth("_username", "_password".toCharArray()))
                .path(Template.builder("/test/{{ctx.watch_id}}").build())
                .putParam("param1", Template.builder("value1").build())
                .putParam("watch_id", Template.builder("{{ctx.watch_id}}").build())
                .body(Template.builder("{{ctx.payload}}").build());

        watcherClient().preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(interval("5s")))
                        .input(simpleInput("key", "value"))
                        .condition(alwaysCondition())
                        .addAction("_id", ActionBuilders.webhookAction(builder)))
                .get();

        if (timeWarped()) {
            timeWarp().scheduler().trigger("_id");
            refresh();
        }

        assertWatchWithMinimumPerformedActionsCount("_id", 1, false);
        RecordedRequest recordedRequest = webServer.takeRequest();
        assertThat(recordedRequest.getPath(), anyOf(equalTo("/test/_id?watch_id=_id&param1=value1"), equalTo("/test/_id?param1=value1&watch_id=_id")));
        assertThat(recordedRequest.getBody().readUtf8Line(), equalTo("{key=value}"));
        assertThat(recordedRequest.getHeader("Authorization"), equalTo("Basic X3VzZXJuYW1lOl9wYXNzd29yZA=="));
    }

}
