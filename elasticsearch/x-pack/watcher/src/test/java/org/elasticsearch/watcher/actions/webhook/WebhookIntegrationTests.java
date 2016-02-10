/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.webhook;


import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.QueueDispatcher;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.watcher.actions.ActionBuilders;
import org.elasticsearch.watcher.history.WatchRecord;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuth;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.support.xcontent.XContentSource;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.junit.After;
import org.junit.Before;

import java.net.BindException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.test.WatcherTestUtils.xContentSource;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class WebhookIntegrationTests extends AbstractWatcherIntegrationTestCase {
    private int webPort;
    private MockWebServer webServer;

    @Before
    public void startWebservice() throws Exception {
        for (webPort = 9250; webPort < 9300; webPort++) {
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

    public void testWebhook() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("localhost", webPort)
                .path(TextTemplate.inline("/test/_id"))
                .putParam("param1", TextTemplate.inline("value1"))
                .putParam("watch_id", TextTemplate.inline("_id"))
                .body(TextTemplate.inline("_body"));

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
        assertThat(recordedRequest.getPath(),
                anyOf(equalTo("/test/_id?watch_id=_id&param1=value1"), equalTo("/test/_id?param1=value1&watch_id=_id")));
        assertThat(recordedRequest.getBody().readUtf8Line(), equalTo("_body"));

        SearchResponse response = searchWatchRecords(new Callback<SearchRequestBuilder>() {
            @Override
            public void handle(SearchRequestBuilder builder) {
                QueryBuilders.termQuery(WatchRecord.Field.STATE.getPreferredName(), "executed");
            }
        });

        assertNoFailures(response);
        XContentSource source = xContentSource(response.getHits().getAt(0).getSourceRef());
        String body = source.getValue("result.actions.0.webhook.response.body");
        assertThat(body, notNullValue());
        assertThat(body, is("body"));
        Number status = source.getValue("result.actions.0.webhook.response.status");
        assertThat(status, notNullValue());
        assertThat(status.intValue(), is(200));
    }

    public void testWebhookWithBasicAuth() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody("body"));
        HttpRequestTemplate.Builder builder = HttpRequestTemplate.builder("localhost", webPort)
                .auth(new BasicAuth("_username", "_password".toCharArray()))
                .path(TextTemplate.inline("/test/_id").build())
                .putParam("param1", TextTemplate.inline("value1").build())
                .putParam("watch_id", TextTemplate.inline("_id").build())
                .body(TextTemplate.inline("_body").build());

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
        assertThat(recordedRequest.getPath(),
                anyOf(equalTo("/test/_id?watch_id=_id&param1=value1"), equalTo("/test/_id?param1=value1&watch_id=_id")));
        assertThat(recordedRequest.getBody().readUtf8Line(), equalTo("_body"));
        assertThat(recordedRequest.getHeader("Authorization"), equalTo("Basic X3VzZXJuYW1lOl9wYXNzd29yZA=="));
    }
}
