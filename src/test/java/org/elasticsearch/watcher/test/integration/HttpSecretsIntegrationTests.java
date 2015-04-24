/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.test.integration;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.shield.ShieldSecretService;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.http.auth.basic.ApplicableBasicAuth;
import org.elasticsearch.watcher.support.http.auth.basic.BasicAuth;
import org.elasticsearch.watcher.support.secret.SecretService;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTests;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.watcher.watch.WatchStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.BindException;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.watcher.actions.ActionBuilders.webhookAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class HttpSecretsIntegrationTests extends AbstractWatcherIntegrationTests {

    static final String USERNAME = "_user";
    static final String PASSWORD = "_passwd";

    private MockWebServer webServer;
    private static Boolean encryptSensitiveData;

    @Before
    public void init() throws Exception {
        for (int webPort = 9200; webPort < 9300; webPort++) {
            try {
                webServer = new MockWebServer();
                webServer.start(webPort);
                return;
            } catch (BindException be) {
                logger.warn("port [{}] was already in use trying next port", webPort);
            }
        }
        throw new WatcherException("unable to find open port between 9200 and 9300");
    }

    @After
    public void cleanup() throws Exception {
        webServer.shutdown();
    }



    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (encryptSensitiveData == null) {
            encryptSensitiveData = shieldEnabled() && randomBoolean();
        }
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("watcher.shield.encrypt_sensitive_data", encryptSensitiveData)
                .build();
    }

    @Test
    public void testHttpInput() throws Exception {
        WatcherClient watcherClient = watcherClient();
        watcherClient.preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0 0 0 1 * ? 2020")))
                        .input(httpInput(HttpRequestTemplate.builder(webServer.getHostName(), webServer.getPort())
                                .path("/")
                                .auth(new BasicAuth(USERNAME, PASSWORD.toCharArray()))))
                        .condition(alwaysCondition())
                        .addAction("_logging", loggingAction("executed")))
                        .get();

        // verifying the basic auth password is stored encrypted in the index when shield
        // is enabled, and when it's not enabled, it's stored in plain text
        GetResponse response = client().prepareGet(WatchStore.INDEX, WatchStore.DOC_TYPE, "_id").get();
        assertThat(response, notNullValue());
        assertThat(response.getId(), is("_id"));
        Map<String, Object> source = response.getSource();
        Object value = XContentMapValues.extractValue("input.http.request.auth.basic.password", source);
        assertThat(value, notNullValue());
        if (shieldEnabled() && encryptSensitiveData) {
            assertThat(value, not(is((Object) PASSWORD)));
            SecretService secretService = getInstanceFromMaster(SecretService.class);
            assertThat(secretService, instanceOf(ShieldSecretService.class));
            assertThat(new String(secretService.decrypt(((String) value).toCharArray())), is(PASSWORD));
        } else {
            assertThat(value, is((Object) PASSWORD));
            SecretService secretService = getInstanceFromMaster(SecretService.class);
            if (shieldEnabled()) {
                assertThat(secretService, instanceOf(ShieldSecretService.class));
            } else {
                assertThat(secretService, instanceOf(SecretService.PlainText.class));
            }
            assertThat(new String(secretService.decrypt(((String) value).toCharArray())), is(PASSWORD));
        }

        // verifying the password is not returned by the GET watch API
        GetWatchResponse watchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(watchResponse, notNullValue());
        assertThat(watchResponse.getId(), is("_id"));
        source = watchResponse.getSourceAsMap();
        value = XContentMapValues.extractValue("input.http.request.auth.basic", source);
        assertThat(value, notNullValue()); // making sure we have the basic auth
        value = XContentMapValues.extractValue("input.http.request.auth.basic.password", source);
        assertThat(value, nullValue()); // and yet we don't have the password

        // now we restart, to make sure the watches and their secrets are reloaded from the index properly
        assertThat(watcherClient.prepareWatchService().restart().get().isAcknowledged(), is(true));
        ensureWatcherStarted();

        // now lets execute the watch manually

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(jsonBuilder().startObject().field("key", "value").endObject().bytes().toUtf8()));

        ExecuteWatchResponse executeResponse = watcherClient.prepareExecuteWatch("_id")
                .setRecordExecution(false)
                .setIgnoreThrottle(true)
                .get();
        assertThat(executeResponse, notNullValue());
        source = executeResponse.getWatchRecordAsMap();
        value = XContentMapValues.extractValue("watch_execution.input_result.http.http_status", source);
        assertThat(value, notNullValue());
        assertThat(value, is((Object) 200));

        RecordedRequest request = webServer.takeRequest();
        assertThat(request.getHeader("Authorization"), equalTo(ApplicableBasicAuth.headerValue(USERNAME, PASSWORD.toCharArray())));
    }

    @Test
    public void testWebhookAction() throws Exception {
        WatcherClient watcherClient = watcherClient();
        watcherClient.preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0 0 0 1 * ? 2020")))
                        .input(simpleInput())
                        .condition(alwaysCondition())
                        .addAction("_webhook", webhookAction(HttpRequestTemplate.builder(webServer.getHostName(), webServer.getPort())
                                .path("/")
                                .auth(new BasicAuth(USERNAME, PASSWORD.toCharArray())))))
                        .get();

        // verifying the basic auth password is stored encrypted in the index when shield
        // is enabled, when it's not enabled, the the passowrd should be stored in plain text
        GetResponse response = client().prepareGet(WatchStore.INDEX, WatchStore.DOC_TYPE, "_id").get();
        assertThat(response, notNullValue());
        assertThat(response.getId(), is("_id"));
        Map<String, Object> source = response.getSource();
        Object value = XContentMapValues.extractValue("actions._webhook.webhook.auth.basic.password", source);
        assertThat(value, notNullValue());

        if (shieldEnabled() && encryptSensitiveData) {
            assertThat(value, not(is((Object) PASSWORD)));
            SecretService secretService = getInstanceFromMaster(SecretService.class);
            assertThat(secretService, instanceOf(ShieldSecretService.class));
            assertThat(new String(secretService.decrypt(((String) value).toCharArray())), is(PASSWORD));
        } else {
            assertThat(value, is((Object) PASSWORD));
            SecretService secretService = getInstanceFromMaster(SecretService.class);
            if (shieldEnabled()) {
                assertThat(secretService, instanceOf(ShieldSecretService.class));
            } else {
                assertThat(secretService, instanceOf(SecretService.PlainText.class));
            }
            assertThat(new String(secretService.decrypt(((String) value).toCharArray())), is(PASSWORD));
        }

        // verifying the password is not returned by the GET watch API
        GetWatchResponse watchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(watchResponse, notNullValue());
        assertThat(watchResponse.getId(), is("_id"));
        source = watchResponse.getSourceAsMap();
        value = XContentMapValues.extractValue("actions._webhook.webhook.auth.basic", source);
        assertThat(value, notNullValue()); // making sure we have the basic auth
        value = XContentMapValues.extractValue("actions._webhook.webhook.auth.basic.password", source);
        assertThat(value, nullValue()); // and yet we don't have the password

        // now we restart, to make sure the watches and their secrets are reloaded from the index properly
        assertThat(watcherClient.prepareWatchService().restart().get().isAcknowledged(), is(true));
        ensureWatcherStarted();

        // now lets execute the watch manually

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(jsonBuilder().startObject().field("key", "value").endObject().bytes().toUtf8()));

        ExecuteWatchResponse executeResponse = watcherClient.prepareExecuteWatch("_id")
                .setRecordExecution(false)
                .setIgnoreThrottle(true)
                .get();
        assertThat(executeResponse, notNullValue());
        source = executeResponse.getWatchRecordAsMap();
        value = XContentMapValues.extractValue("watch_execution.actions_results._webhook.webhook.response.status", source);
        assertThat(value, notNullValue());
        assertThat(value, is((Object) 200));
        value = XContentMapValues.extractValue("watch_execution.actions_results._webhook.webhook.request.auth.username", source);
        assertThat(value, notNullValue()); // the auth username exists
        value = XContentMapValues.extractValue("watch_execution.actions_results._webhook.webhook.request.auth.password", source);
        assertThat(value, nullValue()); // but the auth password was filtered out

        RecordedRequest request = webServer.takeRequest();
        assertThat(request.getHeader("Authorization"), equalTo(ApplicableBasicAuth.headerValue(USERNAME, PASSWORD.toCharArray())));
    }
}
