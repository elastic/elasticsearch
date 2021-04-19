/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.core.watcher.WatcherField;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoService;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoServiceTests;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.core.watcher.transport.actions.put.PutWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.core.watcher.watch.Watch;
import org.elasticsearch.xpack.watcher.common.http.BasicAuth;
import org.elasticsearch.xpack.watcher.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.loggingAction;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.webhookAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.httpInput;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class HttpSecretsIntegrationTests extends AbstractWatcherIntegrationTestCase {

    private static final String USERNAME = "_user";
    private static final String PASSWORD = "_passwd";

    private MockWebServer webServer = new MockWebServer();
    private static Boolean encryptSensitiveData = null;
    private static final byte[] encryptionKey = CryptoServiceTests.generateKey();

    @Before
    public void init() throws Exception {
        webServer.start();
    }

    @After
    public void cleanup() {
        webServer.close();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        if (encryptSensitiveData == null) {
            encryptSensitiveData = randomBoolean();
        }
        if (encryptSensitiveData) {
            MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setFile(WatcherField.ENCRYPTION_KEY_SETTING.getKey(), encryptionKey);
            return Settings.builder()
                    .put(super.nodeSettings(nodeOrdinal, otherSettings))
                    .put("xpack.watcher.encrypt_sensitive_data", encryptSensitiveData)
                    .setSecureSettings(secureSettings)
                    .build();
        }
        return super.nodeSettings(nodeOrdinal, otherSettings);
    }

    public void testHttpInput() throws Exception {
        new PutWatchRequestBuilder(client()).setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0 0 0 1 * ? 2020")))
                        .input(httpInput(HttpRequestTemplate.builder(webServer.getHostName(), webServer.getPort())
                                .path("/")
                                .auth(new BasicAuth(USERNAME, PASSWORD.toCharArray()))))
                        .condition(InternalAlwaysCondition.INSTANCE)
                        .addAction("_logging", loggingAction("executed")))
                        .get();

        // verifying the basic auth password is stored encrypted in the index when security
        // is enabled, and when it's not enabled, it's stored in plain text
        GetResponse response = client().prepareGet().setIndex(Watch.INDEX).setId("_id").get();
        assertThat(response, notNullValue());
        assertThat(response.getId(), is("_id"));
        Map<String, Object> source = response.getSource();
        Object value = XContentMapValues.extractValue("input.http.request.auth.basic.password", source);
        assertThat(value, notNullValue());
        if (encryptSensitiveData) {
            assertThat(value.toString(), startsWith("::es_encrypted::"));
            MockSecureSettings mockSecureSettings = new MockSecureSettings();
            mockSecureSettings.setFile(WatcherField.ENCRYPTION_KEY_SETTING.getKey(), encryptionKey);
            Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
            CryptoService cryptoService = new CryptoService(settings);
            assertThat(new String(cryptoService.decrypt(((String) value).toCharArray())), is(PASSWORD));
        } else {
            assertThat(value, is(PASSWORD));
        }

        // verifying the password is not returned by the GET watch API
        GetWatchResponse watchResponse = new GetWatchRequestBuilder(client()).setId("_id").get();
        assertThat(watchResponse, notNullValue());
        assertThat(watchResponse.getId(), is("_id"));
        XContentSource contentSource = watchResponse.getSource();
        value = contentSource.getValue("input.http.request.auth.basic");
        assertThat(value, notNullValue()); // making sure we have the basic auth
        value = contentSource.getValue("input.http.request.auth.basic.password");
        if (encryptSensitiveData) {
            assertThat(value.toString(), startsWith("::es_encrypted::"));
        } else {
            assertThat(value, is("::es_redacted::"));
        }

        // now we restart, to make sure the watches and their secrets are reloaded from the index properly
        stopWatcher();
        startWatcher();

        // now lets execute the watch manually

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(
                BytesReference.bytes(jsonBuilder().startObject().field("key", "value").endObject()).utf8ToString()));

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC));
        ExecuteWatchResponse executeResponse = new ExecuteWatchRequestBuilder(client()).setId("_id")
                .setRecordExecution(false)
                .setTriggerEvent(triggerEvent)
                .setActionMode("_all", ActionExecutionMode.FORCE_EXECUTE)
                .get();
        assertThat(executeResponse, notNullValue());
        contentSource = executeResponse.getRecordSource();
        value = contentSource.getValue("result.input.http.status_code");
        assertThat(value, notNullValue());
        assertThat(value, is((Object) 200));

        assertThat(webServer.requests(), hasSize(1));
        assertThat(webServer.requests().get(0).getHeader("Authorization"),
                is(headerValue(USERNAME, PASSWORD.toCharArray())));

        // now trigger the by the scheduler and make sure that the password is also correctly transmitted
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(
            BytesReference.bytes(jsonBuilder().startObject().field("key", "value").endObject()).utf8ToString()));
        timeWarp().trigger("_id");
        assertThat(webServer.requests(), hasSize(2));
        assertThat(webServer.requests().get(1).getHeader("Authorization"),
            is(headerValue(USERNAME, PASSWORD.toCharArray())));
    }

    public void testWebhookAction() throws Exception {
        new PutWatchRequestBuilder(client()).setId("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0 0 0 1 * ? 2020")))
                        .input(simpleInput())
                        .condition(InternalAlwaysCondition.INSTANCE)
                        .addAction("_webhook", webhookAction(HttpRequestTemplate.builder(webServer.getHostName(), webServer.getPort())
                                .path("/")
                                .auth(new BasicAuth(USERNAME, PASSWORD.toCharArray())))))
                        .get();

        // verifying the basic auth password is stored encrypted in the index when security
        // is enabled, when it's not enabled, the password should be stored in plain text
        GetResponse response = client().prepareGet().setIndex(Watch.INDEX).setId("_id").get();
        assertThat(response, notNullValue());
        assertThat(response.getId(), is("_id"));
        Map<String, Object> source = response.getSource();
        Object value = XContentMapValues.extractValue("actions._webhook.webhook.auth.basic.password", source);
        assertThat(value, notNullValue());

        if (encryptSensitiveData) {
            assertThat(value, not(is((Object) PASSWORD)));
            MockSecureSettings mockSecureSettings = new MockSecureSettings();
            mockSecureSettings.setFile(WatcherField.ENCRYPTION_KEY_SETTING.getKey(), encryptionKey);
            Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
            CryptoService cryptoService = new CryptoService(settings);
            assertThat(new String(cryptoService.decrypt(((String) value).toCharArray())), is(PASSWORD));
        } else {
            assertThat(value, is((Object) PASSWORD));
        }

        // verifying the password is not returned by the GET watch API
        GetWatchResponse watchResponse = new GetWatchRequestBuilder(client()).setId("_id").get();
        assertThat(watchResponse, notNullValue());
        assertThat(watchResponse.getId(), is("_id"));
        XContentSource contentSource = watchResponse.getSource();
        value = contentSource.getValue("actions._webhook.webhook.auth.basic");
        assertThat(value, notNullValue()); // making sure we have the basic auth
        value = contentSource.getValue("actions._webhook.webhook.auth.basic.password");
        if (encryptSensitiveData) {
            assertThat(value.toString(), startsWith("::es_encrypted::"));
        } else {
            assertThat(value, is("::es_redacted::"));
        }

        // now we restart, to make sure the watches and their secrets are reloaded from the index properly
        stopWatcher();
        startWatcher();

        // now lets execute the watch manually

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(
                BytesReference.bytes(jsonBuilder().startObject().field("key", "value").endObject()).utf8ToString()));

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC));
        ExecuteWatchResponse executeResponse = new ExecuteWatchRequestBuilder(client()).setId("_id")
                .setRecordExecution(false)
                .setActionMode("_all", ActionExecutionMode.FORCE_EXECUTE)
                .setTriggerEvent(triggerEvent)
                .get();
        assertThat(executeResponse, notNullValue());

        contentSource = executeResponse.getRecordSource();

        assertThat(contentSource.getValue("result.actions.0.status"), is("success"));

        value = contentSource.getValue("result.actions.0.webhook.response.status");
        assertThat(value, notNullValue());
        assertThat(value, instanceOf(Number.class));
        assertThat(((Number) value).intValue(), is(200));

        value = contentSource.getValue("result.actions.0.webhook.request.auth.basic.username");
        assertThat(value, notNullValue());
        assertThat(value, instanceOf(String.class));
        assertThat(value, is(USERNAME)); // the auth username exists

        value = contentSource.getValue("result.actions.0.webhook.request.auth.basic.password");
        if (encryptSensitiveData) {
            assertThat(value.toString(), startsWith("::es_encrypted::"));
        } else {
            assertThat(value.toString(), is("::es_redacted::"));
        }

        assertThat(webServer.requests(), hasSize(1));
        assertThat(webServer.requests().get(0).getHeader("Authorization"),
                is(headerValue(USERNAME, PASSWORD.toCharArray())));
    }

    private String headerValue(String username, char[] password) {
        return "Basic " + Base64.getEncoder().encodeToString((username + ":" + new String(password)).getBytes(StandardCharsets.UTF_8));
    }
}
