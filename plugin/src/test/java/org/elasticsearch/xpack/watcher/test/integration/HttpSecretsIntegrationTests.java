/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.xpack.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.common.http.auth.basic.ApplicableBasicAuth;
import org.elasticsearch.xpack.common.http.auth.basic.BasicAuth;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.watcher.transport.actions.get.GetWatchResponse;
import org.elasticsearch.xpack.watcher.trigger.TriggerEvent;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.elasticsearch.xpack.watcher.watch.Watch;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;

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
import static org.hamcrest.Matchers.nullValue;
import static org.joda.time.DateTimeZone.UTC;

public class HttpSecretsIntegrationTests extends AbstractWatcherIntegrationTestCase {

    static final String USERNAME = "_user";
    static final String PASSWORD = "_passwd";

    private MockWebServer webServer = new MockWebServer();
    private static Boolean encryptSensitiveData;
    private static byte[] encryptionKey;

    @Before
    public void init() throws Exception {
        webServer.start();
    }

    @After
    public void cleanup() throws Exception {
        webServer.close();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (encryptSensitiveData == null) {
            encryptSensitiveData = randomBoolean();
            if (encryptSensitiveData) {
                encryptionKey = CryptoService.generateKey();
            }
        }
        if (encryptSensitiveData) {
            MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setFile(Watcher.ENCRYPTION_KEY_SETTING.getKey(), encryptionKey);
            return Settings.builder()
                    .put(super.nodeSettings(nodeOrdinal))
                    .put("xpack.watcher.encrypt_sensitive_data", encryptSensitiveData)
                    .setSecureSettings(secureSettings)
                    .build();
        }
        return super.nodeSettings(nodeOrdinal);
    }

    public void testHttpInput() throws Exception {
        WatcherClient watcherClient = watcherClient();
        watcherClient.preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0 0 0 1 * ? 2020")))
                        .input(httpInput(HttpRequestTemplate.builder(webServer.getHostName(), webServer.getPort())
                                .path("/")
                                .auth(new BasicAuth(USERNAME, PASSWORD.toCharArray()))))
                        .condition(AlwaysCondition.INSTANCE)
                        .addAction("_logging", loggingAction("executed")))
                        .get();

        // verifying the basic auth password is stored encrypted in the index when security
        // is enabled, and when it's not enabled, it's stored in plain text
        GetResponse response = client().prepareGet(Watch.INDEX, Watch.DOC_TYPE, "_id").get();
        assertThat(response, notNullValue());
        assertThat(response.getId(), is("_id"));
        Map<String, Object> source = response.getSource();
        Object value = XContentMapValues.extractValue("input.http.request.auth.basic.password", source);
        assertThat(value, notNullValue());
        if (encryptSensitiveData) {
            assertThat(value, not(is((Object) PASSWORD)));
            MockSecureSettings mockSecureSettings = new MockSecureSettings();
            mockSecureSettings.setFile(Watcher.ENCRYPTION_KEY_SETTING.getKey(), encryptionKey);
            Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
            CryptoService cryptoService = new CryptoService(settings);
            assertThat(new String(cryptoService.decrypt(((String) value).toCharArray())), is(PASSWORD));
        } else {
            assertThat(value, is((Object) PASSWORD));
        }

        // verifying the password is not returned by the GET watch API
        GetWatchResponse watchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(watchResponse, notNullValue());
        assertThat(watchResponse.getId(), is("_id"));
        XContentSource contentSource = watchResponse.getSource();
        value = contentSource.getValue("input.http.request.auth.basic");
        assertThat(value, notNullValue()); // making sure we have the basic auth
        value = contentSource.getValue("input.http.request.auth.basic.password");
        assertThat(value, nullValue()); // and yet we don't have the password

        // now we restart, to make sure the watches and their secrets are reloaded from the index properly
        stopWatcher();
        startWatcher();

        // now lets execute the watch manually

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(
                jsonBuilder().startObject().field("key", "value").endObject().bytes().utf8ToString()));

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(new DateTime(UTC), new DateTime(UTC));
        ExecuteWatchResponse executeResponse = watcherClient.prepareExecuteWatch("_id")
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
                is(ApplicableBasicAuth.headerValue(USERNAME, PASSWORD.toCharArray())));
    }

    public void testWebhookAction() throws Exception {
        WatcherClient watcherClient = watcherClient();
        watcherClient.preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0 0 0 1 * ? 2020")))
                        .input(simpleInput())
                        .condition(AlwaysCondition.INSTANCE)
                        .addAction("_webhook", webhookAction(HttpRequestTemplate.builder(webServer.getHostName(), webServer.getPort())
                                .path("/")
                                .auth(new BasicAuth(USERNAME, PASSWORD.toCharArray())))))
                        .get();

        // verifying the basic auth password is stored encrypted in the index when security
        // is enabled, when it's not enabled, the the passowrd should be stored in plain text
        GetResponse response = client().prepareGet(Watch.INDEX, Watch.DOC_TYPE, "_id").get();
        assertThat(response, notNullValue());
        assertThat(response.getId(), is("_id"));
        Map<String, Object> source = response.getSource();
        Object value = XContentMapValues.extractValue("actions._webhook.webhook.auth.basic.password", source);
        assertThat(value, notNullValue());

        if (encryptSensitiveData) {
            assertThat(value, not(is((Object) PASSWORD)));
            MockSecureSettings mockSecureSettings = new MockSecureSettings();
            mockSecureSettings.setFile(Watcher.ENCRYPTION_KEY_SETTING.getKey(), encryptionKey);
            Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
            CryptoService cryptoService = new CryptoService(settings);
            assertThat(new String(cryptoService.decrypt(((String) value).toCharArray())), is(PASSWORD));
        } else {
            assertThat(value, is((Object) PASSWORD));
        }

        // verifying the password is not returned by the GET watch API
        GetWatchResponse watchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(watchResponse, notNullValue());
        assertThat(watchResponse.getId(), is("_id"));
        XContentSource contentSource = watchResponse.getSource();
        value = contentSource.getValue("actions._webhook.webhook.auth.basic");
        assertThat(value, notNullValue()); // making sure we have the basic auth
        value = contentSource.getValue("actions._webhook.webhook.auth.basic.password");
        assertThat(value, nullValue()); // and yet we don't have the password

        // now we restart, to make sure the watches and their secrets are reloaded from the index properly
        stopWatcher();
        startWatcher();

        // now lets execute the watch manually

        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(
                jsonBuilder().startObject().field("key", "value").endObject().bytes().utf8ToString()));

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(new DateTime(UTC), new DateTime(UTC));
        ExecuteWatchResponse executeResponse = watcherClient.prepareExecuteWatch("_id")
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
        assertThat(value, nullValue()); // but the auth password was filtered out

        assertThat(webServer.requests(), hasSize(1));
        assertThat(webServer.requests().get(0).getHeader("Authorization"),
                is(ApplicableBasicAuth.headerValue(USERNAME, PASSWORD.toCharArray())));
    }
}
