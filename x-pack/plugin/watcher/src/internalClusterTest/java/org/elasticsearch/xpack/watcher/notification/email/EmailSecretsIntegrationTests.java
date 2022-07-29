/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
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
import org.elasticsearch.xpack.watcher.actions.ActionBuilders;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.notification.email.support.EmailServer;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.trigger.schedule.ScheduleTriggerEvent;
import org.junit.After;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class EmailSecretsIntegrationTests extends AbstractWatcherIntegrationTestCase {
    private EmailServer server;
    private Boolean encryptSensitiveData;
    private byte[] encryptionKey;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        server = EmailServer.localhost(logger);
    }

    @After
    public void cleanup() throws Exception {
        server.stop();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        if (encryptSensitiveData == null) {
            encryptSensitiveData = randomBoolean();
            if (encryptSensitiveData) {
                encryptionKey = CryptoServiceTests.generateKey();
            }
        }
        Settings.Builder builder = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("xpack.notification.email.account.test.smtp.auth", true)
            .put("xpack.notification.email.account.test.smtp.port", server.port())
            .put("xpack.notification.email.account.test.smtp.host", "localhost")
            .put("xpack.watcher.encrypt_sensitive_data", encryptSensitiveData);
        if (encryptSensitiveData) {
            MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setFile(WatcherField.ENCRYPTION_KEY_SETTING.getKey(), encryptionKey);
            builder.setSecureSettings(secureSettings);
        }
        return builder.build();
    }

    public void testEmail() throws Exception {
        new PutWatchRequestBuilder(client(), "_id").setSource(
            watchBuilder().trigger(schedule(cron("0 0 0 1 * ? 2020")))
                .input(simpleInput())
                .condition(InternalAlwaysCondition.INSTANCE)
                .addAction(
                    "_email",
                    ActionBuilders.emailAction(EmailTemplate.builder().from("from@example.org").to("to@example.org").subject("_subject"))
                        .setAuthentication(EmailServer.USERNAME, EmailServer.PASSWORD.toCharArray())
                )
        ).get();

        // verifying the email password is stored encrypted in the index
        GetResponse response = client().prepareGet().setIndex(Watch.INDEX).setId("_id").get();
        assertThat(response, notNullValue());
        assertThat(response.getId(), is("_id"));
        Map<String, Object> source = response.getSource();
        Object value = XContentMapValues.extractValue("actions._email.email.password", source);
        assertThat(value, notNullValue());
        if (encryptSensitiveData) {
            assertThat(value, not(is(EmailServer.PASSWORD)));
            MockSecureSettings mockSecureSettings = new MockSecureSettings();
            mockSecureSettings.setFile(WatcherField.ENCRYPTION_KEY_SETTING.getKey(), encryptionKey);
            Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
            CryptoService cryptoService = new CryptoService(settings);
            assertThat(new String(cryptoService.decrypt(((String) value).toCharArray())), is(EmailServer.PASSWORD));
        } else {
            assertThat(value, is(EmailServer.PASSWORD));
        }

        // verifying the password is not returned by the GET watch API
        GetWatchResponse watchResponse = new GetWatchRequestBuilder(client(), "_id").get();
        assertThat(watchResponse, notNullValue());
        assertThat(watchResponse.getId(), is("_id"));
        XContentSource contentSource = watchResponse.getSource();
        value = contentSource.getValue("actions._email.email.password");
        if (encryptSensitiveData) {
            assertThat(value.toString(), startsWith("::es_encrypted::"));
        } else {
            assertThat(value, is("::es_redacted::"));
        }

        // now we restart, to make sure the watches and their secrets are reloaded from the index properly
        stopWatcher();
        startWatcher();

        // now lets execute the watch manually
        final CountDownLatch latch = new CountDownLatch(1);
        server.addListener(message -> {
            assertThat(message.getSubject(), is("_subject"));
            latch.countDown();
        });

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(ZonedDateTime.now(ZoneOffset.UTC), ZonedDateTime.now(ZoneOffset.UTC));
        ExecuteWatchResponse executeResponse = new ExecuteWatchRequestBuilder(client(), "_id").setRecordExecution(false)
            .setTriggerEvent(triggerEvent)
            .setActionMode("_all", ActionExecutionMode.FORCE_EXECUTE)
            .get();
        assertThat(executeResponse, notNullValue());
        contentSource = executeResponse.getRecordSource();

        value = contentSource.getValue("result.actions.0.status");
        assertThat(value, is("success"));

        if (latch.await(5, TimeUnit.SECONDS) == false) {
            fail("waiting too long for the email to be sent");
        }
    }
}
