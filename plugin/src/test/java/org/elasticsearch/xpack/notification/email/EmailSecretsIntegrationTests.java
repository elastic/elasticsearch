/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.email;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.notification.email.support.EmailServer;
import org.elasticsearch.xpack.security.crypto.CryptoService;
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
import org.joda.time.DateTimeZone;
import org.junit.After;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.emailAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.cron;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class EmailSecretsIntegrationTests extends AbstractWatcherIntegrationTestCase {

    private EmailServer server;
    private Boolean encryptSensitiveData;

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
    protected Settings nodeSettings(int nodeOrdinal) {
        if (encryptSensitiveData == null) {
            encryptSensitiveData = securityEnabled() && randomBoolean();
        }
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("xpack.notification.email.account.test.smtp.auth", true)
                .put("xpack.notification.email.account.test.smtp.port", server.port())
                .put("xpack.notification.email.account.test.smtp.host", "localhost")
                .put("xpack.watcher.encrypt_sensitive_data", encryptSensitiveData)
                .build();
    }

    public void testEmail() throws Exception {
        WatcherClient watcherClient = watcherClient();
        watcherClient.preparePutWatch("_id")
                .setSource(watchBuilder()
                        .trigger(schedule(cron("0 0 0 1 * ? 2020")))
                        .input(simpleInput())
                        .condition(AlwaysCondition.INSTANCE)
                        .addAction("_email", emailAction(
                                EmailTemplate.builder()
                                        .from("_from")
                                        .to("_to")
                                        .subject("_subject"))
                                .setAuthentication(EmailServer.USERNAME, EmailServer.PASSWORD.toCharArray())))
                .get();

        // verifying the email password is stored encrypted in the index
        GetResponse response = client().prepareGet(Watch.INDEX, Watch.DOC_TYPE, "_id").get();
        assertThat(response, notNullValue());
        assertThat(response.getId(), is("_id"));
        Map<String, Object> source = response.getSource();
        Object value = XContentMapValues.extractValue("actions._email.email.password", source);
        assertThat(value, notNullValue());
        if (securityEnabled() && encryptSensitiveData) {
            assertThat(value, not(is(EmailServer.PASSWORD)));
            CryptoService cryptoService = getInstanceFromMaster(CryptoService.class);
            assertThat(new String(cryptoService.decrypt(((String) value).toCharArray())), is(EmailServer.PASSWORD));
        } else {
            assertThat(value, is(EmailServer.PASSWORD));
        }

        // verifying the password is not returned by the GET watch API
        GetWatchResponse watchResponse = watcherClient.prepareGetWatch("_id").get();
        assertThat(watchResponse, notNullValue());
        assertThat(watchResponse.getId(), is("_id"));
        XContentSource contentSource = watchResponse.getSource();
        value = contentSource.getValue("actions._email.email.password");
        assertThat(value, nullValue());

        // now we restart, to make sure the watches and their secrets are reloaded from the index properly
        assertThat(watcherClient.prepareWatchService().restart().get().isAcknowledged(), is(true));
        ensureWatcherStarted();

        // now lets execute the watch manually

        final CountDownLatch latch = new CountDownLatch(1);
        server.addListener(message -> {
            assertThat(message.getSubject(), is("_subject"));
            latch.countDown();
        });

        TriggerEvent triggerEvent = new ScheduleTriggerEvent(new DateTime(DateTimeZone.UTC), new DateTime(DateTimeZone.UTC));
        ExecuteWatchResponse executeResponse = watcherClient.prepareExecuteWatch("_id")
                .setRecordExecution(false)
                .setTriggerEvent(triggerEvent)
                .setActionMode("_all", ActionExecutionMode.FORCE_EXECUTE)
                .get();
        assertThat(executeResponse, notNullValue());
        contentSource = executeResponse.getRecordSource();

        value = contentSource.getValue("result.actions.0.status");
        assertThat(value, is("success"));

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("waiting too long for the email to be sent");
        }
    }
}
