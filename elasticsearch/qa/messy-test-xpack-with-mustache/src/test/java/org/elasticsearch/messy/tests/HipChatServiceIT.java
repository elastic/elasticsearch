/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.messy.tests;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockMustacheScriptEngine;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.watcher.actions.hipchat.HipChatAction;
import org.elasticsearch.xpack.notification.hipchat.HipChatAccount;
import org.elasticsearch.xpack.notification.hipchat.HipChatMessage;
import org.elasticsearch.xpack.notification.hipchat.HipChatService;
import org.elasticsearch.xpack.notification.hipchat.SentMessages;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.watcher.actions.ActionBuilders.hipchatAction;
import static org.elasticsearch.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.watcher.condition.ConditionBuilders.alwaysCondition;
import static org.elasticsearch.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
@Network
@TestLogging("watcher.support.http:TRACE")
public class HipChatServiceIT extends AbstractWatcherIntegrationTestCase {
    @Override
    protected boolean timeWarped() {
        return true;
    }

    @Override
    protected boolean enableShield() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        Collection<Class<? extends Plugin>> mockPlugins = super.getMockPlugins();
        mockPlugins.remove(MockMustacheScriptEngine.TestPlugin.class);
        return mockPlugins;
    }

    @Override
    protected List<Class<? extends Plugin>> pluginTypes() {
        List<Class<? extends Plugin>> types = super.pluginTypes();
        types.add(MustachePlugin.class);
        return types;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))

                // this is for the `test-watcher-integration` group level integration in HipChat
                .put("xpack.notification.hipchat.account.integration_account.profile", "integration")
                .put("xpack.notification.hipchat.account.integration_account.auth_token",
                        "huuS9v7ccuOy3ZBWWWr1vt8Lqu3sQnLUE81nrLZU")
                .put("xpack.notification.hipchat.account.integration_account.room", "test-watcher")

                // this is for the Watcher Test account in HipChat
                .put("xpack.notification.hipchat.account.user_account.profile", "user")
                .put("xpack.notification.hipchat.account.user_account.auth_token", "FYVx16oDH78ZW9r13wtXbcszyoyA7oX5tiMWg9X0")

                // this is for the `test-watcher-v1` notification token
                .put("xpack.notification.hipchat.account.v1_account.profile", "v1")
                .put("xpack.notification.hipchat.account.v1_account.auth_token", "a734baf62df618b96dda55b323fc30")
                .build();
    }

    public void testSendMessageV1Account() throws Exception {
        HipChatService service = getInstanceFromMaster(HipChatService.class);
        HipChatMessage hipChatMessage = new HipChatMessage(
                "HipChatServiceTests#testSendMessage_V1Account",
                new String[] { "test-watcher", "test-watcher-2" },
                null, // users are unsupported in v1
                "watcher-tests",
                HipChatMessage.Format.TEXT,
                randomFrom(HipChatMessage.Color.values()),
                true);

        HipChatAccount account = service.getAccount("v1_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(hipChatMessage);
        assertSentMessagesAreValid(2, messages);
    }

    public void testSendMessageIntegrationAccount() throws Exception {
        HipChatService service = getInstanceFromMaster(HipChatService.class);
        HipChatMessage.Color color = randomFrom(HipChatMessage.Color.values());
        HipChatMessage hipChatMessage = new HipChatMessage(
                "HipChatServiceTests#testSendMessage_IntegrationAccount colored " + color.value(),
                null, // custom rooms are unsupported by integration profiles
                null, // users are unsupported by integration profiles
                null, // custom "from" is not supported by integration profiles
                HipChatMessage.Format.TEXT,
                color,
                true);

        HipChatAccount account = service.getAccount("integration_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(hipChatMessage);
        assertSentMessagesAreValid(1, messages);
    }

    public void testSendMessageUserAccount() throws Exception {
        HipChatService service = getInstanceFromMaster(HipChatService.class);
        HipChatMessage.Color color = randomFrom(HipChatMessage.Color.values());
        HipChatMessage hipChatMessage = new HipChatMessage(
                "HipChatServiceTests#testSendMessage_UserAccount colored " + color.value(),
                new String[] { "test-watcher", "test-watcher-2" },
                new String[] { "watcher@elastic.co" },
                null, // custom "from" is not supported by integration profiles
                HipChatMessage.Format.TEXT,
                color,
                false);

        HipChatAccount account = service.getAccount("user_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(hipChatMessage);
        assertSentMessagesAreValid(3, messages);
    }

    public void testWatchWithHipChatAction() throws Exception {
        HipChatAccount.Profile profile = randomFrom(HipChatAccount.Profile.values());
        HipChatMessage.Color color = randomFrom(HipChatMessage.Color.values());
        String account;
        HipChatAction.Builder actionBuilder;
        switch (profile) {
            case USER:
                account = "user_account";
                actionBuilder = hipchatAction(account, "{{ctx.payload.ref}}")
                        .addRooms("test-watcher", "test-watcher-2")
                        .addUsers("watcher@elastic.co")
                        .setFormat(HipChatMessage.Format.TEXT)
                        .setColor(color)
                        .setNotify(false);
                break;

            case INTEGRATION:
                account = "integration_account";
                actionBuilder = hipchatAction(account, "{{ctx.payload.ref}}")
                        .setFormat(HipChatMessage.Format.TEXT)
                        .setColor(color)
                        .setNotify(false);
                break;

            default:
                assertThat(profile, is(HipChatAccount.Profile.V1));
                account = "v1_account";
                actionBuilder = hipchatAction(account, "{{ctx.payload.ref}}")
                        .addRooms("test-watcher", "test-watcher-2")
                        .setFrom("watcher-test")
                        .setFormat(HipChatMessage.Format.TEXT)
                        .setColor(color)
                        .setNotify(false);
        }

        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("1").setSource(watchBuilder()
                .trigger(schedule(interval("10m")))
                .input(simpleInput("ref", "HipChatServiceTests#testWatchWithHipChatAction"))
                .condition(alwaysCondition())
                .addAction("hipchat", actionBuilder))
                .execute().get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("1");
        flush();
        refresh();

        assertWatchWithMinimumPerformedActionsCount("1", 1L, false);

        SearchResponse response = searchHistory(searchSource().query(boolQuery()
                .must(termQuery("result.actions.id", "hipchat"))
                .must(termQuery("result.actions.type", "hipchat"))
                .must(termQuery("result.actions.status", "success"))
                .must(termQuery("result.actions.hipchat.account", account))
                .must(termQuery("result.actions.hipchat.sent_messages.status", "success"))));

        assertThat(response, notNullValue());
        assertThat(response.getHits().getTotalHits(), is(1L));
    }

    public void testDefaultValuesForColorAndFormatWorks() {
        HipChatService service = getInstanceFromMaster(HipChatService.class);
        HipChatMessage hipChatMessage = new HipChatMessage(
                "HipChatServiceTests#testSendMessage_UserAccount with default Color and text",
                new String[] { "test-watcher" },
                new String[] { "watcher@elastic.co" },
                null, // custom "from" is not supported by integration profiles
                null,
                null,
                false);

        HipChatAccount account = service.getAccount("user_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(hipChatMessage);
        assertSentMessagesAreValid(2, messages);
    }

    private void assertSentMessagesAreValid(int expectedMessageSize, SentMessages messages) {
        assertThat(messages.count(), is(expectedMessageSize));
        for (SentMessages.SentMessage message : messages) {
            assertThat("Expected no failures, but got [" + message.getFailureReason() + "]", message.successful(), is(true));
            assertThat(message.getRequest(), notNullValue());
            assertThat(message.getResponse(), notNullValue());
            assertThat(message.getResponse().status(), lessThan(300));
        }
    }
}
