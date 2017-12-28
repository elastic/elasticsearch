/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockMustacheScriptEngine;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.XPackSingleNodeTestCase;
import org.elasticsearch.xpack.watcher.WatcherService;
import org.elasticsearch.xpack.watcher.WatcherState;
import org.elasticsearch.xpack.watcher.actions.hipchat.HipChatAction;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.condition.InternalAlwaysCondition;
import org.elasticsearch.xpack.watcher.history.HistoryStore;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatAccount;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatMessage;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatService;
import org.elasticsearch.xpack.watcher.notification.hipchat.SentMessages;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.hipchatAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

@Network
@TestLogging("org.elasticsearch.xpack.watcher.common.http:TRACE")
public class HipChatServiceTests extends XPackSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(XPackPlugin.class, MockMustacheScriptEngine.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
                .put(super.nodeSettings())
                .put(XPackSettings.WATCHER_ENABLED.getKey(), true)
                .put(XPackSettings.SECURITY_ENABLED.getKey(), false)
                .put(XPackSettings.MONITORING_ENABLED.getKey(), false)

                // this is for the `test-watcher-integration` group level integration in HipChat
                .put("xpack.notification.hipchat.account.integration_account.profile", "integration")
                .put("xpack.notification.hipchat.account.integration_account.auth_token",
                        "huuS9v7ccuOy3ZBWWWr1vt8Lqu3sQnLUE81nrLZU")
                .put("xpack.notification.hipchat.account.integration_account.room", "test-watcher")

                // this is for the Watcher Test account in HipChat
                .put("xpack.notification.hipchat.account.user_account.profile", "user")
                .put("xpack.notification.hipchat.account.user_account.auth_token", "4UefsFLvKRw01EMN5vo3oyoY6BLiz7IQBQbGug8K")

                // this is for the `test-watcher-v1` notification token
                .put("xpack.notification.hipchat.account.v1_account.profile", "v1")
                .put("xpack.notification.hipchat.account.v1_account.auth_token", "a734baf62df618b96dda55b323fc30")
                .build();
    }

    public void testSendMessageV1Account() throws Exception {
        HipChatService service = getInstanceFromNode(HipChatService.class);
        HipChatMessage hipChatMessage = new HipChatMessage(
                "HipChatServiceTests#testSendMessage_V1Account",
                new String[] { "test-watcher", "test-watcher-2", "test watcher with spaces" },
                null, // users are unsupported in v1
                "watcher-tests",
                HipChatMessage.Format.TEXT,
                randomFrom(HipChatMessage.Color.values()),
                true);

        HipChatAccount account = service.getAccount("v1_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(hipChatMessage, null);
        assertSentMessagesAreValid(3, messages);
    }

    public void testSendMessageIntegrationAccount() throws Exception {
        HipChatService service = getInstanceFromNode(HipChatService.class);
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
        SentMessages messages = account.send(hipChatMessage, null);
        assertSentMessagesAreValid(1, messages);
    }

    public void testSendMessageUserAccount() throws Exception {
        HipChatService service = getInstanceFromNode(HipChatService.class);
        HipChatMessage.Color color = randomFrom(HipChatMessage.Color.values());
        HipChatMessage hipChatMessage = new HipChatMessage(
                "HipChatServiceTests#testSendMessage_UserAccount colored " + color.value(),
                new String[] { "test-watcher", "test-watcher-2", "test watcher with spaces" },
                new String[] { "watcher@elastic.co" },
                null,
                HipChatMessage.Format.TEXT,
                color,
                false);

        HipChatAccount account = service.getAccount("user_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(hipChatMessage, null);
        assertSentMessagesAreValid(4, messages);
    }

    public void testWatchWithHipChatAction() throws Exception {
        assertBusy(() -> assertThat(getInstanceFromNode(WatcherService.class).state(), is(WatcherState.STARTED)));

        HipChatAccount.Profile profile = randomFrom(HipChatAccount.Profile.values());
        HipChatMessage.Color color = randomFrom(HipChatMessage.Color.values());
        String account;
        HipChatAction.Builder actionBuilder;
        switch (profile) {
            case USER:
                account = "user_account";
                actionBuilder = hipchatAction(account, "_message")
                        .addRooms("test-watcher", "test-watcher-2", "test watcher with spaces")
                        .addUsers("watcher@elastic.co")
                        .setFormat(HipChatMessage.Format.TEXT)
                        .setColor(color)
                        .setNotify(false);
                break;

            case INTEGRATION:
                account = "integration_account";
                actionBuilder = hipchatAction(account, "_message")
                        .setFormat(HipChatMessage.Format.TEXT)
                        .setColor(color)
                        .setNotify(false);
                break;

            default:
                assertThat(profile, is(HipChatAccount.Profile.V1));
                account = "v1_account";
                actionBuilder = hipchatAction(account, "_message")
                        .addRooms("test-watcher", "test-watcher-2", "test watcher with spaces")
                        .setFrom("watcher-test")
                        .setFormat(HipChatMessage.Format.TEXT)
                        .setColor(color)
                        .setNotify(false);
        }

        String id = randomAlphaOfLength(10);
        WatcherClient watcherClient = new WatcherClient(client());
        PutWatchResponse putWatchResponse = watcherClient.preparePutWatch(id).setSource(watchBuilder()
                .trigger(schedule(interval("10m")))
                .input(simpleInput("ref", "HipChatServiceTests#testWatchWithHipChatAction"))
                .condition(InternalAlwaysCondition.INSTANCE)
                .addAction("hipchat", actionBuilder))
                .execute().get();

        assertThat(putWatchResponse.isCreated(), is(true));

        watcherClient.prepareExecuteWatch(id).setRecordExecution(true).execute().actionGet();

        client().admin().indices().prepareRefresh(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*").execute().actionGet();
        SearchResponse response = client().prepareSearch(HistoryStore.INDEX_PREFIX_WITH_TEMPLATE + "*")
                .setSource(searchSource().query(boolQuery()
                .must(termQuery("result.actions.id", "hipchat"))
                .must(termQuery("result.actions.type", "hipchat"))
                .must(termQuery("result.actions.status", "success"))
                .must(termQuery("result.actions.hipchat.account", account))
                .must(termQuery("result.actions.hipchat.sent_messages.status", "success"))))
                .get();

        assertThat(response, notNullValue());
        assertThat(response.getHits().getTotalHits(), is(1L));
    }

    public void testDefaultValuesForColorAndFormatWorks() {
        HipChatService service = getInstanceFromNode(HipChatService.class);
        HipChatMessage hipChatMessage = new HipChatMessage(
                "HipChatServiceTests#testSendMessage_UserAccount with default Color and text",
                new String[] { "test-watcher", "test-watcher-2", "test watcher with spaces" },
                new String[] { "watcher@elastic.co" },
                null, // custom "from" is not supported by integration profiles
                null,
                null,
                false);

        HipChatAccount account = service.getAccount("user_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(hipChatMessage, null);
        assertSentMessagesAreValid(4, messages);
    }

    private void assertSentMessagesAreValid(int expectedMessageSize, SentMessages messages) {
        assertThat(messages.count(), is(expectedMessageSize));
        for (SentMessages.SentMessage message : messages) {
            logger.info("Request: [{}]", message.getRequest());
            logger.info("Response: [{}]", message.getResponse());
            if (message.getException() != null) {
                logger.info("Exception stacktrace: [{}]", ExceptionsHelper.stackTrace(message.getException()));
            }
            assertThat(message.isSuccess(), is(true));
            assertThat(message.getRequest(), notNullValue());
            assertThat(message.getResponse(), notNullValue());
            assertThat(message.getResponse().status(), lessThan(300));
        }
    }
}
