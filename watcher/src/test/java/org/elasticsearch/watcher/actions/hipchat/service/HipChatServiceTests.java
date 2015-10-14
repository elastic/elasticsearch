/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.hipchat.service;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.watcher.actions.hipchat.HipChatAction;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;

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
public class HipChatServiceTests extends AbstractWatcherIntegrationTestCase {
    @Override
    protected boolean timeWarped() {
        return true;
    }

    @Override
    protected boolean enableShield() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))

                // this is for the `test-watcher-integration` group level integration in HipChat
                .put("watcher.actions.hipchat.service.account.integration_account.profile", "integration")
                .put("watcher.actions.hipchat.service.account.integration_account.auth_token", "huuS9v7ccuOy3ZBWWWr1vt8Lqu3sQnLUE81nrLZU")
                .put("watcher.actions.hipchat.service.account.integration_account.room", "test-watcher")

                // this is for the Watcher Test account in HipChat
                .put("watcher.actions.hipchat.service.account.user_account.profile", "user")
                .put("watcher.actions.hipchat.service.account.user_account.auth_token", "FYVx16oDH78ZW9r13wtXbcszyoyA7oX5tiMWg9X0")

                // this is for the `test-watcher-v1` notification token
                .put("watcher.actions.hipchat.service.account.v1_account.profile", "v1")
                .put("watcher.actions.hipchat.service.account.v1_account.auth_token", "a734baf62df618b96dda55b323fc30")
                .build();
    }

    public void testSendMessageV1Account() throws Exception {
        HipChatService service = getInstanceFromMaster(HipChatService.class);
        HipChatMessage hipChatMessage = new HipChatMessage(
                "/code HipChatServiceIT#testSendMessage_V1Account",
                new String[] { "test-watcher", "test-watcher-2" },
                null, // users are unsupported in v1
                "watcher-tests",
                HipChatMessage.Format.TEXT,
                randomFrom(HipChatMessage.Color.values()),
                true);

        HipChatAccount account = service.getAccount("v1_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(hipChatMessage);
        assertThat(messages.count(), is(2));
        for (SentMessages.SentMessage message : messages) {
            assertThat("Expected no failures, but got [" + message.failureReason + "]", message.successful(), is(true));
            assertThat(message.request, notNullValue());
            assertThat(message.response, notNullValue());
            assertThat(message.response.status(), lessThan(300));
        }
    }

    public void testSendMessageIntegrationAccount() throws Exception {
        HipChatService service = getInstanceFromMaster(HipChatService.class);
        HipChatMessage hipChatMessage = new HipChatMessage(
                "/code HipChatServiceIT#testSendMessage_IntegrationAccount",
                null, // custom rooms are unsupported by integration profiles
                null, // users are unsupported by integration profiles
                null, // custom "from" is not supported by integration profiles
                HipChatMessage.Format.TEXT,
                randomFrom(HipChatMessage.Color.values()),
                true);

        HipChatAccount account = service.getAccount("integration_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(hipChatMessage);
        assertThat(messages.count(), is(1));
        for (SentMessages.SentMessage message : messages) {
            assertThat("Expected no failures, but got [" + message.failureReason + "]", message.successful(), is(true));
            assertThat(message.request, notNullValue());
            assertThat(message.response, notNullValue());
            assertThat(message.response.status(), lessThan(300));
        }
    }

    public void testSendMessageUserAccount() throws Exception {
        HipChatService service = getInstanceFromMaster(HipChatService.class);
        HipChatMessage hipChatMessage = new HipChatMessage(
                "/code HipChatServiceIT#testSendMessage_UserAccount",
                new String[] { "test-watcher", "test-watcher-2" },
                new String[] { "watcher@elastic.co" },
                null, // custom "from" is not supported by integration profiles
                HipChatMessage.Format.TEXT,
                randomFrom(HipChatMessage.Color.values()),
                false);

        HipChatAccount account = service.getAccount("user_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(hipChatMessage);
        assertThat(messages.count(), is(3));
        for (SentMessages.SentMessage message : messages) {
            assertThat("Expected no failures, but got [" + message.failureReason + "]", message.successful(), is(true));
            assertThat(message.request, notNullValue());
            assertThat(message.response, notNullValue());
            assertThat(message.response.status(), lessThan(300));
        }
    }

    public void testWatchWithHipChatAction() throws Exception {
        HipChatAccount.Profile profile = randomFrom(HipChatAccount.Profile.values());
        String account;
        HipChatAction.Builder actionBuilder;
        switch (profile) {
            case USER:
                account = "user_account";
                actionBuilder = hipchatAction(account, "/code {{ctx.payload.ref}}")
                        .addRooms("test-watcher", "test-watcher-2")
                        .addUsers("watcher@elastic.co")
                        .setFormat(HipChatMessage.Format.TEXT)
                        .setColor(randomFrom(HipChatMessage.Color.values()))
                        .setNotify(false);
                break;

            case INTEGRATION:
                account = "integration_account";
                actionBuilder = hipchatAction(account, "/code {{ctx.payload.ref}}")
                        .setFormat(HipChatMessage.Format.TEXT)
                        .setColor(randomFrom(HipChatMessage.Color.values()))
                        .setNotify(false);
                break;

            default:
                assertThat(profile, is(HipChatAccount.Profile.V1));
                account = "v1_account";
                actionBuilder = hipchatAction(account, "/code {{ctx.payload.ref}}")
                        .addRooms("test-watcher", "test-watcher-2")
                        .setFrom("watcher-test")
                        .setFormat(HipChatMessage.Format.TEXT)
                        .setColor(randomFrom(HipChatMessage.Color.values()))
                        .setNotify(false);
        }

        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("1").setSource(watchBuilder()
                .trigger(schedule(interval("10m")))
                .input(simpleInput("ref", "HipChatServiceIT#testWatchWithHipChatAction"))
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
}
