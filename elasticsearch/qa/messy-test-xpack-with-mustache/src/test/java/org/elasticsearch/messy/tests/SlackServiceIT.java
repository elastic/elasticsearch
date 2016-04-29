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
import org.elasticsearch.watcher.actions.slack.SlackAction;
import org.elasticsearch.xpack.notification.slack.SentMessages;
import org.elasticsearch.xpack.notification.slack.SlackAccount;
import org.elasticsearch.xpack.notification.slack.SlackService;
import org.elasticsearch.xpack.notification.slack.message.Attachment;
import org.elasticsearch.xpack.notification.slack.message.SlackMessage;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.watcher.transport.actions.put.PutWatchResponse;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.watcher.actions.ActionBuilders.slackAction;
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
public class SlackServiceIT extends AbstractWatcherIntegrationTestCase {
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
                .put("xpack.notification.slack.account.test_account.url",
                        "https://hooks.slack.com/services/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")
                .build();
    }

    public void testSendMessage() throws Exception {
        SlackService service = getInstanceFromMaster(SlackService.class);
        Attachment[] attachments = new Attachment[] {
                new Attachment("fallback", randomFrom("good", "warning", "danger"), "pretext", "author_name", null, null,
                        "title", null, "_text", null, null, null)
        };
        SlackMessage message = new SlackMessage(
                "SlackServiceTests",
                new String[] { "#watcher-test", "#watcher-test-2"}, // TODO once we have a dedicated test user in slack, add it here
                null,
                "slack integration test `testSendMessage()`", attachments);

        SlackAccount account = service.getAccount("test_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(message);
        assertThat(messages.count(), is(2));
        for (SentMessages.SentMessage sentMessage : messages) {
            assertThat(sentMessage.successful(), is(true));
            assertThat(sentMessage.getRequest(), notNullValue());
            assertThat(sentMessage.getResponse(), notNullValue());
            assertThat(sentMessage.getResponse().status(), lessThan(300));
        }
    }

    public void testWatchWithSlackAction() throws Exception {
        String account = "test_account";
        SlackAction.Builder actionBuilder = slackAction(account, SlackMessage.Template.builder()
                .setText("slack integration test `{{ctx.payload.ref}}`")
                .addTo("#watcher-test", "#watcher-test-2"));

        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("1").setSource(watchBuilder()
                .trigger(schedule(interval("10m")))
                .input(simpleInput("ref", "testWatchWithSlackAction()"))
                .condition(alwaysCondition())
                .addAction("slack", actionBuilder))
                .execute().get();

        assertThat(putWatchResponse.isCreated(), is(true));

        timeWarp().scheduler().trigger("1");
        flush();
        refresh();

        assertWatchWithMinimumPerformedActionsCount("1", 1L, false);

        SearchResponse response = searchHistory(searchSource().query(boolQuery()
                .must(termQuery("result.actions.id", "slack"))
                .must(termQuery("result.actions.type", "slack"))
                .must(termQuery("result.actions.status", "success"))
                .must(termQuery("result.actions.slack.account", account))
                .must(termQuery("result.actions.slack.sent_messages.status", "success"))));

        assertThat(response, notNullValue());
        assertThat(response.getHits().getTotalHits(), is(1L));
    }
}
