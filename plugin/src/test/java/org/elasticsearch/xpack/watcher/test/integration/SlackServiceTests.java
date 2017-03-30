/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.test.integration;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.notification.slack.SentMessages;
import org.elasticsearch.xpack.notification.slack.SlackAccount;
import org.elasticsearch.xpack.notification.slack.SlackService;
import org.elasticsearch.xpack.notification.slack.message.Attachment;
import org.elasticsearch.xpack.notification.slack.message.SlackMessage;
import org.elasticsearch.xpack.watcher.actions.slack.SlackAction;
import org.elasticsearch.xpack.watcher.condition.AlwaysCondition;
import org.elasticsearch.xpack.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchResponse;
import org.joda.time.DateTime;

import java.util.Locale;

import static org.elasticsearch.common.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;
import static org.elasticsearch.xpack.watcher.actions.ActionBuilders.slackAction;
import static org.elasticsearch.xpack.watcher.client.WatchSourceBuilders.watchBuilder;
import static org.elasticsearch.xpack.watcher.input.InputBuilders.simpleInput;
import static org.elasticsearch.xpack.watcher.trigger.TriggerBuilders.schedule;
import static org.elasticsearch.xpack.watcher.trigger.schedule.Schedules.interval;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;

@Network
public class SlackServiceTests extends AbstractWatcherIntegrationTestCase {
    @Override
    protected boolean timeWarped() {
        return true;
    }

    @Override
    protected boolean enableSecurity() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
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
                "slack integration test `testSendMessage()` " + DateTime.now(), attachments);

        SlackAccount account = service.getAccount("test_account");
        assertThat(account, notNullValue());
        SentMessages messages = account.send(message, null);
        assertThat(messages.count(), is(2));
        for (SentMessages.SentMessage sentMessage : messages) {
            try {
                assertThat(sentMessage.successful(), is(true));
                assertThat(sentMessage.getRequest(), notNullValue());
                assertThat(sentMessage.getResponse(), notNullValue());
                assertThat(sentMessage.getResponse().status(), lessThan(300));
            } catch (AssertionError e) {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.prettyPrint();
                sentMessage.toXContent(builder, EMPTY_PARAMS);
                final String messageDescription = builder.string();
                logger.warn("failed to send message. full message description: \n"
                                + messageDescription, e);
                throw e;
            }
        }
    }

    public void testWatchWithSlackAction() throws Exception {
        String account = "test_account";
        SlackAction.Builder actionBuilder = slackAction(account, SlackMessage.Template.builder()
                .setText("slack integration test `testWatchWithSlackAction()` " + DateTime.now())
                .addTo("#watcher-test", "#watcher-test-2"));

        PutWatchResponse putWatchResponse = watcherClient().preparePutWatch("1").setSource(watchBuilder()
                .trigger(schedule(interval("10m")))
                .input(simpleInput("ref", "testWatchWithSlackAction()"))
                .condition(AlwaysCondition.INSTANCE)
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
                .must(termQuery("result.actions.slack.account", account))));

        assertThat(response.getHits().getTotalHits(), is(1L));

        SearchHit hit = response.getHits().getAt(0);
        assertSuccess(hit, "result.actions.0.slack.sent_messages.0.status");
        assertSuccess(hit, "result.actions.0.slack.sent_messages.1.status");
        assertSuccess(hit, "result.actions.0.status");
    }

    private void assertSuccess(SearchHit hit, String path) {
        XContentSource source = new XContentSource(hit.getSourceRef(), XContentType.JSON);
        String json = hit.getSourceAsString();
        String message = String.format(Locale.ROOT, "Expected path [%s] to be [success], json is %s", path, json);
        assertThat(message, source.getValue(path), is("success"));
    }
}
