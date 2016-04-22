/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.slack;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.xpack.notification.slack.SentMessages;
import org.elasticsearch.xpack.notification.slack.SlackAccount;
import org.elasticsearch.xpack.notification.slack.SlackService;
import org.elasticsearch.xpack.notification.slack.message.SlackMessage;
import org.elasticsearch.xpack.notification.slack.message.SlackMessageDefaults;
import org.elasticsearch.xpack.notification.slack.message.SlackMessageTests;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.elasticsearch.watcher.watch.Payload;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.actions.ActionBuilders.slackAction;
import static org.elasticsearch.watcher.test.WatcherTestUtils.mockExecutionContextBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class SlackActionTests extends ESTestCase {
    private SlackService service;

    @Before
    public void init() throws Exception {
        service = mock(SlackService.class);
    }

    public void testExecute() throws Exception {
        final String accountName = "account1";

        TextTemplateEngine templateEngine = mock(TextTemplateEngine.class);

        SlackMessage.Template messageTemplate = mock(SlackMessage.Template.class);
        SlackMessage message = mock(SlackMessage.class);

        SlackAction action = new SlackAction(accountName, messageTemplate);
        ExecutableSlackAction executable = new ExecutableSlackAction(action, logger, service, templateEngine);

        Map<String, Object> data = new HashMap<>();
        Payload payload = new Payload.Simple(data);

        Map<String, Object> metadata = MapBuilder.<String, Object>newMapBuilder().put("_key", "_val").map();

        DateTime now = DateTime.now(DateTimeZone.UTC);

        Wid wid = new Wid(randomAsciiOfLength(5), randomLong(), now);
        WatchExecutionContext ctx = mockExecutionContextBuilder(wid.watchId())
                .wid(wid)
                .payload(payload)
                .time(wid.watchId(), now)
                .metadata(metadata)
                .buildMock();

        Map<String, Object> triggerModel = new HashMap<>();
        triggerModel.put("triggered_time", now);
        triggerModel.put("scheduled_time", now);
        Map<String, Object> ctxModel = new HashMap<>();
        ctxModel.put("id", ctx.id().value());
        ctxModel.put("watch_id", wid.watchId());
        ctxModel.put("payload", data);
        ctxModel.put("metadata", metadata);
        ctxModel.put("execution_time", now);
        ctxModel.put("trigger", triggerModel);
        ctxModel.put("vars", emptyMap());
        Map<String, Object> expectedModel = singletonMap("ctx", ctxModel);

        when(messageTemplate.render(eq(wid.watchId()), eq("_action"), eq(templateEngine), eq(expectedModel),
                any(SlackMessageDefaults.class))).thenReturn(message);
        SlackAccount account = mock(SlackAccount.class);
        when(service.getAccount(accountName)).thenReturn(account);


        List<SentMessages.SentMessage> messages = new ArrayList<>();
        boolean hasError = false;
        boolean hasSuccess = false;
        int count = randomIntBetween(1, 2);
        for (int i = 0; i < count; i++) {
            HttpResponse response = mock(HttpResponse.class);
            HttpRequest request = mock(HttpRequest.class);
            switch (randomIntBetween(0, 2)) {
                case 0:
                    messages.add(SentMessages.SentMessage.error(randomAsciiOfLength(10), message, "unknown error"));
                    hasError = true;
                    break;
                case 1:
                    when(response.status()).thenReturn(randomIntBetween(300, 600)); // error reponse
                    messages.add(SentMessages.SentMessage.responded(randomAsciiOfLength(10), message, request, response));
                    hasError = true;
                    break;
                case 2:
                    when(response.status()).thenReturn(randomIntBetween(200, 299)); // success
                    messages.add(SentMessages.SentMessage.responded(randomAsciiOfLength(10), message, request, response));
                    hasSuccess = true;
            }
        }
        SentMessages sentMessages = new SentMessages(accountName, messages);
        when(account.send(message)).thenReturn(sentMessages);

        Action.Result.Status expectedStatus = !hasError ? Action.Result.Status.SUCCESS :
                !hasSuccess ? Action.Result.Status.FAILURE :
                        Action.Result.Status.PARTIAL_FAILURE;


        Action.Result result = executable.execute("_action", ctx, payload);

        assertThat(result, notNullValue());
        assertThat(result, instanceOf(SlackAction.Result.Executed.class));
        assertThat(result.status(), equalTo(expectedStatus));
        assertThat(((SlackAction.Result.Executed) result).sentMessages(), sameInstance(sentMessages));
    }

    public void testParser() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject();

        String accountName = randomAsciiOfLength(10);
        SlackMessage.Template message = SlackMessageTests.createRandomTemplate();

        builder.field("account", accountName);
        builder.field("message", message, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        BytesReference bytes = builder.bytes();
        logger.info("slack action json [{}]", bytes.toUtf8());
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();

        SlackAction action = SlackAction.parse("_watch", "_action", parser);

        assertThat(action, notNullValue());
        assertThat(action.account, is(accountName));
        assertThat(action.message, notNullValue());
        assertThat(action.message, is(message));
    }

    public void testParserSelfGenerated() throws Exception {
        String accountName = randomBoolean() ? randomAsciiOfLength(10) : null;
        SlackMessage.Template message = SlackMessageTests.createRandomTemplate();

        SlackAction action = slackAction(accountName, message).build();

        XContentBuilder builder = jsonBuilder();
        action.toXContent(builder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = builder.bytes();
        logger.info("{}", bytes.toUtf8());
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();

        SlackAction parsedAction = SlackAction.parse("_watch", "_action", parser);

        assertThat(parsedAction, notNullValue());
        assertThat(parsedAction, is(action));
    }

    public void testParserInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("unknown_field", "value");
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        try {
            SlackAction.parse("_watch", "_action", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("failed to parse [slack] action [_watch/_action]. unexpected token [VALUE_STRING]"));
        }
    }
}
