/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.hipchat;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.JodaCompatibleZonedDateTime;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.execution.Wid;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatAccount;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatMessage;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatService;
import org.elasticsearch.xpack.watcher.notification.hipchat.SentMessages;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContextBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HipChatActionTests extends ESTestCase {
    private HipChatService service;

    @Before
    public void init() throws Exception {
        service = mock(HipChatService.class);
    }

    public void testExecute() throws Exception {
        final String accountName = "account1";

        TextTemplateEngine templateEngine = mock(TextTemplateEngine.class);

        TextTemplate body = new TextTemplate("_body");
        HipChatMessage.Template.Builder messageBuilder = new HipChatMessage.Template.Builder(body);

        HipChatMessage.Template messageTemplate = messageBuilder.build();

        HipChatAction action = new HipChatAction(accountName, messageTemplate, null);
        ExecutableHipChatAction executable = new ExecutableHipChatAction(action, logger, service, templateEngine);

        Map<String, Object> data = new HashMap<>();
        Payload payload = new Payload.Simple(data);

        Map<String, Object> metadata = MapBuilder.<String, Object>newMapBuilder().put("_key", "_val").map();

        DateTime now = DateTime.now(DateTimeZone.UTC);
        JodaCompatibleZonedDateTime jodaJavaNow = new JodaCompatibleZonedDateTime(Instant.ofEpochMilli(now.getMillis()), ZoneOffset.UTC);

        Wid wid = new Wid(randomAlphaOfLength(5), now);
        WatchExecutionContext ctx = mockExecutionContextBuilder(wid.watchId())
                .wid(wid)
                .payload(payload)
                .time(wid.watchId(), now)
                .metadata(metadata)
                .buildMock();

        Map<String, Object> triggerModel = new HashMap<>();
        triggerModel.put("triggered_time", jodaJavaNow);
        triggerModel.put("scheduled_time", jodaJavaNow);
        Map<String, Object> ctxModel = new HashMap<>();
        ctxModel.put("id", ctx.id().value());
        ctxModel.put("watch_id", wid.watchId());
        ctxModel.put("payload", data);
        ctxModel.put("metadata", metadata);
        ctxModel.put("execution_time", jodaJavaNow);
        ctxModel.put("trigger", triggerModel);
        ctxModel.put("vars", Collections.emptyMap());
        Map<String, Object> expectedModel = singletonMap("ctx", ctxModel);

        if (body != null) {
            when(templateEngine.render(body, expectedModel)).thenReturn(body.getTemplate());
        }

        String[] rooms = new String[] { "_r1" };
        HipChatMessage message = new HipChatMessage(body.getTemplate(), rooms, null, null, null, null, null);
        HipChatAccount account = mock(HipChatAccount.class);
        when(account.render(wid.watchId(), "_id", templateEngine, messageTemplate, expectedModel)).thenReturn(message);
        boolean responseFailure = randomBoolean();
        HttpResponse response = new HttpResponse(responseFailure ? 404 : 200);
        HttpRequest request = HttpRequest.builder("localhost", 12345).path("/").build();
        SentMessages sentMessages = new SentMessages(accountName, Arrays.asList(
                SentMessages.SentMessage.responded("_r1", SentMessages.SentMessage.TargetType.ROOM, message, request, response)
        ));
        when(account.send(message, null)).thenReturn(sentMessages);
        when(service.getAccount(accountName)).thenReturn(account);

        Action.Result result = executable.execute("_id", ctx, payload);

        assertThat(result, notNullValue());
        assertThat(result, instanceOf(HipChatAction.Result.Executed.class));
        if (responseFailure) {
            assertThat(result.status(), equalTo(Action.Result.Status.FAILURE));
        } else {
            assertThat(result.status(), equalTo(Action.Result.Status.SUCCESS));
        }
        assertThat(((HipChatAction.Result.Executed) result).sentMessages(), sameInstance(sentMessages));
        assertValidToXContent(result);
    }

    public void testParser() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject();

        String accountName = randomAlphaOfLength(10);
        builder.field("account", accountName);
        builder.startObject("message");

        TextTemplate body = new TextTemplate("_body");
        builder.field("body", body);

        TextTemplate[] rooms = null;
        if (randomBoolean()) {
            TextTemplate r1 = new TextTemplate("_r1");
            TextTemplate r2 = new TextTemplate("_r2");
            rooms = new TextTemplate[] { r1, r2 };
            builder.array("room", r1, r2);
        }
        TextTemplate[] users = null;
        if (randomBoolean()) {
            TextTemplate u1 = new TextTemplate("_u1");
            TextTemplate u2 = new TextTemplate("_u2");
            users = new TextTemplate[] { u1, u2 };
            builder.array("user", u1, u2);
        }
        String from = null;
        if (randomBoolean()) {
            from = randomAlphaOfLength(10);
            builder.field("from", from);
        }
        HipChatMessage.Format format = null;
        if (randomBoolean()) {
            format = randomFrom(HipChatMessage.Format.values());
            builder.field("format", format.value());
        }
        TextTemplate color = null;
        if (randomBoolean()) {
            color = new TextTemplate(randomFrom(HipChatMessage.Color.values()).value());
            builder.field("color", color);
        }
        Boolean notify = null;
        if (randomBoolean()) {
            notify = randomBoolean();
            builder.field("notify", notify);
        }
        builder.endObject();
        HttpProxy proxy = null;
        if (randomBoolean()) {
            proxy = new HttpProxy("localhost", 8080);
            builder.startObject("proxy").field("host", "localhost").field("port", 8080).endObject();
        }
        builder.endObject();

        BytesReference bytes = BytesReference.bytes(builder);
        logger.info("hipchat action json [{}]", bytes.utf8ToString());
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken();

        HipChatAction action = HipChatAction.parse("_watch", "_action", parser);

        assertThat(action, notNullValue());
        assertThat(action.account, is(accountName));
        assertThat(action.proxy, is(proxy));
        assertThat(action.message, notNullValue());
        assertThat(action.message, is(new HipChatMessage.Template(body, rooms, users, from, format, color, notify)));
    }

    public void testParserSelfGenerated() throws Exception {
        String accountName = randomAlphaOfLength(10);
        TextTemplate body = new TextTemplate("_body");
        HipChatMessage.Template.Builder templateBuilder = new HipChatMessage.Template.Builder(body);

        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("account", accountName);

        HttpProxy proxy = null;
        if (randomBoolean()) {
            proxy = new HttpProxy("localhost", 8080);
            builder.startObject("proxy").field("host", "localhost").field("port", 8080).endObject();
        }

        builder.startObject("message");
        builder.field("body", body);

        if (randomBoolean()) {
            TextTemplate r1 = new TextTemplate("_r1");
            TextTemplate r2 = new TextTemplate("_r2");
            templateBuilder.addRooms(r1, r2);
            builder.array("room", r1, r2);
        }
        if (randomBoolean()) {
            TextTemplate u1 = new TextTemplate("_u1");
            TextTemplate u2 = new TextTemplate("_u2");
            templateBuilder.addUsers(u1, u2);
            builder.array("user", u1, u2);
        }
        if (randomBoolean()) {
            String from = randomAlphaOfLength(10);
            templateBuilder.setFrom(from);
            builder.field("from", from);
        }
        if (randomBoolean()) {
            HipChatMessage.Format format = randomFrom(HipChatMessage.Format.values());
            templateBuilder.setFormat(format);
            builder.field("format", format.value());
        }
        if (randomBoolean()) {
            TextTemplate color = new TextTemplate(randomFrom(HipChatMessage.Color.values()).value());
            templateBuilder.setColor(color);
            builder.field("color", color);
        }
        if (randomBoolean()) {
            boolean notify = randomBoolean();
            templateBuilder.setNotify(notify);
            builder.field("notify", notify);
        }

        builder.endObject();
        builder.endObject();

        HipChatMessage.Template template = templateBuilder.build();

        HipChatAction action = new HipChatAction(accountName, template, proxy);

        XContentBuilder jsonBuilder = jsonBuilder();
        action.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(builder);
        logger.info("{}", bytes.utf8ToString());
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken();

        HipChatAction parsedAction = HipChatAction.parse("_watch", "_action", parser);

        assertThat(parsedAction, notNullValue());
        assertThat(parsedAction, is(action));
    }

    public void testParserInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("unknown_field", "value").endObject();
        XContentParser parser = createParser(builder);
        parser.nextToken();
        try {
            HipChatAction.parse("_watch", "_action", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("failed to parse [hipchat] action [_watch/_action]. unexpected token [VALUE_STRING]"));
        }
    }

    // ensure that toXContent can be serialized and read again
    private void assertValidToXContent(Action.Result result) throws IOException {
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            Strings.toString(builder);
            try (XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, Strings.toString(builder))) {
                parser.map();
            }
        }
    }
}
