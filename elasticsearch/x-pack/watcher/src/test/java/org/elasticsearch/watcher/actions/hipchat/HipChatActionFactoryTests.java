/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.hipchat;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.notification.hipchat.HipChatAccount;
import org.elasticsearch.xpack.notification.hipchat.HipChatMessage;
import org.elasticsearch.xpack.notification.hipchat.HipChatService;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.junit.Before;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.actions.ActionBuilders.hipchatAction;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
public class HipChatActionFactoryTests extends ESTestCase {
    private HipChatActionFactory factory;
    private HipChatService hipchatService;

    @Before
    public void init() throws Exception {
        hipchatService = mock(HipChatService.class);
        factory = new HipChatActionFactory(Settings.EMPTY, mock(TextTemplateEngine.class), hipchatService);
    }

    public void testParseAction() throws Exception {
        HipChatAccount account = mock(HipChatAccount.class);
        when(hipchatService.getAccount("_account1")).thenReturn(account);

        HipChatAction action = hipchatAction("_account1", "_body").build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();

        HipChatAction parsedAction = factory.parseAction("_w1", "_a1", parser);
        assertThat(parsedAction, is(action));

        verify(account, times(1)).validateParsedTemplate("_w1", "_a1", action.message);
    }

    public void testParseActionUnknownAccount() throws Exception {
        when(hipchatService.getAccount("_unknown")).thenReturn(null);

        HipChatAction action = hipchatAction("_unknown", "_body").build();
        XContentBuilder jsonBuilder = jsonBuilder().value(action);
        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();
        try {
            factory.parseAction("_w1", "_a1", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("could not parse [hipchat] action [_w1]. unknown hipchat account [_unknown]"));
        }
    }

    public void testParser() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject();

        String accountName = randomAsciiOfLength(10);
        builder.field("account", accountName);
        builder.startObject("message");

        TextTemplate body = TextTemplate.inline("_body").build();
        builder.field("body", body);

        TextTemplate[] rooms = null;
        if (randomBoolean()) {
            TextTemplate r1 = TextTemplate.inline("_r1").build();
            TextTemplate r2 = TextTemplate.inline("_r2").build();
            rooms = new TextTemplate[] { r1, r2 };
            builder.array("room", r1, r2);
        }
        TextTemplate[] users = null;
        if (randomBoolean()) {
            TextTemplate u1 = TextTemplate.inline("_u1").build();
            TextTemplate u2 = TextTemplate.inline("_u2").build();
            users = new TextTemplate[] { u1, u2 };
            builder.array("user", u1, u2);
        }
        String from = null;
        if (randomBoolean()) {
            from = randomAsciiOfLength(10);
            builder.field("from", from);
        }
        HipChatMessage.Format format = null;
        if (randomBoolean()) {
            format = randomFrom(HipChatMessage.Format.values());
            builder.field("format", format.value());
        }
        TextTemplate color = null;
        if (randomBoolean()) {
            color = TextTemplate.inline(randomFrom(HipChatMessage.Color.values()).value()).build();
            builder.field("color", color);
        }
        Boolean notify = null;
        if (randomBoolean()) {
            notify = randomBoolean();
            builder.field("notify", notify);
        }

        builder.endObject();
        builder.endObject();

        BytesReference bytes = builder.bytes();
        logger.info("hipchat action json [{}]", bytes.toUtf8());
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();

        HipChatAction action = HipChatAction.parse("_watch", "_action", parser);

        assertThat(action, notNullValue());
        assertThat(action.account, is(accountName));
        assertThat(action.message, notNullValue());
        assertThat(action.message, is(new HipChatMessage.Template(body, rooms, users, from, format, color, notify)));
    }

    public void testParserSelfGenerated() throws Exception {
        String accountName = randomAsciiOfLength(10);
        TextTemplate body = TextTemplate.inline("_body").build();
        HipChatMessage.Template.Builder templateBuilder = new HipChatMessage.Template.Builder(body);

        XContentBuilder builder = jsonBuilder().startObject();
        builder.field("account", accountName);
        builder.startObject("message");
        builder.field("body", body);

        if (randomBoolean()) {
            TextTemplate r1 = TextTemplate.inline("_r1").build();
            TextTemplate r2 = TextTemplate.inline("_r2").build();
            templateBuilder.addRooms(r1, r2);
            builder.array("room", r1, r2);
        }
        if (randomBoolean()) {
            TextTemplate u1 = TextTemplate.inline("_u1").build();
            TextTemplate u2 = TextTemplate.inline("_u2").build();
            templateBuilder.addUsers(u1, u2);
            builder.array("user", u1, u2);
        }
        if (randomBoolean()) {
            String from = randomAsciiOfLength(10);
            templateBuilder.setFrom(from);
            builder.field("from", from);
        }
        if (randomBoolean()) {
            HipChatMessage.Format format = randomFrom(HipChatMessage.Format.values());
            templateBuilder.setFormat(format);
            builder.field("format", format.value());
        }
        if (randomBoolean()) {
            TextTemplate color = TextTemplate.inline(randomFrom(HipChatMessage.Color.values()).value()).build();
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

        HipChatAction action = new HipChatAction(accountName, template);

        XContentBuilder jsonBuilder = jsonBuilder();
        action.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = builder.bytes();
        logger.info("{}", bytes.toUtf8());
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();

        HipChatAction parsedAction = HipChatAction.parse("_watch", "_action", parser);

        assertThat(parsedAction, notNullValue());
        assertThat(parsedAction, is(action));
    }

    public void testParserInvalid() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject().field("unknown_field", "value");
        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();
        try {
            HipChatAction.parse("_watch", "_action", parser);
            fail("Expected ElasticsearchParseException");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("failed to parse [hipchat] action [_watch/_action]. unexpected token [VALUE_STRING]"));
        }
    }
}
