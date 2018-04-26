/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.hipchat;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class HipChatMessageTests extends ESTestCase {

    public void testToXContent() throws Exception {
        String message = randomAlphaOfLength(10);
        String[] rooms = generateRandomStringArray(3, 10, true);
        String[] users = generateRandomStringArray(3, 10, true);
        String from = randomBoolean() ? null : randomAlphaOfLength(10);
        HipChatMessage.Format format = rarely() ? null : randomFrom(HipChatMessage.Format.values());
        HipChatMessage.Color color = rarely() ? null : randomFrom(HipChatMessage.Color.values());
        Boolean notify = rarely() ? null : randomBoolean();
        HipChatMessage msg = new HipChatMessage(message, rooms, users, from, format, color, notify);

        XContentBuilder builder = jsonBuilder();
        boolean includeTarget = randomBoolean();
        if (includeTarget && randomBoolean()) {
            msg.toXContent(builder, ToXContent.EMPTY_PARAMS);
        } else {
            msg.toXContent(builder, ToXContent.EMPTY_PARAMS, includeTarget);
        }
        BytesReference bytes = BytesReference.bytes(builder);

        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken();

        assertThat(parser.currentToken(), is(XContentParser.Token.START_OBJECT));

        message = null;
        rooms = null;
        users = null;
        from = null;
        format = null;
        color = null;
        notify = null;
        XContentParser.Token token = null;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("body".equals(currentFieldName)) {
                message = parser.text();
            } else if ("room".equals(currentFieldName)) {
                rooms = parser.list().stream().map(Object::toString).toArray(String[]::new);
            } else if ("user".equals(currentFieldName)) {
                users = parser.list().stream().map(Object::toString).toArray(String[]::new);
            } else if ("from".equals(currentFieldName)) {
                from = parser.text();
            } else if ("format".equals(currentFieldName)) {
                format = HipChatMessage.Format.parse(parser);
            } else if ("color".equals(currentFieldName)) {
                color = HipChatMessage.Color.parse(parser);
            } else if ("notify".equals(currentFieldName)) {
                notify = parser.booleanValue();
            } else {
                fail("unexpected xcontent field [" + currentFieldName + "] in hipchat message");
            }
        }

        assertThat(message, notNullValue());
        assertThat(message, is(msg.body));
        if (includeTarget) {
            if (msg.rooms == null || msg.rooms.length == 0) {
                assertThat(rooms, nullValue());
            } else {
                assertThat(rooms, arrayContaining(msg.rooms));
            }
            if (msg.users == null || msg.users.length == 0) {
                assertThat(users, nullValue());
            } else {
                assertThat(users, arrayContaining(msg.users));
            }
        }
        assertThat(from, is(msg.from));
        assertThat(format, is(msg.format));
        assertThat(color, is(msg.color));
        assertThat(notify, is(msg.notify));
    }

    public void testEquals() throws Exception {
        String message = randomAlphaOfLength(10);
        String[] rooms = generateRandomStringArray(3, 10, true);
        String[] users = generateRandomStringArray(3, 10, true);
        String from = randomBoolean() ? null : randomAlphaOfLength(10);
        HipChatMessage.Format format = rarely() ? null : randomFrom(HipChatMessage.Format.values());
        HipChatMessage.Color color = rarely() ? null : randomFrom(HipChatMessage.Color.values());
        Boolean notify = rarely() ? null : randomBoolean();
        HipChatMessage msg1 = new HipChatMessage(message, rooms, users, from, format, color, notify);

        boolean equals = randomBoolean();
        if (!equals) {
            equals = true;
            if (rarely()) {
                equals = false;
                message = "another message";
            }
            if (rarely()) {
                equals = false;
                rooms = rooms == null ? new String[] { "roomX" } : randomBoolean() ? null : new String[] { "roomX" , "roomY"};
            }
            if (rarely()) {
                equals = false;
                users = users == null ? new String[] { "userX" } : randomBoolean() ? null : new String[] { "userX", "userY" };
            }
            if (rarely()) {
                equals = false;
                from = from == null ? "fromX" : randomBoolean() ? null : "fromY";
            }
            if (rarely()) {
                equals = false;
                format = format == null ?
                        randomFrom(HipChatMessage.Format.values()) :
                        randomBoolean() ?
                                null :
                                    randomFromWithExcludes(HipChatMessage.Format.values(), format);
            }
            if (rarely()) {
                equals = false;
                color = color == null ?
                        randomFrom(HipChatMessage.Color.values()) :
                        randomBoolean() ?
                                null :
                                    randomFromWithExcludes(HipChatMessage.Color.values(), color);
            }
            if (rarely()) {
                equals = false;
                notify = notify == null ? (Boolean) randomBoolean() : randomBoolean() ? null : !notify;
            }
        }

        HipChatMessage msg2 = new HipChatMessage(message, rooms, users, from, format, color, notify);
        assertThat(msg1.equals(msg2), is(equals));
    }

    public void testTemplateParse() throws Exception {
        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();

        TextTemplate body = new TextTemplate(randomAlphaOfLength(200));
        jsonBuilder.field("body", body, ToXContent.EMPTY_PARAMS);
        TextTemplate[] rooms = null;
        if (randomBoolean()) {
            jsonBuilder.startArray("room");
            rooms = new TextTemplate[randomIntBetween(1, 3)];
            for (int i = 0; i < rooms.length; i++) {
                rooms[i] = new TextTemplate(randomAlphaOfLength(10));
                rooms[i].toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
            }
            jsonBuilder.endArray();
        }
        TextTemplate[] users = null;
        if (randomBoolean()) {
            jsonBuilder.startArray("user");
            users = new TextTemplate[randomIntBetween(1, 3)];
            for (int i = 0; i < users.length; i++) {
                users[i] = new TextTemplate(randomAlphaOfLength(10));
                users[i].toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
            }
            jsonBuilder.endArray();
        }
        String from = null;
        if (randomBoolean()) {
            from = randomAlphaOfLength(10);
            jsonBuilder.field("from", from);
        }
        TextTemplate color = null;
        if (randomBoolean()) {
            color = new TextTemplate(randomAlphaOfLength(10));
            jsonBuilder.field("color", color, ToXContent.EMPTY_PARAMS);
        }
        HipChatMessage.Format format = null;
        if (randomBoolean()) {
            format = randomFrom(HipChatMessage.Format.values());
            jsonBuilder.field("format", format.value());
        }
        Boolean notify = null;
        if (randomBoolean()) {
            notify = randomBoolean();
            jsonBuilder.field("notify", notify);
        }

        BytesReference bytes = BytesReference.bytes(jsonBuilder.endObject());
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken();

        HipChatMessage.Template template = HipChatMessage.Template.parse(parser);

        assertThat(template, notNullValue());
        assertThat(template.body, is(body));
        if (rooms == null) {
            assertThat(template.rooms, nullValue());
        } else {
            assertThat(template.rooms, arrayContaining(rooms));
        }
        if (users == null) {
            assertThat(template.users, nullValue());
        } else {
            assertThat(template.users, arrayContaining(users));
        }
        assertThat(template.from, is(from));
        assertThat(template.color, is(color));
        assertThat(template.format, is(format));
        assertThat(template.notify, is(notify));
    }

    public void testTemplateParseSelfGenerated() throws Exception {
        TextTemplate body = new TextTemplate(randomAlphaOfLength(10));
        HipChatMessage.Template.Builder templateBuilder = new HipChatMessage.Template.Builder(body);

        if (randomBoolean()) {
            int count = randomIntBetween(1, 3);
            for (int i = 0; i < count; i++) {
                templateBuilder.addRooms(new TextTemplate(randomAlphaOfLength(10)));
            }
        }
        if (randomBoolean()) {
            int count = randomIntBetween(1, 3);
            for (int i = 0; i < count; i++) {
                templateBuilder.addUsers(new TextTemplate(randomAlphaOfLength(10)));
            }
        }
        if (randomBoolean()) {
            templateBuilder.setFrom(randomAlphaOfLength(10));
        }
        if (randomBoolean()) {
            templateBuilder.setColor(new TextTemplate(randomAlphaOfLength(5)));
        }
        if (randomBoolean()) {
            templateBuilder.setFormat(randomFrom(HipChatMessage.Format.values()));
        }
        if (randomBoolean()) {
            templateBuilder.setNotify(randomBoolean());
        }
        HipChatMessage.Template template = templateBuilder.build();

        XContentBuilder jsonBuilder = jsonBuilder();
        template.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(jsonBuilder);

        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        parser.nextToken();

        HipChatMessage.Template parsed = HipChatMessage.Template.parse(parser);

        assertThat(parsed, equalTo(template));
    }

    public void testAuthTokenParamIsFiltered() throws Exception {
        HttpResponse response = new HttpResponse(500);
        String token = randomAlphaOfLength(20);
        HttpRequest request = HttpRequest.builder("localhost", 1234).setParam("auth_token", token).build();

        // String body, String[] rooms, String[] users, String from, Format format, Color color, Boolean notify
        HipChatMessage hipChatMessage = new HipChatMessage("body", new String[]{"room"}, null, "from",
                HipChatMessage.Format.TEXT, HipChatMessage.Color.RED, false);
        SentMessages.SentMessage sentMessage = SentMessages.SentMessage.responded("targetName", SentMessages.SentMessage.TargetType.ROOM,
                hipChatMessage, request, response);


        try (XContentBuilder builder = jsonBuilder()) {
            WatcherParams params = WatcherParams.builder().hideSecrets(false).build();
            sentMessage.toXContent(builder, params);
            assertThat(Strings.toString(builder), containsString(token));

            try (XContentParser parser = builder.contentType().xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            Strings.toString(builder))) {
                parser.map();
            }
        }
        try (XContentBuilder builder = jsonBuilder()) {
            sentMessage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(Strings.toString(builder), not(containsString(token)));

            try (XContentParser parser = builder.contentType().xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            Strings.toString(builder))) {
                parser.map();
            }
        }
    }

    static <E extends Enum> E randomFromWithExcludes(E[] values, E... exclude) {
        List<E> excludes = Arrays.asList(exclude);
        List<E> includes = new ArrayList<>();
        for (E value : values) {
            if (!excludes.contains(value)) {
                includes.add(value);
            }
        }
        return randomFrom(includes);
    }
}
