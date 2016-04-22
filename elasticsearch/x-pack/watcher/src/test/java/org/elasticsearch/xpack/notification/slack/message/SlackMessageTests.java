/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.notification.slack.message;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.elasticsearch.watcher.test.MockTextTemplateEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class SlackMessageTests extends ESTestCase {
    public void testToXContent() throws Exception {
        String from = randomBoolean() ? null : randomAsciiOfLength(10);
        String[] to = rarely() ? null : new String[randomIntBetween(0, 2)];
        if (to != null) {
            for (int i = 0; i < to.length; i++) {
                to[i] = randomAsciiOfLength(10);
            }
        }
        String icon = randomBoolean() ? null : randomAsciiOfLength(10);
        String text = randomBoolean() ? null : randomAsciiOfLength(50);
        Attachment[] attachments = randomBoolean() ? null : new Attachment[randomIntBetween(0, 2)];
        if (attachments != null) {
            for (int i = 0; i < attachments.length; i++) {
                String fallback = randomBoolean() ? null : randomAsciiOfLength(10);
                String color = randomBoolean() ? null : randomAsciiOfLength(10);
                String pretext = randomBoolean() ? null : randomAsciiOfLength(10);
                String authorName = randomBoolean() ? null : randomAsciiOfLength(10);
                String authorLink = authorName == null || randomBoolean() ? null : randomAsciiOfLength(10);
                String authorIcon = authorName == null || randomBoolean() ? null : randomAsciiOfLength(10);
                String title = randomBoolean() ? null : randomAsciiOfLength(10);
                String titleLink = title == null ||randomBoolean() ? null : randomAsciiOfLength(10);
                String attachmentText = randomBoolean() ? null : randomAsciiOfLength(10);
                Field[] fields = randomBoolean() ? null : new Field[randomIntBetween(0, 2)];
                if (fields != null) {
                    for (int j = 0; j < fields.length; j++) {
                        fields[j] = new Field(randomAsciiOfLength(10), randomAsciiOfLength(10), randomBoolean());
                    }
                }
                String imageUrl = randomBoolean() ? null : randomAsciiOfLength(10);
                String thumbUrl = randomBoolean() ? null : randomAsciiOfLength(10);
                attachments[i] = new Attachment(fallback, color, pretext, authorName, authorLink, authorIcon, title, titleLink,
                        attachmentText, fields, imageUrl, thumbUrl);
            }
        }

        SlackMessage expected = new SlackMessage(from, to,  icon, text, attachments);

        boolean includeTarget = randomBoolean();

        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        writeFieldIfNotNull(builder, "from", from);
        if (includeTarget) {
            writeFieldIfNotNull(builder, "to", to);
        }
        writeFieldIfNotNull(builder, "icon", icon);
        writeFieldIfNotNull(builder, "text", text);
        if (attachments != null) {
            builder.startArray("attachments");
            for (Attachment attachment : attachments) {
                builder.startObject();
                writeFieldIfNotNull(builder, "fallback", attachment.fallback);
                writeFieldIfNotNull(builder, "color", attachment.color);
                writeFieldIfNotNull(builder, "pretext", attachment.pretext);
                writeFieldIfNotNull(builder, "author_name", attachment.authorName);
                writeFieldIfNotNull(builder, "author_link", attachment.authorLink);
                writeFieldIfNotNull(builder, "author_icon", attachment.authorIcon);
                writeFieldIfNotNull(builder, "title", attachment.title);
                writeFieldIfNotNull(builder, "title_link", attachment.titleLink);
                writeFieldIfNotNull(builder, "text", attachment.text);
                if (attachment.fields != null) {
                    builder.startArray("fields");
                    for (Field field : attachment.fields) {
                        builder.startObject();
                        builder.field("title", field.title);
                        builder.field("value", field.value);
                        builder.field("short", field.isShort);
                        builder.endObject();
                    }
                    builder.endArray();
                }
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();

        builder = jsonBuilder();
        if (includeTarget && randomBoolean()) {
            expected.toXContent(builder, ToXContent.EMPTY_PARAMS);
        } else {
            expected.toXContent(builder, ToXContent.EMPTY_PARAMS, includeTarget);
        }

        XContentParser parser = JsonXContent.jsonXContent.createParser(builder.bytes());
        parser.nextToken();

        from = null;
        to = null;
        icon = null;
        text = null;
        attachments = null;

        String currentFieldName = null;
        XContentParser.Token token = parser.currentToken();
        assertThat(token, is(XContentParser.Token.START_OBJECT));
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("from".equals(currentFieldName)) {
                from = parser.text();
            } else if ("to".equals(currentFieldName)) {
                List<String> list = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    list.add(parser.text());
                }
                to = list.toArray(new String[list.size()]);
            } else if ("icon".equals(currentFieldName)) {
                icon = parser.text();
            } else if ("text".equals(currentFieldName)) {
                text = parser.text();
            } else if ("attachments".equals(currentFieldName)) {
                List<Attachment> list = new ArrayList<>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    assertThat(token, is(XContentParser.Token.START_OBJECT));
                    String fallback = null;
                    String color = null;
                    String pretext = null;
                    String authorName = null;
                    String authorLink = null;
                    String authorIcon = null;
                    String title = null;
                    String titleLink = null;
                    String attachmentText = null;
                    Field[] fields = null;
                    String imageUrl = null;
                    String thumbUrl = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if ("fallback".equals(currentFieldName)) {
                            fallback = parser.text();
                        } else if ("color".equals(currentFieldName)) {
                            color = parser.text();
                        } else if ("pretext".equals(currentFieldName)) {
                            pretext = parser.text();
                        } else if ("author_name".equals(currentFieldName)) {
                            authorName = parser.text();
                        } else if ("author_link".equals(currentFieldName)) {
                            authorLink = parser.text();
                        } else if ("author_icon".equals(currentFieldName)) {
                            authorIcon = parser.text();
                        } else if ("title".equals(currentFieldName)) {
                            title = parser.text();
                        } else if ("title_link".equals(currentFieldName)) {
                            titleLink = parser.text();
                        } else if ("text".equals(currentFieldName)) {
                            attachmentText = parser.text();
                        } else if ("fields".equals(currentFieldName)) {
                            List<Field> fieldList = new ArrayList<>();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                assertThat(token, is(XContentParser.Token.START_OBJECT));
                                String fieldTitle = null;
                                String fieldValue = null;
                                boolean isShort = false;
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    if (token == XContentParser.Token.FIELD_NAME) {
                                        currentFieldName = parser.currentName();
                                    } else if ("title".equals(currentFieldName)) {
                                        fieldTitle = parser.text();
                                    } else if ("value".equals(currentFieldName)) {
                                        fieldValue = parser.text();
                                    } else if ("short".equals(currentFieldName)) {
                                        isShort = parser.booleanValue();
                                    }
                                }
                                fieldList.add(new Field(fieldTitle, fieldValue, isShort));
                            }
                            fields = fieldList.toArray(new Field[fieldList.size()]);
                        } else if ("image_url".equals(currentFieldName)) {
                            imageUrl = parser.text();
                        } else if ("thumb_url".equals(currentFieldName)) {
                            thumbUrl = parser.text();
                        }
                    }
                    list.add(new Attachment(fallback, color, pretext, authorName, authorLink, authorIcon, title, titleLink,
                            attachmentText, fields, imageUrl, thumbUrl));
                }
                attachments = list.toArray(new Attachment[list.size()]);
            }
        }

        if (!includeTarget) {
            assertThat(to, nullValue());
            to = expected.to;
        }

        SlackMessage actual = new SlackMessage(from, to, icon, text, attachments);

        assertThat(actual, equalTo(expected));
    }

    public void testTemplateParse() throws Exception {
        ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();

        TextTemplate from = null;
        if (randomBoolean()) {
            from = TextTemplate.inline(randomAsciiOfLength(200)).build();
            jsonBuilder.field("from", from, params);
        }
        TextTemplate[] to = null;
        if (randomBoolean()) {
            jsonBuilder.startArray("to");
            to = new TextTemplate[randomIntBetween(1, 3)];
            for (int i = 0; i < to.length; i++) {
                to[i] = TextTemplate.inline(randomAsciiOfLength(10)).build();
                to[i].toXContent(jsonBuilder, params);
            }
            jsonBuilder.endArray();
        }
        TextTemplate text = null;
        if (randomBoolean()) {
            text = TextTemplate.inline(randomAsciiOfLength(200)).build();
            jsonBuilder.field("text", text, params);
        }
        TextTemplate icon = null;
        if (randomBoolean()) {
            icon = TextTemplate.inline(randomAsciiOfLength(10)).build();
            jsonBuilder.field("icon", icon);
        }
        Attachment.Template[] attachments = null;
        if (randomBoolean()) {
            jsonBuilder.startArray("attachments");
            attachments = new Attachment.Template[randomIntBetween(1, 3)];
            for (int i = 0; i < attachments.length; i++) {
                jsonBuilder.startObject();
                TextTemplate fallback = null;
                if (randomBoolean()) {
                    fallback = TextTemplate.inline(randomAsciiOfLength(200)).build();
                    jsonBuilder.field("fallback", fallback, params);
                }
                TextTemplate color = null;
                if (randomBoolean()) {
                    color = TextTemplate.inline(randomAsciiOfLength(200)).build();
                    jsonBuilder.field("color", color, params);
                }
                TextTemplate pretext = null;
                if (randomBoolean()) {
                    pretext = TextTemplate.inline(randomAsciiOfLength(200)).build();
                    jsonBuilder.field("pretext", pretext, params);
                }
                TextTemplate authorName = null;
                TextTemplate authorLink = null;
                TextTemplate authorIcon = null;
                if (randomBoolean()) {
                    authorName = TextTemplate.inline(randomAsciiOfLength(200)).build();
                    jsonBuilder.field("author_name", authorName, params);
                    if (randomBoolean()) {
                        authorLink = TextTemplate.inline(randomAsciiOfLength(200)).build();
                        jsonBuilder.field("author_link", authorLink, params);
                    }
                    if (randomBoolean()) {
                        authorIcon = TextTemplate.inline(randomAsciiOfLength(200)).build();
                        jsonBuilder.field("author_icon", authorIcon, params);
                    }
                }
                TextTemplate title = null;
                TextTemplate titleLink = null;
                if (randomBoolean()) {
                    title = TextTemplate.inline(randomAsciiOfLength(200)).build();
                    jsonBuilder.field("title", title, params);
                    if (randomBoolean()) {
                        titleLink = TextTemplate.inline(randomAsciiOfLength(200)).build();
                        jsonBuilder.field("title_link", titleLink, params);
                    }
                }
                TextTemplate attachmentText = null;
                if (randomBoolean()) {
                    attachmentText = TextTemplate.inline(randomAsciiOfLength(200)).build();
                    jsonBuilder.field("text", attachmentText, params);
                }
                TextTemplate imageUrl = null;
                if (randomBoolean()) {
                    imageUrl = TextTemplate.inline(randomAsciiOfLength(200)).build();
                    jsonBuilder.field("image_url", imageUrl, params);
                }
                TextTemplate thumbUrl = null;
                if (randomBoolean()) {
                    thumbUrl = TextTemplate.inline(randomAsciiOfLength(200)).build();
                    jsonBuilder.field("thumb_url", thumbUrl, params);
                }
                Field.Template[] fields = null;
                if (randomBoolean()) {
                    jsonBuilder.startArray("fields");
                    fields = new Field.Template[randomIntBetween(1,3)];
                    for (int j = 0; j < fields.length; j++) {
                        jsonBuilder.startObject();
                        TextTemplate fieldTitle = TextTemplate.inline(randomAsciiOfLength(50)).build();
                        jsonBuilder.field("title", fieldTitle, params);
                        TextTemplate fieldValue = TextTemplate.inline(randomAsciiOfLength(50)).build();
                        jsonBuilder.field("value", fieldValue, params);
                        boolean isShort = randomBoolean();
                        jsonBuilder.field("short", isShort);
                        fields[j] = new Field.Template(fieldTitle, fieldValue, isShort);
                        jsonBuilder.endObject();
                    }
                    jsonBuilder.endArray();
                }
                jsonBuilder.endObject();
                attachments[i] = new Attachment.Template(fallback, color, pretext, authorName, authorLink, authorIcon, title,
                        titleLink, attachmentText, fields, imageUrl, thumbUrl);
            }
            jsonBuilder.endArray();
        }
        jsonBuilder.endObject();

        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();
        assertThat(parser.currentToken(), is(XContentParser.Token.START_OBJECT));

        SlackMessage.Template template = SlackMessage.Template.parse(parser);
        assertThat(template, notNullValue());
        assertThat(template.from, is(from));
        if (to == null) {
            assertThat(template.to, nullValue());
        } else {
            assertThat(template.to, arrayContaining(to));
        }
        assertThat(template.icon, is(icon));
        assertThat(template.text, is(text));
        if (attachments == null) {
            assertThat(template.attachments, nullValue());
        } else {
            for (int i = 0; i < attachments.length; i++) {
                assertThat(template.attachments[i], is(attachments[i]));
            }
        }
    }

    public void testTemplateParseSelfGenerated() throws Exception {
        SlackMessage.Template template = createRandomTemplate();

        XContentBuilder jsonBuilder = jsonBuilder();
        template.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);

        XContentParser parser = JsonXContent.jsonXContent.createParser(jsonBuilder.bytes());
        parser.nextToken();

        SlackMessage.Template parsed = SlackMessage.Template.parse(parser);

        assertThat(parsed, equalTo(template));
    }

    public void testTemplateRender() throws Exception {
        Settings settings = SlackMessageDefaultsTests.randomSettings();
        SlackMessageDefaults defaults = new SlackMessageDefaults(settings);
        SlackMessage.Template.Builder templateBuilder = SlackMessage.Template.builder();

        if (randomBoolean()) {
            templateBuilder.setFrom(randomAsciiOfLength(10));
        }
        if (randomBoolean()) {
            int count = randomIntBetween(0, 3);
            for (int i = 0; i < count; i++) {
                templateBuilder.addTo(randomAsciiOfLength(10));
            }
        }
        if (randomBoolean()) {
            templateBuilder.setIcon(randomAsciiOfLength(10));
        }
        if (randomBoolean()) {
            templateBuilder.setText(randomAsciiOfLength(10));
        }
        if (randomBoolean()) {
            int count = randomIntBetween(0, 3);
            for (int i = 0; i < count; i++) {
                Attachment.Template.Builder attachmentBuilder = Attachment.Template.builder();
                if (randomBoolean()) {
                    attachmentBuilder.setAuthorName(randomAsciiOfLength(10));
                    if (randomBoolean()) {
                        attachmentBuilder.setAuthorIcon(randomAsciiOfLength(10));
                    }
                    if (randomBoolean()) {
                        attachmentBuilder.setAuthorLink(randomAsciiOfLength(10));
                    }
                }
                if (randomBoolean()) {
                    attachmentBuilder.setColor(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    attachmentBuilder.setFallback(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    attachmentBuilder.setImageUrl(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    attachmentBuilder.setPretext(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    attachmentBuilder.setThumbUrl(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    attachmentBuilder.setTitle(randomAsciiOfLength(10));
                    if (randomBoolean()) {
                        attachmentBuilder.setTitleLink(randomAsciiOfLength(10));
                    }
                }
                if (randomBoolean()) {
                    attachmentBuilder.setText(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    int fieldCount = randomIntBetween(0, 3);
                    for (int j = 0; j < fieldCount; j++) {
                        attachmentBuilder.addField(randomAsciiOfLength(10), randomAsciiOfLength(10), randomBoolean());
                    }
                }
                templateBuilder.addAttachments(attachmentBuilder);
            }
        }

        // relies on the fact that all the templates we use are inline templates without param place holders
        TextTemplateEngine engine = new MockTextTemplateEngine();

        SlackMessage.Template template = templateBuilder.build();

        SlackMessage message = template.render("_w1", "_a1", engine, Collections.emptyMap(), defaults);
        assertThat(message, notNullValue());
        if (template.from != null) {
            assertThat(message.from, is(template.from.getTemplate()));
        } else {
            assertThat(message.from, is(defaults.from != null ? defaults.from : "_w1"));
        }
        if (template.to == null) {
            assertThat(message.to, is(defaults.to));
        } else {
            String[] expected = new String[message.to.length];
            for (int i = 0; i < expected.length; i++) {
                expected[i] = template.to[i].getTemplate();
            }
            assertThat(message.to, arrayContaining(expected));
        }
        assertThat(message.icon, is(template.icon != null ? template.icon.getTemplate() : defaults.icon));
        assertThat(message.text, is(template.text != null ? template.text.getTemplate() : defaults.text));
        if (template.attachments == null) {
            assertThat(message.attachments, nullValue());
        } else {
            for (int i = 0; i < template.attachments.length; i++) {
                Attachment.Template attachmentTemplate = template.attachments[i];
                Attachment attachment = message.attachments[i];
                assertThat(attachment.authorName, is(attachmentTemplate.authorName != null ? attachmentTemplate.authorName.getTemplate()
                        : defaults.attachment.authorName));
                assertThat(attachment.authorLink, is(attachmentTemplate.authorLink != null ? attachmentTemplate.authorLink.getTemplate()
                        : defaults.attachment.authorLink));
                assertThat(attachment.authorIcon, is(attachmentTemplate.authorIcon != null ? attachmentTemplate.authorIcon.getTemplate()
                        : defaults.attachment.authorIcon));
                assertThat(attachment.color, is(attachmentTemplate.color != null ? attachmentTemplate.color.getTemplate()
                        : defaults.attachment.color));
                assertThat(attachment.fallback, is(attachmentTemplate.fallback != null ? attachmentTemplate.fallback.getTemplate()
                        : defaults.attachment.fallback));
                assertThat(attachment.imageUrl, is(attachmentTemplate.imageUrl != null ? attachmentTemplate.imageUrl.getTemplate()
                        : defaults.attachment.imageUrl));
                assertThat(attachment.pretext, is(attachmentTemplate.pretext != null ? attachmentTemplate.pretext.getTemplate()
                        : defaults.attachment.pretext));
                assertThat(attachment.thumbUrl, is(attachmentTemplate.thumbUrl != null ? attachmentTemplate.thumbUrl.getTemplate()
                        : defaults.attachment.thumbUrl));
                assertThat(attachment.title, is(attachmentTemplate.title != null ? attachmentTemplate.title.getTemplate()
                        : defaults.attachment.title));
                assertThat(attachment.titleLink, is(attachmentTemplate.titleLink != null ? attachmentTemplate.titleLink.getTemplate()
                        : defaults.attachment.titleLink));
                assertThat(attachment.text, is(attachmentTemplate.text != null ? attachmentTemplate.text.getTemplate()
                        : defaults.attachment.text));
                if (attachmentTemplate.fields == null) {
                    assertThat(attachment.fields, nullValue());
                } else {
                    for (int j = 0; j < attachmentTemplate.fields.length; j++) {
                        Field.Template fieldTemplate = attachmentTemplate.fields[j];
                        Field field = attachment.fields[j];
                        assertThat(field.title,
                                is(fieldTemplate.title != null ? fieldTemplate.title.getTemplate(): defaults.attachment.field.title));
                        assertThat(field.value,
                                is(fieldTemplate.value != null ? fieldTemplate.value.getTemplate() : defaults.attachment.field.value));
                        assertThat(field.isShort,
                                is(fieldTemplate.isShort != null ? fieldTemplate.isShort : defaults.attachment.field.isShort));
                    }
                }
            }
        }
    }

    static void writeFieldIfNotNull(XContentBuilder builder, String field, Object value) throws IOException {
        if (value != null) {
            builder.field(field, value);
        }
    }

    public static SlackMessage.Template createRandomTemplate() {
        SlackMessage.Template.Builder templateBuilder = SlackMessage.Template.builder();

        if (randomBoolean()) {
            templateBuilder.setFrom(randomAsciiOfLength(10));
        }
        if (randomBoolean()) {
            int count = randomIntBetween(0, 3);
            for (int i = 0; i < count; i++) {
                templateBuilder.addTo(randomAsciiOfLength(10));
            }
        }
        if (randomBoolean()) {
            templateBuilder.setIcon(randomAsciiOfLength(10));
        }
        if (randomBoolean()) {
            templateBuilder.setText(randomAsciiOfLength(10));
        }
        if (randomBoolean()) {
            int count = randomIntBetween(0, 3);
            for (int i = 0; i < count; i++) {
                Attachment.Template.Builder attachmentBuilder = Attachment.Template.builder();
                if (randomBoolean()) {
                    attachmentBuilder.setAuthorName(randomAsciiOfLength(10));
                    if (randomBoolean()) {
                        attachmentBuilder.setAuthorIcon(randomAsciiOfLength(10));
                    }
                    if (randomBoolean()) {
                        attachmentBuilder.setAuthorLink(randomAsciiOfLength(10));
                    }
                }
                if (randomBoolean()) {
                    attachmentBuilder.setColor(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    attachmentBuilder.setFallback(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    attachmentBuilder.setImageUrl(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    attachmentBuilder.setPretext(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    attachmentBuilder.setThumbUrl(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    attachmentBuilder.setTitle(randomAsciiOfLength(10));
                    if (randomBoolean()) {
                        attachmentBuilder.setTitleLink(randomAsciiOfLength(10));
                    }
                }
                if (randomBoolean()) {
                    attachmentBuilder.setText(randomAsciiOfLength(10));
                }
                if (randomBoolean()) {
                    int fieldCount = randomIntBetween(0, 3);
                    for (int j = 0; j < fieldCount; j++) {
                        attachmentBuilder.addField(randomAsciiOfLength(10), randomAsciiOfLength(10), randomBoolean());
                    }
                }
                templateBuilder.addAttachments(attachmentBuilder);
            }
        }

        return templateBuilder.build();
    }
}
