/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.slack.message;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

class Field implements MessageElement {

    final String title;
    final String value;
    final boolean isShort;

    Field(String title, String value, boolean isShort) {
        this.title = title;
        this.value = value;
        this.isShort = isShort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Field field = (Field) o;

        if (isShort != field.isShort) return false;
        if (title.equals(field.title) == false) return false;
        return value.equals(field.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, value, isShort);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field(XField.TITLE.getPreferredName(), title)
                .field(XField.VALUE.getPreferredName(), value)
                .field(XField.SHORT.getPreferredName(), isShort)
                .endObject();
    }

    static class Template implements ToXContentObject {

        final TextTemplate title;
        final TextTemplate value;
        final Boolean isShort;

        Template(TextTemplate title, TextTemplate value, Boolean isShort) {
            this.title = title;
            this.value = value;
            this.isShort = isShort;
        }

        public Field render(TextTemplateEngine engine, Map<String, Object> model,
                            SlackMessageDefaults.AttachmentDefaults.FieldDefaults defaults) {
            String title = this.title != null ? engine.render(this.title, model) : defaults.title;
            String value = this.value != null ? engine.render(this.value, model) : defaults.value;
            Boolean isShort = this.isShort != null ? this.isShort : defaults.isShort;
            return new Field(title, value, isShort);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Template template = (Template) o;

            if (isShort != template.isShort) return false;
            if (title.equals(template.title) == false) return false;
            return value.equals(template.value);
        }

        @Override
        public int hashCode() {
            int result = title.hashCode();
            result = 31 * result + value.hashCode();
            result = 31 * result + (isShort ? 1 : 0);
            return result;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field(XField.TITLE.getPreferredName(), title)
                    .field(XField.VALUE.getPreferredName(), value)
                    .field(XField.SHORT.getPreferredName(), isShort)
                    .endObject();
        }

        public static Template parse(XContentParser parser) throws IOException {

            TextTemplate title = null;
            TextTemplate value = null;
            boolean isShort = false;

            XContentParser.Token token = null;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (XField.TITLE.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        title = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment field. failed to parse [{}] field", pe,
                                XField.TITLE);
                    }
                } else if (XField.VALUE.match(currentFieldName, parser.getDeprecationHandler())) {
                    try {
                        value = TextTemplate.parse(parser);
                    } catch (ElasticsearchParseException pe) {
                        throw new ElasticsearchParseException("could not parse message attachment field. failed to parse [{}] field", pe,
                                XField.VALUE);
                    }
                } else if (XField.SHORT.match(currentFieldName, parser.getDeprecationHandler())) {
                    if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        isShort = parser.booleanValue();
                    } else {
                        throw new ElasticsearchParseException("could not parse message attachment field. expected a boolean value for " +
                                "[{}] field, but found [{}]", XField.SHORT, token);
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse message attachment field. unexpected field [{}]",
                            currentFieldName);
                }
            }

            if (title == null) {
                throw new ElasticsearchParseException("could not parse message attachment field. missing required [{}] field",
                        XField.TITLE);
            }
            if (value == null) {
                throw new ElasticsearchParseException("could not parse message attachment field. missing required [{}] field",
                        XField.VALUE);
            }
            return new Template(title, value, isShort);
        }
    }

    interface XField extends MessageElement.XField {
        ParseField VALUE = new ParseField("value");
        ParseField SHORT = new ParseField("short");
    }
}
