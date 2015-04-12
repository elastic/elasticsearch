/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.watcher.WatcherException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class Template implements ToXContent {

    private final String text;
    private final @Nullable ScriptType type;
    private final @Nullable Map<String, Object> params;

    public Template(String text) {
        this(text, ScriptType.INLINE, ImmutableMap.<String, Object>of());
    }

    public Template(String text, ScriptType type, Map<String, Object> params) {
        this.text = text;
        this.type = type;
        this.params = params;
    }

    public String getText() {
        return text;
    }

    public ScriptType getType() {
        return type != null ? type : ScriptType.INLINE;
    }

    public Map<String, Object> getParams() {
        return params != null ? params : ImmutableMap.<String, Object>of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Template template = (Template) o;
        return Objects.equals(text, template.text) &&
                Objects.equals(type, template.type) &&
                Objects.equals(params, template.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(text, type, params);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (type == null && params == null) {
            return builder.value(text);
        }
        builder.startObject();
        builder.field(Field.TEXT.getPreferredName(), text);
        if (type != null) {
            builder.field(Field.TYPE.getPreferredName(), type.name().toLowerCase(Locale.ROOT));
        }
        if (this.params != null) {
            builder.field(Field.PARAMS.getPreferredName(), this.params);
        }
        return builder.endObject();
    }

    public static Template parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return new Template(parser.text());
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ParseException("expected a string value or an object, but found [" + token + "] instead");
        }

        String text = null;
        ScriptType type = ScriptType.INLINE;
        Map<String, Object> params = ImmutableMap.of();

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.TEXT.match(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    text = parser.text();
                } else {
                    throw new ParseException("expected a string value for field [" + currentFieldName + "], but found [" + token + "]");
                }
            } else if (Field.TYPE.match(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    String value = parser.text();
                    try {
                        type = ScriptType.valueOf(value.toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException iae) {
                        throw new ParseException("unknown template type [" + value + "]");
                    }
                }
            } else if (Field.PARAMS.match(currentFieldName)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    params = parser.map();
                } else {
                    throw new ParseException("expected an object for field [" + currentFieldName + "], but found [" + token + "]");
                }
            } else {
                throw new ParseException("unexpected field [" + currentFieldName + "]");
            }
        }
        if (text == null) {
            throw new ParseException("missing required string field [" + Field.TEXT.getPreferredName() + "]");
        }
        return new Template(text, type, params);
    }

    public static Builder builder(String text) {
        return new Builder(text);
    }

    public static class Builder {

        private final String text;
        private ScriptType type;
        private HashMap<String, Object> params = new HashMap<>();

        private Builder(String text) {
            this.text = text;
        }

        public Builder setType(ScriptType type) {
            this.type = type;
            return null;
        }

        public Builder putParams(Map<String, Object> params) {
            this.params.putAll(params);
            return this;
        }

        public Builder putParam(String key, Object value) {
            params.put(key, value);
            return this;
        }

        public Template build() {
            type = type != null ? type : ScriptType.INLINE;
            return new Template(text, type, params);
        }
    }

    public static class ParseException extends WatcherException {

        public ParseException(String msg) {
            super(msg);
        }

        public ParseException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    public interface Field {
        ParseField TEXT = new ParseField("text");
        ParseField TYPE = new ParseField("type");
        ParseField PARAMS = new ParseField("params");
    }
}

