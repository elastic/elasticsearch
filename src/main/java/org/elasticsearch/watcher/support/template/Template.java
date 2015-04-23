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

    private final String template;
    private final @Nullable ScriptType type;
    private final @Nullable Map<String, Object> params;

    public Template(String template) {
        this(template, ScriptType.INLINE, ImmutableMap.<String, Object>of());
    }

    public Template(String template, ScriptType type, Map<String, Object> params) {
        this.template = template;
        this.type = type;
        this.params = params;
    }

    public String getTemplate() {
        return template;
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
        return Objects.equals(this.template, template.template) &&
                Objects.equals(type, template.type) &&
                Objects.equals(params, template.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(template, type, params);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (type == null && params == null) {
            return builder.value(template);
        }
        builder.startObject();
        builder.field(Field.TEMPLATE.getPreferredName(), template);
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

        String template = null;
        ScriptType type = ScriptType.INLINE;
        Map<String, Object> params = ImmutableMap.of();

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (Field.TEMPLATE.match(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    template = parser.text();
                } else {
                    throw new ParseException("expected a string field [{}], but found [{}]", currentFieldName, token);
                }
            } else if (Field.TYPE.match(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    String value = parser.text();
                    try {
                        type = ScriptType.valueOf(value.toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException iae) {
                        throw new ParseException("unknown template type [{}]", value);
                    }
                }
            } else if (Field.PARAMS.match(currentFieldName)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    params = parser.map();
                } else {
                    throw new ParseException("expected an object for field [{}], but found [{}]", currentFieldName, token);
                }
            } else {
                throw new ParseException("unexpected field [{}]", currentFieldName);
            }
        }
        if (template == null) {
            throw new ParseException("missing required string field [{}]", Field.TEMPLATE.getPreferredName());
        }
        return new Template(template, type, params);
    }

    public static Builder builder(String text) {
        return new Builder(text);
    }

    public static class Builder {

        private final String template;
        private ScriptType type;
        private HashMap<String, Object> params = new HashMap<>();

        private Builder(String template) {
            this.template = template;
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
            return new Template(template, type, params);
        }
    }

    public static class ParseException extends WatcherException {

        public ParseException(String msg, Object... args) {
            super(msg, args);
        }

        public ParseException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }

    public interface Field {
        ParseField TEMPLATE = new ParseField("template");
        ParseField TYPE = new ParseField("type");
        ParseField PARAMS = new ParseField("params");
    }
}

