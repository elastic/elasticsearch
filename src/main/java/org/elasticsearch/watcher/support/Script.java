/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.watcher.WatcherException;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class Script implements ToXContent {

    public static final ScriptService.ScriptType DEFAULT_TYPE = ScriptService.ScriptType.INLINE;
    public static final String DEFAULT_LANG = ScriptService.DEFAULT_LANG;

    public static final ParseField SCRIPT_FIELD = new ParseField("script");
    public static final ParseField TYPE_FIELD = new ParseField("type");
    public static final ParseField LANG_FIELD = new ParseField("lang");
    public static final ParseField PARAMS_FIELD = new ParseField("params");

    private final String script;
    private final @Nullable ScriptService.ScriptType type;
    private final @Nullable String lang;
    private final @Nullable Map<String, Object> params;

    public Script(String script) {
        this(script, null, null, null);
    }

    public Script(String script, ScriptService.ScriptType type, String lang) {
        this(script, type, lang, null);
    }

    public Script(String script, ScriptService.ScriptType type, String lang, Map<String, Object> params) {
        this.script = script;
        this.type = type;
        this.lang = lang;
        this.params = params;
    }

    public String script() {
        return script;
    }

    public ScriptService.ScriptType type() {
        return type != null ? type : ScriptService.ScriptType.INLINE;
    }

    public String lang() {
        return lang != null ? lang : DEFAULT_LANG;
    }

    public Map<String, Object> params() {
        return params != null ? params : ImmutableMap.<String, Object>of();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Script script1 = (Script) o;

        if (!script.equals(script1.script)) return false;
        if (type != script1.type) return false;
        if (lang != null ? !lang.equals(script1.lang) : script1.lang != null) return false;
        return !(params != null ? !params.equals(script1.params) : script1.params != null);
    }

    @Override
    public int hashCode() {
        int result = script.hashCode();
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (lang != null ? lang.hashCode() : 0);
        result = 31 * result + (params != null ? params.hashCode() : 0);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (type == null && lang == null && params == null) {
            return builder.value(script);
        }
        builder.startObject();
        builder.field(SCRIPT_FIELD.getPreferredName(), script);
        if (type != null) {
            builder.field(TYPE_FIELD.getPreferredName(), type.name().toLowerCase(Locale.ROOT));
        }
        if (lang != null) {
            builder.field(LANG_FIELD.getPreferredName(), lang);
        }
        if (this.params != null) {
            builder.field(PARAMS_FIELD.getPreferredName(), this.params);
        }
        return builder.endObject();
    }

    public static Script parse(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return new Script(parser.text());
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ParseException("expected a string value or an object, but found [{}] instead", token);
        }

        String script = null;
        ScriptService.ScriptType type = null;
        String lang = null;
        Map<String, Object> params = null;

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (SCRIPT_FIELD.match(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    script = parser.text();
                } else {
                    throw new ParseException("expected a string value for field [{}], but found [{}]", currentFieldName, token);
                }
            } else if (TYPE_FIELD.match(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    String value = parser.text();
                    try {
                        type = ScriptService.ScriptType.valueOf(value.toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException iae) {
                        throw new ParseException("unknown script type [{}]", value);
                    }
                }
            } else if (LANG_FIELD.match(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    lang = parser.text();
                } else {
                    throw new ParseException("expected a string value for field [{}], but found [{}]", currentFieldName, token);
                }
            } else if (PARAMS_FIELD.match(currentFieldName)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    params = parser.map();
                } else {
                    throw new ParseException("expected an object for field [{}], but found [{}]", currentFieldName, token);
                }
            } else {
                throw new ParseException("unexpected field [{}]", currentFieldName);
            }
        }
        if (script == null) {
            throw new ParseException("missing required string field [{}]", SCRIPT_FIELD.getPreferredName());
        }
        return new Script(script, type, lang, params);
    }

    public static class ParseException extends WatcherException {

        public ParseException(String msg, Object... args) {
            super(msg, args);
        }

        public ParseException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }
}
