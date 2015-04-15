/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.common.ParseField;
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
    private final ScriptService.ScriptType type;
    private final String lang;
    private final Map<String, Object> params;

    public Script(String script) {
        this(script, DEFAULT_TYPE, DEFAULT_LANG, Collections.<String, Object>emptyMap());
    }

    public Script(String script, ScriptService.ScriptType type, String lang) {
        this(script, type, lang, Collections.<String, Object>emptyMap());
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
        return type;
    }

    public String lang() {
        return lang;
    }

    public Map<String, Object> params() {
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Script script1 = (Script) o;

        if (!lang.equals(script1.lang)) return false;
        if (!params.equals(script1.params)) return false;
        if (!script.equals(script1.script)) return false;
        if (type != script1.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = script.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + lang.hashCode();
        result = 31 * result + params.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field(SCRIPT_FIELD.getPreferredName(), script)
                .field(TYPE_FIELD.getPreferredName(), type.name().toLowerCase(Locale.ROOT))
                .field(LANG_FIELD.getPreferredName(), lang)
                .field(PARAMS_FIELD.getPreferredName(), this.params)
                .endObject();
    }

    public static Script parse(XContentParser parser) throws IOException {
        return parse(parser, ScriptService.DEFAULT_LANG);
    }

    public static Script parse(XContentParser parser, String defaultLang) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return new Script(parser.text());
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ParseException("expected a string value or an object, but found [" + token + "] instead");
        }

        String script = null;
        ScriptService.ScriptType type = ScriptService.ScriptType.INLINE;
        String lang = defaultLang;
        Map<String, Object> params = Collections.emptyMap();

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (SCRIPT_FIELD.match(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    script = parser.text();
                } else {
                    throw new ParseException("expected a string value for field [" + currentFieldName + "], but found [" + token + "]");
                }
            } else if (TYPE_FIELD.match(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    String value = parser.text();
                    try {
                        type = ScriptService.ScriptType.valueOf(value.toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException iae) {
                        throw new ParseException("unknown script type [" + value + "]");
                    }
                }
            } else if (LANG_FIELD.match(currentFieldName)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    lang = parser.text();
                } else {
                    throw new ParseException("expected a string value for field [" + currentFieldName + "], but found [" + token + "]");
                }
            } else if (PARAMS_FIELD.match(currentFieldName)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    params = parser.map();
                } else {
                    throw new ParseException("expected an object for field [" + currentFieldName + "], but found [" + token + "]");
                }
            } else {
                throw new ParseException("unexpected field [" + currentFieldName + "]");
            }
        }
        if (script == null) {
            throw new ParseException("missing required string field [" + currentFieldName + "]");
        }
        return new Script(script, type, lang, params);
    }

    public static class ParseException extends WatcherException {

        public ParseException(String msg) {
            super(msg);
        }

        public ParseException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}
