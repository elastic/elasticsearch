/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.text;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class TextTemplate implements ToXContent {

    private final Script script;
    private final String value;

    public TextTemplate(String template) {
        this.script = null;
        this.value = template;
    }

    public TextTemplate(String template, @Nullable XContentType contentType, ScriptType type,
                        @Nullable Map<String, Object> params) {
        this.script = new Script(template, type, null, params, contentType);
        this.value = null;
    }

    public TextTemplate(Script script) {
        this.script = script;
        this.value = null;
    }

    public Script getScript() {
        return script;
    }

    public String getTemplate() {
        return script != null ? script.getScript() : value;
    }

    public XContentType getContentType() {
        return script != null ? script.getContentType() : null;
    }

    public ScriptType getType() {
        return script != null ? script.getType(): ScriptType.INLINE;
    }

    public Map<String, Object> getParams() {
        return script != null ? script.getParams(): null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TextTemplate template1 = (TextTemplate) o;
        return Objects.equals(script, template1.script) &&
                Objects.equals(value, template1.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(script, value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (script != null) {
            script.toXContent(builder, params);
        } else {
            builder.value(value);
        }
        return builder;
    }

    public static TextTemplate parse(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new TextTemplate(parser.text());
        } else {
            return new TextTemplate(Script.parse(parser, ParseFieldMatcher.STRICT, null));
        }
    }
}

