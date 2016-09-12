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

/**
 * Holds a template to be used in many places in a watch as configuration.
 *
 * One liner templates are kept around as just strings and {@link Script} is used for
 * parsing/serialization logic for any non inlined templates and/or when templates
 * have custom params, lang or content type.
 */
public class TextTemplate implements ToXContent {

    public static final String DEFAULT_TEMPLATE_LANG = "mustache";

    private final Script script;
    private final String inlineTemplate;

    public TextTemplate(String template) {
        this.script = null;
        this.inlineTemplate = template;
    }

    public TextTemplate(String template, @Nullable XContentType contentType, ScriptType type,
                        @Nullable Map<String, Object> params) {
        this.script = new Script(template, type, DEFAULT_TEMPLATE_LANG, params, contentType);
        this.inlineTemplate = null;
    }

    public TextTemplate(Script script) {
        this.script = script;
        this.inlineTemplate = null;
    }

    public Script getScript() {
        return script;
    }

    public String getTemplate() {
        return script != null ? script.getScript() : inlineTemplate;
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
                Objects.equals(inlineTemplate, template1.inlineTemplate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(script, inlineTemplate);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (script != null) {
            script.toXContent(builder, params);
        } else {
            builder.value(inlineTemplate);
        }
        return builder;
    }

    public static TextTemplate parse(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return new TextTemplate(parser.text());
        } else {
            return new TextTemplate(Script.parse(parser, ParseFieldMatcher.STRICT, DEFAULT_TEMPLATE_LANG));
        }
    }
}

