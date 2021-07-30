/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.common.text;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import java.io.IOException;
import java.util.HashMap;
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

    private final Script script;
    private final String inlineTemplate;
    private final boolean mayRequireCompilation;

    public TextTemplate(String template) {
        this.script = null;
        this.inlineTemplate = template;
        this.mayRequireCompilation = template.contains("{{");
    }

    public TextTemplate(String template, @Nullable XContentType contentType, ScriptType type,
                        @Nullable Map<String, Object> params) {
        Map<String, String> options = null;
        if (type == ScriptType.INLINE) {
            options = new HashMap<>();
            if (contentType != null) {
                options.put(Script.CONTENT_TYPE_OPTION, contentType.canonical().mediaType());
            }
        }
        if (params == null) {
            params = new HashMap<>();
        }
        this.script = new Script(type, type == ScriptType.STORED ? null : Script.DEFAULT_TEMPLATE_LANG, template, options, params);
        this.inlineTemplate = null;
        this.mayRequireCompilation = script.getType() == ScriptType.STORED || script.getIdOrCode().contains("{{");
    }

    public TextTemplate(Script script) {
        this.script = script;
        this.inlineTemplate = null;
        this.mayRequireCompilation = script.getType() == ScriptType.STORED || script.getIdOrCode().contains("{{");
    }

    public Script getScript() {
        return script;
    }

    public String getTemplate() {
        return script != null ? script.getIdOrCode() : inlineTemplate;
    }

    /**
     * Check if compilation may be required.
     * If a stored script is used, we cannot tell at this stage, so we always assume
     * that stored scripts require compilation.
     * If an inline script is used, we checked for the mustache opening brackets
     */
    public boolean mayRequireCompilation() {
        return mayRequireCompilation;
    }

    public XContentType getContentType() {
        if (script == null || script.getOptions() == null) {
            return null;
        }

        String mediaType = script.getOptions().get(Script.CONTENT_TYPE_OPTION);

        if (mediaType == null) {
            return null;
        }

        return XContentType.fromMediaType(mediaType);
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
            Script template = Script.parse(parser, Script.DEFAULT_TEMPLATE_LANG);
            return new TextTemplate(template);
        }
    }
}

