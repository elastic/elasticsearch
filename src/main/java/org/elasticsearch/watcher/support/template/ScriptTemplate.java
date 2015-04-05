/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class ScriptTemplate implements ToXContent, Template {

    public static final String DEFAULT_LANG = "mustache";

    private final Script script;
    private final ScriptServiceProxy service;

    public ScriptTemplate(ScriptServiceProxy service, String script) {
        this(service, new Script(script, ScriptService.ScriptType.INLINE, DEFAULT_LANG, Collections.<String, Object>emptyMap()));
    }

    public ScriptTemplate(ScriptServiceProxy service, Script script) {
        this.script = script;
        this.service = service;
    }

    public Script script() {
        return script;
    }

    @Override
    public String render(Map<String, Object> model) {
        Map<String, Object> mergedModel = new HashMap<>();
        mergedModel.putAll(script.params());
        mergedModel.putAll(model);
        ExecutableScript executable = service.executable(script.lang(), script.script(), script.type(), mergedModel);
        Object result = executable.run();
        if (result instanceof BytesReference) {
            return ((BytesReference) result).toUtf8();
        }
        return result.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptTemplate template = (ScriptTemplate) o;

        if (!script.equals(template.script)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return script.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return script.toXContent(builder, params);
    }

    /**
     */
    public static class Parser extends AbstractComponent implements Template.Parser<ScriptTemplate> {

        private final ScriptServiceProxy scriptService;

        @Inject
        public Parser(Settings settings, ScriptServiceProxy scriptService) {
            super(settings);
            this.scriptService = scriptService;
        }

        @Override
        public ScriptTemplate parse(XContentParser parser) throws IOException {
            // we need to parse the string here, because the default script lang is different
            // than the one Script assumes
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                return new ScriptTemplate(scriptService, parser.text());
            }
            try {
                Script script = Script.parse(parser, DEFAULT_LANG);
                return new ScriptTemplate(scriptService, script);
            } catch (Script.ParseException pe) {
                throw new ParseException("could not parse script template", pe);
            }

        }
    }

    public static class SourceBuilder implements Template.SourceBuilder {

        private final String script;
        private String lang;
        private ScriptService.ScriptType type;
        private Map<String, Object> params;

        public SourceBuilder(String script) {
            this.script = script;
        }

        public SourceBuilder lang(String lang) {
            this.lang = lang;
            return this;
        }

        public SourceBuilder setType(ScriptService.ScriptType type) {
            this.type = type;
            return this;
        }

        public SourceBuilder setParams(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (lang == null && type == null && params == null) {
                return builder.value(script);
            }
            builder.startObject();
            builder.field(Script.SCRIPT_FIELD.getPreferredName(), script);
            if (lang != null) {
                builder.field(Script.LANG_FIELD.getPreferredName(), lang);
            }
            if (type != null) {
                builder.field(Script.TYPE_FIELD.getPreferredName(), type.name().toLowerCase(Locale.ROOT));
            }
            if (this.params != null) {
                builder.field(Script.PARAMS_FIELD.getPreferredName(), this.params);
            }
            return builder.endObject();
        }
    }
}
