/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition.script;

import org.elasticsearch.alerts.AlertsSettingsException;
import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.ConditionException;
import org.elasticsearch.alerts.support.Script;
import org.elasticsearch.alerts.support.Variables;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * This class executes a script against the ctx payload and returns a boolean
 */
public class ScriptCondition extends Condition<ScriptCondition.Result> {

    public static final String TYPE = "script";

    private final ScriptServiceProxy scriptService;
    private final Script script;

    public ScriptCondition(ESLogger logger, ScriptServiceProxy scriptService, Script script) {
        super(logger);
        this.scriptService = scriptService;
        this.script = script;
    }

    @Override
    public String type() {
        return TYPE;
    }

    public Script script() {
        return script;
    }

    @Override
    public Result execute(ExecutionContext ctx) throws IOException {
        ImmutableMap<String, Object> model = ImmutableMap.<String, Object>builder()
                .putAll(script.params())
                .putAll(Variables.createCtxModel(ctx, ctx.payload()))
                .build();
        ExecutableScript executable = scriptService.executable(script.lang(), script.script(), script.type(), model);
        Object value = executable.run();
        if (value instanceof Boolean) {
            return (Boolean) value ? Result.MET : Result.UNMET;
        }
        throw new ConditionException("condition script [" + script + "] did not return a boolean value");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return script.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptCondition that = (ScriptCondition) o;

        if (!script.equals(that.script)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return script.hashCode();
    }

    public static class Parser extends AbstractComponent implements Condition.Parser<Result, ScriptCondition> {

        private final ScriptServiceProxy scriptService;

        @Inject
        public Parser(Settings settings, ScriptServiceProxy service) {
            super(settings);
            scriptService = service;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public ScriptCondition parse(XContentParser parser) throws IOException {
            try {
                Script script = Script.parse(parser);
                return new ScriptCondition(logger, scriptService, script);
            } catch (Script.ParseException pe) {
                throw new AlertsSettingsException("could not parse [script] condition", pe);
            }
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            Boolean met = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (Condition.MET_FIELD.match(currentFieldName)) {
                            met = parser.booleanValue();
                        } else {
                            throw new ConditionException("unable to parse [script] condition result. expected a boolean, got [" + parser.text() + "]");
                        }
                    } else {
                        throw new ConditionException("unable to parse [script] condition result. unexpected field [" + currentFieldName + "]");
                    }
                }
            }

            if (met == null) {
                throw new ConditionException("could not parse [script] condition result. [met] is a required field");
            }

            return met ? Result.MET : Result.UNMET;
        }
    }

    public static class Result extends Condition.Result {

        static final Result MET = new Result(true);
        static final Result UNMET = new Result(false);

        private Result(boolean met) {
            super(TYPE, met);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field(MET_FIELD.getPreferredName(), met())
                    .endObject();
        }
    }

    public static class SourceBuilder implements Condition.SourceBuilder {

        private String script;
        private String lang = ScriptService.DEFAULT_LANG;
        private ScriptService.ScriptType type = ScriptService.ScriptType.INLINE;
        private Map<String, Object> params = Collections.emptyMap();

        public SourceBuilder script(String script) {
            this.script = script;
            return this;
        }

        public SourceBuilder lang(String lang) {
            this.lang = lang;
            return this;
        }

        public SourceBuilder type(ScriptService.ScriptType type) {
            this.type = type;
            return this;
        }

        public SourceBuilder type(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return new Script(script, type, lang, this.params).toXContent(builder, params);
        }
    }
}
