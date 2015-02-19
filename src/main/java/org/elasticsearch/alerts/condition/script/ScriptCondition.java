/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.condition.script;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.condition.Condition;
import org.elasticsearch.alerts.condition.ConditionException;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Locale;

/**
 * This class executes a script against the ctx payload and returns a boolean
 */
public class ScriptCondition extends Condition<ScriptCondition.Result> {

    public static final String TYPE = "script";

    private final String script;
    private final ScriptService.ScriptType scriptType;
    private final String scriptLang;

    private final ScriptServiceProxy scriptService;

    public ScriptCondition(ESLogger logger, ScriptServiceProxy scriptService, String script, ScriptService.ScriptType scriptType, String scriptLang) {
        super(logger);
        this.script = script;
        this.scriptType = scriptType;
        this.scriptLang = scriptLang;
        this.scriptService = scriptService;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(ExecutionContext ctx) throws IOException {
        return processPayload(ctx.payload());
    }

    protected Result processPayload(Payload payload) {
        ExecutableScript executable = scriptService.executable(scriptLang, script, scriptType, payload.data());
        Object value = executable.run();
        if (value instanceof Boolean) {
            return (Boolean) value ? Result.MET : Result.UNMET;
        }
        throw new ConditionException("condition script [" + script + "] did not return a boolean value");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ScriptService.SCRIPT_INLINE.getPreferredName(), script);
        builder.field(Parser.SCRIPT_TYPE_FIELD.getPreferredName(), scriptType);
        builder.field(ScriptService.SCRIPT_LANG.getPreferredName(), scriptLang);
        return builder.endObject();
    }

    public static class Parser extends AbstractComponent implements Condition.Parser<Result, ScriptCondition> {

        public static ParseField SCRIPT_TYPE_FIELD = new ParseField("script_type");

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
            String scriptLang = null;
            String script = null;
            ScriptService.ScriptType scriptType = ScriptService.ScriptType.INLINE;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ((token.isValue() || token == XContentParser.Token.START_OBJECT) && currentFieldName != null) {
                    if (ScriptService.SCRIPT_ID.match(currentFieldName)) {
                        script = parser.text();
                        scriptType = ScriptService.ScriptType.INDEXED;
                    } else if (ScriptService.SCRIPT_INLINE.match(currentFieldName)) {
                        script = parser.text();
                    } else if (SCRIPT_TYPE_FIELD.match(currentFieldName)) {
                        String value = parser.text();
                        try {
                            scriptType = ScriptService.ScriptType.valueOf(value.toUpperCase(Locale.ROOT));
                        } catch (IllegalArgumentException iae) {
                            throw new ConditionException("could not parse [script] condition. unknown script type [" + value + "]");
                        }
                    } else if (ScriptService.SCRIPT_LANG.match(currentFieldName)) {
                        scriptLang = parser.text();
                    } else {
                        throw new ConditionException("could not parse [script] condition. unexpected field [" + currentFieldName + "]");
                    }
                }
            }

            if (script == null) {
                throw new ConditionException("could not parse [script] condition. either [script] or [script_id] must be provided");
            }

            return new ScriptCondition(logger, scriptService, script, scriptType, scriptLang);
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
}
