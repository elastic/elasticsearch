/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.script;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.support.Script;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ScriptCondition implements Condition {

    public static final String TYPE = "script";

    final Script script;

    public ScriptCondition(Script script) {
        this.script = script;
    }

    @Override
    public final String type() {
        return TYPE;
    }

    public Script getScript() {
        return script;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return script.toXContent(builder, params);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptCondition condition = (ScriptCondition) o;

        return script.equals(condition.script);
    }

    @Override
    public int hashCode() {
        return script.hashCode();
    }

    public static ScriptCondition parse(String watchId, XContentParser parser) throws IOException {
        try {
            Script script = Script.parse(parser);
            return new ScriptCondition(script);
        } catch (Script.ParseException pe) {
            throw new ScriptConditionException("could not parse [{}] condition for watch [{}]. failed to parse script", pe, TYPE, watchId);
        }
    }

    public static Builder builder(String script) {
        return new Builder(script);
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
                    .field(Field.MET.getPreferredName(), met)
                    .endObject();
        }

        public static Result parse(String watchId, XContentParser parser) throws IOException {
            Boolean met = null;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (Field.MET.match(currentFieldName)) {
                        met = parser.booleanValue();
                    } else {
                        throw new ScriptConditionException("could not parse [{}] condition result for watch [{}]. unexpected boolean field [{}]", TYPE, watchId, currentFieldName);
                    }
                } else {
                    throw new ScriptConditionException("could not parse [{}] condition result for watch [{}]. unexpected token [{}]", TYPE, watchId, token);
                }
            }

            if (met == null) {
                throw new ScriptConditionException("could not parse [{}] condition result for watch [{}]. missing required [{}] field", TYPE, watchId, Field.MET.getPreferredName());
            }

            return met ? ScriptCondition.Result.MET : ScriptCondition.Result.UNMET;
        }
    }

    public static class Builder implements Condition.Builder<ScriptCondition> {

        private final String script;
        private ScriptService.ScriptType type = Script.DEFAULT_TYPE;
        private String lang = Script.DEFAULT_LANG;
        private ImmutableMap.Builder<String, Object> vars = ImmutableMap.builder();

        private Builder(String script) {
            this.script = script;
        }

        public Builder setType(ScriptService.ScriptType type) {
            this.type = type;
            return this;
        }

        public Builder setLang(String lang) {
            this.lang = lang;
            return this;
        }

        public Builder addVars(Map<String, Object> vars) {
            this.vars.putAll(vars);
            return this;
        }

        public Builder setVar(String name, Object value) {
            this.vars.put(name, value);
            return this;
        }

        @Override
        public ScriptCondition build() {
            return new ScriptCondition(new Script(this.script, type, lang, vars.build()));
        }
    }
}
