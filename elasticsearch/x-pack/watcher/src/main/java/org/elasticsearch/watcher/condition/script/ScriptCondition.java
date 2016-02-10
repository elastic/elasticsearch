/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.script;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.condition.Condition;
import org.elasticsearch.watcher.support.Script;

import java.io.IOException;

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
        } catch (ElasticsearchParseException pe) {
            throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. failed to parse script", pe, TYPE,
                    watchId);
        }
    }

    public static Builder builder(Script script) {
        return new Builder(script);
    }

    public static class Result extends Condition.Result {

        static final Result MET = new Result(true);
        static final Result UNMET = new Result(false);

        private Result(boolean met) {
            super(TYPE, met);
        }

        Result(Exception e) {
            super(TYPE, e);
        }

        @Override
        protected XContentBuilder typeXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    public static class Builder implements Condition.Builder<ScriptCondition> {

        private final Script script;

        private Builder(Script script) {
            this.script = script;
        }

        @Override
        public ScriptCondition build() {
            return new ScriptCondition(script);
        }
    }
}
