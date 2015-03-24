/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform;

import org.elasticsearch.watcher.WatcherSettingsException;
import org.elasticsearch.watcher.watch.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.watcher.support.Script;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.watcher.support.Variables.createCtxModel;

/**
 *
 */
public class ScriptTransform extends Transform<ScriptTransform.Result> {

    public static final String TYPE = "script";

    private final ScriptServiceProxy scriptService;
    private final Script script;

    public ScriptTransform(ScriptServiceProxy scriptService, Script script) {
        this.scriptService = scriptService;
        this.script = script;
    }

    @Override
    public String type() {
        return TYPE;
    }

    Script script() {
        return script;
    }

    @Override
    public Result apply(WatchExecutionContext ctx, Payload payload) throws IOException {
        Map<String, Object> model = new HashMap<>();
        model.putAll(script.params());
        model.putAll(createCtxModel(ctx, payload));
        ExecutableScript executable = scriptService.executable(script.lang(), script.script(), script.type(), model);
        Object value = executable.run();
        if (!(value instanceof Map)) {
            throw new TransformException("illegal [script] transform [" + script.script() + "]. script must output a Map<String, Object> structure but outputted [" + value.getClass().getSimpleName() + "] instead");
        }
        return new Result(TYPE, new Payload.Simple((Map<String, Object>) value));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(script);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptTransform transform = (ScriptTransform) o;

        if (!script.equals(transform.script)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return script.hashCode();
    }

    public static class Result extends Transform.Result {

        public Result(String type, Payload payload) {
            super(type, payload);
        }

        @Override
        protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    public static class Parser implements Transform.Parser<Result, ScriptTransform> {

        private final ScriptServiceProxy scriptService;

        @Inject
        public Parser(ScriptServiceProxy scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public ScriptTransform parse(XContentParser parser) throws IOException {
            Script script = null;
            try {
                script = Script.parse(parser);
            } catch (Script.ParseException pe) {
                throw new WatcherSettingsException("could not parse [script] transform", pe);
            }
            return new ScriptTransform(scriptService, script);
        }

        @Override
        public Result parseResult(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.currentToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new TransformException("could not parse [script] transform result. expected an object, but found [" + token + "]");
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.FIELD_NAME || !PAYLOAD_FIELD.match(parser.currentName())) {
                throw new TransformException("could not parse [script] transform result. expected a payload field, but found [" + token + "]");
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new TransformException("could not parse [script] transform result. expected a payload object, but found [" + token + "]");
            }
            return new Result(TYPE, new Payload.XContent(parser));
        }
    }

    public static class SourceBuilder implements Transform.SourceBuilder {

        private final Script script;

        public SourceBuilder(String script) {
            this(new Script(script));
        }

        public SourceBuilder(Script script) {
            this.script = script;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return script.toXContent(builder, params);
        }
    }
}
