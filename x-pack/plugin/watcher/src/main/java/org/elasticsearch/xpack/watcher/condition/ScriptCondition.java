/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.core.watcher.support.Exceptions.illegalState;

/**
 * This class executes a script against the ctx payload and returns a boolean
 */
public final class ScriptCondition implements ExecutableCondition {
    public static final String TYPE = "script";
    private static final Result MET = new Result(null, TYPE, true);
    private static final Result UNMET = new Result(null, TYPE, false);

    private final Script script;
    private final ExecutableScript.Factory scriptFactory;

    public ScriptCondition(Script script) {
        this.script = script;
        scriptFactory = null;
    }

    ScriptCondition(Script script, ExecutableScript.Factory scriptFactory) {
        this.script = script;
        this.scriptFactory = scriptFactory;
    }

    public Script getScript() {
        return script;
    }

    public static ScriptCondition parse(ScriptService scriptService, String watchId, XContentParser parser) throws IOException {
        try {
            Script script = Script.parse(parser);
            return new ScriptCondition(script, scriptService.compile(script, Watcher.SCRIPT_EXECUTABLE_CONTEXT));
        } catch (ElasticsearchParseException pe) {
            throw new ElasticsearchParseException("could not parse [{}] condition for watch [{}]. failed to parse script", pe, TYPE,
                    watchId);
        }
    }

    @Override
    public Result execute(WatchExecutionContext ctx) {
        return doExecute(ctx);
    }

    public Result doExecute(WatchExecutionContext ctx) {
        Map<String, Object> parameters = Variables.createCtxModel(ctx, ctx.payload());
        if (script.getParams() != null && !script.getParams().isEmpty()) {
            parameters.putAll(script.getParams());
        }
        ExecutableScript executable = scriptFactory.newInstance(parameters);
        Object value = executable.run();
        if (value instanceof Boolean) {
            return (Boolean) value ? MET : UNMET;
        }
        throw illegalState("condition [{}] must return a boolean value (true|false) but instead returned [{}]", type(), ctx.watch().id(),
                script, value);
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

    @Override
    public String type() {
        return TYPE;
    }
}
