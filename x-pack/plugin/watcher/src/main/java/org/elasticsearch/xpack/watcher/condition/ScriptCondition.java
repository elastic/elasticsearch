/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;

import java.io.IOException;

/**
 * This class executes a script against the ctx payload and returns a boolean
 */
public final class ScriptCondition implements ExecutableCondition {
    public static final String TYPE = "script";
    private static final Result MET = new Result(null, TYPE, true);
    private static final Result UNMET = new Result(null, TYPE, false);

    private final Script script;
    private final WatcherConditionScript.Factory scriptFactory;

    public ScriptCondition(Script script) {
        this.script = script;
        this.scriptFactory = null;
    }

    ScriptCondition(Script script, ScriptService scriptService) {
        this.script = script;
        this.scriptFactory = scriptService.compile(script, WatcherConditionScript.CONTEXT);
    }

    public Script getScript() {
        return script;
    }

    public static ScriptCondition parse(ScriptService scriptService, String watchId, XContentParser parser) throws IOException {
        try {
            Script script = Script.parse(parser);
            return new ScriptCondition(script, scriptService);
        } catch (ElasticsearchParseException pe) {
            throw new ElasticsearchParseException(
                "could not parse [{}] condition for watch [{}]. failed to parse script",
                pe,
                TYPE,
                watchId
            );
        }
    }

    @Override
    public Result execute(WatchExecutionContext ctx) {
        return doExecute(ctx);
    }

    public Result doExecute(WatchExecutionContext ctx) {
        WatcherConditionScript conditionScript = scriptFactory.newInstance(script.getParams(), ctx);
        return conditionScript.execute() ? MET : UNMET;
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
