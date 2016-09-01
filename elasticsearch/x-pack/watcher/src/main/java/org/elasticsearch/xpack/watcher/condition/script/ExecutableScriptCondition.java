/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition.script;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.xpack.watcher.support.Exceptions.invalidScript;

/**
 * This class executes a script against the ctx payload and returns a boolean
 */
public class ExecutableScriptCondition extends ExecutableCondition<ScriptCondition, ScriptCondition.Result> {

    private final ScriptService scriptService;
    private final CompiledScript compiledScript;

    public ExecutableScriptCondition(ScriptCondition condition, Logger logger, ScriptService scriptService) {
        super(condition, logger);
        this.scriptService = scriptService;
        try {
            Script script = new Script(condition.script.getScript(), condition.script.getType(),
                                       condition.script.getLang(), condition.script.getParams());
            compiledScript = scriptService.compile(script, Watcher.SCRIPT_CONTEXT, Collections.emptyMap());
        } catch (Exception e) {
            throw invalidScript("failed to compile script [{}]", e, condition.script, e);
        }
    }

    @Override
    public ScriptCondition.Result execute(WatchExecutionContext ctx) {
        return doExecute(ctx);
    }

    public ScriptCondition.Result doExecute(WatchExecutionContext ctx) {
        Map<String, Object> parameters = Variables.createCtxModel(ctx, ctx.payload());
        if (condition.script.getParams() != null && !condition.script.getParams().isEmpty()) {
            parameters.putAll(condition.script.getParams());
        }
        ExecutableScript executable = scriptService.executable(compiledScript, parameters);
        Object value = executable.run();
        if (value instanceof Boolean) {
            return (Boolean) value ? ScriptCondition.Result.MET : ScriptCondition.Result.UNMET;
        }
        throw invalidScript("condition [{}] must return a boolean value (true|false) but instead returned [{}]", type(), ctx.watch().id(),
                condition.script, value);
    }
}
