/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.condition.script;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.xpack.common.ScriptServiceProxy;
import org.elasticsearch.xpack.watcher.condition.ExecutableCondition;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.util.Map;

import static org.elasticsearch.xpack.watcher.support.Exceptions.invalidScript;

/**
 * This class executes a script against the ctx payload and returns a boolean
 */
public class ExecutableScriptCondition extends ExecutableCondition<ScriptCondition, ScriptCondition.Result> {

    private final ScriptServiceProxy scriptService;
    private final CompiledScript compiledScript;

    public ExecutableScriptCondition(ScriptCondition condition, ESLogger logger, ScriptServiceProxy scriptService) {
        super(condition, logger);
        this.scriptService = scriptService;
        try {
            compiledScript = scriptService.compile(condition.script);
        } catch (Exception e) {
            throw invalidScript("failed to compile script [{}] with lang [{}] of type [{}]", e, condition.script.script(),
                    condition.script.lang(), condition.script.type(), e);
        }
    }

    @Override
    public ScriptCondition.Result execute(WatchExecutionContext ctx) {
        return doExecute(ctx);
    }

    public ScriptCondition.Result doExecute(WatchExecutionContext ctx) {
        Map<String, Object> parameters = Variables.createCtxModel(ctx, ctx.payload());
        if (condition.script.params() != null && !condition.script.params().isEmpty()) {
            parameters.putAll(condition.script.params());
        }
        ExecutableScript executable = scriptService.executable(compiledScript, parameters);
        Object value = executable.run();
        if (value instanceof Boolean) {
            return (Boolean) value ? ScriptCondition.Result.MET : ScriptCondition.Result.UNMET;
        }
        throw invalidScript("condition [{}] must return a boolean value (true|false) but instead returned [{}]", type(), ctx.watch().id(),
                condition.script.script(), value);
    }
}
