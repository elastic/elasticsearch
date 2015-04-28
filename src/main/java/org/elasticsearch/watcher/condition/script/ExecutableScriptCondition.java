/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.condition.script;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.groovy.GroovyScriptExecutionException;
import org.elasticsearch.watcher.condition.ExecutableCondition;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;

import java.io.IOException;
import java.util.Map;

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
            throw new ScriptConditionValidationException("failed to compile script [{}] with lang [{}] of type [{}]", e, condition.script.script(), condition.script.lang(), condition.script.type(), e);
        }
    }

    @Override
    public ScriptCondition.Result execute(WatchExecutionContext ctx) throws IOException {
        try {
            Map<String, Object> parameters = Variables.createCtxModel(ctx, ctx.payload());
            if (condition.script.params() != null && !condition.script.params().isEmpty()) {
                parameters.putAll(condition.script.params());
            }
            ExecutableScript executable = scriptService.executable(compiledScript, parameters);
            Object value = executable.run();
            if (value instanceof Boolean) {
                return (Boolean) value ? ScriptCondition.Result.MET : ScriptCondition.Result.UNMET;
            }
            throw new ScriptConditionException("failed to execute [{}] condition for watch [{}]. script [{}] must return a boolean value (true|false) but instead returned [{}]", type(), ctx.watch().id(), condition.script.script(), value);
        } catch (GroovyScriptExecutionException gsee) {
            throw new ScriptConditionException("failed to execute [{}] condition for watch [{}]. script [{}] threw an exception", gsee, type(), ctx.watch().id(), condition.script.script());
        }
    }
}
