/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transform.script;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.watcher.watch.Payload;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.watcher.support.Exceptions.invalidScript;
import static org.elasticsearch.xpack.watcher.support.Variables.createCtxModel;

public class ExecutableScriptTransform extends ExecutableTransform<ScriptTransform, ScriptTransform.Result> {

    private final ScriptService scriptService;
    private final CompiledScript compiledScript;

    public ExecutableScriptTransform(ScriptTransform transform, ESLogger logger, ScriptService scriptService) {
        super(transform, logger);
        this.scriptService = scriptService;
        Script script = transform.getScript();
        try {
            compiledScript = scriptService.compile(script, Watcher.SCRIPT_CONTEXT, Collections.emptyMap());
        } catch (Exception e) {
            throw invalidScript("failed to compile script [{}]", e, script, e);
        }
    }

    @Override
    public ScriptTransform.Result execute(WatchExecutionContext ctx, Payload payload) {
        try {
            return doExecute(ctx, payload);
        } catch (Exception e) {
            logger.error("failed to execute [{}] transform for [{}]", e, ScriptTransform.TYPE, ctx.id());
            return new ScriptTransform.Result(e);
        }
    }


    ScriptTransform.Result doExecute(WatchExecutionContext ctx, Payload payload) throws IOException {
        Script script = transform.getScript();
        Map<String, Object> model = new HashMap<>();
        if (script.getParams() != null) {
            model.putAll(script.getParams());
        }
        model.putAll(createCtxModel(ctx, payload));
        ExecutableScript executable = scriptService.executable(compiledScript, model);
        Object value = executable.run();
        if (value instanceof Map) {
            return new ScriptTransform.Result(new Payload.Simple((Map<String, Object>) value));
        }
        Map<String, Object> data = new HashMap<>();
        data.put("_value", value);
        return new ScriptTransform.Result(new Payload.Simple(data));
    }
}
