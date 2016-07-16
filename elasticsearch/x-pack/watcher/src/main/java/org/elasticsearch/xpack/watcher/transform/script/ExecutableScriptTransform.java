/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transform.script;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.WatcherScript;
import org.elasticsearch.xpack.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.watcher.watch.Payload;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.watcher.support.Exceptions.invalidScript;
import static org.elasticsearch.xpack.watcher.support.Variables.createCtxModel;

/**
 *
 */
public class ExecutableScriptTransform extends ExecutableTransform<ScriptTransform, ScriptTransform.Result> {

    private final ScriptService scriptService;
    private final CompiledScript compiledScript;

    public ExecutableScriptTransform(ScriptTransform transform, ESLogger logger, ScriptService scriptService) {
        super(transform, logger);
        this.scriptService = scriptService;
        WatcherScript script = transform.getScript();
        try {
            compiledScript = scriptService.compile(script.toScript(), WatcherScript.CTX, Collections.emptyMap());
        } catch (Exception e) {
            throw invalidScript("failed to compile script [{}] with lang [{}] of type [{}]", e, script.script(), script.lang(),
                    script.type(), e);
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
        WatcherScript script = transform.getScript();
        Map<String, Object> model = new HashMap<>();
        model.putAll(script.params());
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
