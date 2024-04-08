/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transform.script;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.transform.ExecutableTransform;
import org.elasticsearch.xpack.core.watcher.watch.Payload;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.watcher.transform.script.ScriptTransform.TYPE;

public class ExecutableScriptTransform extends ExecutableTransform<ScriptTransform, ScriptTransform.Result> {

    private final ScriptService scriptService;

    public ExecutableScriptTransform(ScriptTransform transform, Logger logger, ScriptService scriptService) {
        super(transform, logger);
        this.scriptService = scriptService;
        Script script = transform.getScript();
        // try to compile so we catch syntax errors early
        scriptService.compile(script, WatcherTransformScript.CONTEXT);
    }

    @Override
    public ScriptTransform.Result execute(WatchExecutionContext ctx, Payload payload) {
        try {
            return doExecute(ctx, payload);
        } catch (Exception e) {
            logger.error(() -> format("failed to execute [%s] transform for [%s]", TYPE, ctx.id()), e);
            return new ScriptTransform.Result(e);
        }
    }

    ScriptTransform.Result doExecute(WatchExecutionContext ctx, Payload payload) throws IOException {
        Script script = transform.getScript();
        WatcherTransformScript.Factory factory = scriptService.compile(script, WatcherTransformScript.CONTEXT);
        WatcherTransformScript transformScript = factory.newInstance(script.getParams(), ctx, payload);
        Object value = transformScript.execute();
        // TODO: deprecate one of these styles (returning a map or returning an opaque value below)
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            final Payload.Simple simplePayload = new Payload.Simple((Map<String, Object>) value);
            return new ScriptTransform.Result(simplePayload);
        }
        Map<String, Object> data = new HashMap<>();
        data.put("_value", value);
        return new ScriptTransform.Result(new Payload.Simple(data));
    }
}
