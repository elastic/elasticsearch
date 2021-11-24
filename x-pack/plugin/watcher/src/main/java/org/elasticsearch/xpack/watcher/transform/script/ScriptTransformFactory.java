/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.transform.script;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.transform.TransformFactory;

import java.io.IOException;

public class ScriptTransformFactory extends TransformFactory<ScriptTransform, ScriptTransform.Result, ExecutableScriptTransform> {

    private final ScriptService scriptService;

    public ScriptTransformFactory(ScriptService scriptService) {
        super(LogManager.getLogger(ExecutableScriptTransform.class));
        this.scriptService = scriptService;
    }

    @Override
    public String type() {
        return ScriptTransform.TYPE;
    }

    @Override
    public ScriptTransform parseTransform(String watchId, XContentParser parser) throws IOException {
        return ScriptTransform.parse(watchId, parser);
    }

    @Override
    public ExecutableScriptTransform createExecutable(ScriptTransform transform) {
        return new ExecutableScriptTransform(transform, transformLogger, scriptService);
    }
}
