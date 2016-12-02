/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transform.script;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.xpack.watcher.transform.TransformFactory;

import java.io.IOException;

public class ScriptTransformFactory extends TransformFactory<ScriptTransform, ScriptTransform.Result, ExecutableScriptTransform> {

    private final ScriptService scriptService;

    public ScriptTransformFactory(Settings settings, ScriptService scriptService) {
        super(Loggers.getLogger(ExecutableScriptTransform.class, settings));
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
