/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transform.script;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.support.ScriptServiceProxy;
import org.elasticsearch.watcher.transform.TransformFactory;

import java.io.IOException;

/**
 *
 */
public class ScriptTransformFactory extends TransformFactory<ScriptTransform, ScriptTransform.Result, ExecutableScriptTransform> {

    private final ScriptServiceProxy scriptService;

    @Inject
    public ScriptTransformFactory(Settings settings, ScriptServiceProxy scriptService) {
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
