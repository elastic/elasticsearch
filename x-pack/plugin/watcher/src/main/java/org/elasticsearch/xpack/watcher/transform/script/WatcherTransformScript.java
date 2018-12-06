/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transform.script;

import org.elasticsearch.script.ParameterMap;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A script to transform the results of a watch execution.
 */
public abstract class WatcherTransformScript {
    public static final String[] PARAMETERS = {};

    private static final Map<String, String> DEPRECATIONS;

    static {
        Map<String, String> deprecations = new HashMap<>();
        deprecations.put(
            "ctx",
            "Accessing variable [ctx] via [params.ctx] from within a watcher_transform script " +
                "is deprecated in favor of directly accessing [ctx]."
        );
        DEPRECATIONS = Collections.unmodifiableMap(deprecations);
    }

    private final Map<String, Object> params;
    // TODO: ctx should have its members extracted into execute parameters, but it needs to be a member bwc access in params
    private final Map<String, Object> ctx;

    public WatcherTransformScript(Map<String, Object> params, WatchExecutionContext watcherContext, Payload payload) {
        Map<String, Object> paramsWithCtx = new HashMap<>(params);
        Map<String, Object> ctx = Variables.createCtx(watcherContext, payload);
        paramsWithCtx.put("ctx", ctx);
        this.params = new ParameterMap(Collections.unmodifiableMap(paramsWithCtx), DEPRECATIONS);
        this.ctx = ctx;
    }

    public abstract Object execute();

    public Map<String, Object> getParams() {
        return params;
    }

    public Map<String, Object> getCtx() {
        return ctx;
    }

    public interface Factory {
        WatcherTransformScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext, Payload payload);
    }

    public static ScriptContext<Factory> CONTEXT = new ScriptContext<>("watcher_transform", Factory.class);
}
