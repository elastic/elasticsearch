/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.condition;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.util.Map;

/**
 * A script to determine whether a watch should be run.
 */
public abstract class WatcherConditionScript {

    public static final String[] PARAMETERS = {};

    private final Map<String, Object> params;
    // TODO: ctx should have its members extracted into execute parameters, but it needs to be a member for bwc access in params
    private final Map<String, Object> ctx;

    public WatcherConditionScript(Map<String, Object> params, WatchExecutionContext watcherContext) {
        this.params = params;
        this.ctx = Variables.createCtx(watcherContext, watcherContext.payload());
    }

    public abstract boolean execute();

    public Map<String, Object> getParams() {
        return params;
    }

    public Map<String, Object> getCtx() {
        return ctx;
    }

    public interface Factory {
        WatcherConditionScript newInstance(Map<String, Object> params, WatchExecutionContext watcherContext);
    }

    public static ScriptContext<Factory> CONTEXT = new ScriptContext<>(
        "watcher_condition",
        Factory.class,
        200,
        TimeValue.timeValueMillis(0),
        false,
        true
    );
}
