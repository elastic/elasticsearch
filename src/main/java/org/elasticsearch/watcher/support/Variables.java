/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Payload;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public final class Variables {

    public static final String CTX = "ctx";
    public static final String WATCH_ID = "watch_id";
    public static final String EXECUTION_TIME = "execution_time";
    public static final String TRIGGER = "trigger";
    public static final String PAYLOAD = "payload";
    public static final String METADATA = "metadata";

    public static Map<String, Object> createCtxModel(WatchExecutionContext ctx, Payload payload) {
        Map<String, Object> vars = new HashMap<>();
        vars.put(WATCH_ID, ctx.watch().id());
        vars.put(EXECUTION_TIME, ctx.executionTime());
        vars.put(TRIGGER, ctx.triggerEvent().data());
        if (payload != null) {
            vars.put(PAYLOAD, payload.data());
        }
        vars.put(METADATA, ctx.watch().metadata());
        Map<String, Object> model = new HashMap<>();
        model.put(CTX, vars);
        return model;
    }


}
