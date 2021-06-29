/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.execute;

import org.elasticsearch.action.ActionType;

/**
 * This action executes a watch, either ignoring the schedule and condition or just the schedule and can execute a subset of the actions,
 * optionally persisting the history entry
 */
public class ExecuteWatchAction extends ActionType<ExecuteWatchResponse> {

    public static final ExecuteWatchAction INSTANCE = new ExecuteWatchAction();
    public static final String NAME = "cluster:admin/xpack/watcher/watch/execute";

    private ExecuteWatchAction() {
        super(NAME, ExecuteWatchResponse::new);
    }
}
