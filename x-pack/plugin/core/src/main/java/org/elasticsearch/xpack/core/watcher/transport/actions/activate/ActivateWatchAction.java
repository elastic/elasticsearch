/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.activate;

import org.elasticsearch.action.ActionType;

/**
 * This action acks a watch in memory, and the index
 */
public class ActivateWatchAction extends ActionType<ActivateWatchResponse> {

    public static final ActivateWatchAction INSTANCE = new ActivateWatchAction();
    public static final String NAME = "cluster:admin/xpack/watcher/watch/activate";

    private ActivateWatchAction() {
        super(NAME, ActivateWatchResponse::new);
    }
}
