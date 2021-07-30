/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.ack;

import org.elasticsearch.action.ActionType;

/**
 * This action acks a watch in memory, and the index
 */
public class AckWatchAction extends ActionType<AckWatchResponse> {

    public static final AckWatchAction INSTANCE = new AckWatchAction();
    public static final String NAME = "cluster:admin/xpack/watcher/watch/ack";

    private AckWatchAction() {
        super(NAME, AckWatchResponse::new);
    }
}
