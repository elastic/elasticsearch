/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.delete;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.protocol.xpack.watcher.DeleteWatchResponse;

/**
 * This action deletes an watch from in memory, the scheduler and the index
 */
public class DeleteWatchAction extends ActionType<DeleteWatchResponse> {

    public static final DeleteWatchAction INSTANCE = new DeleteWatchAction();
    public static final String NAME = "cluster:admin/xpack/watcher/watch/delete";

    private DeleteWatchAction() {
        super(NAME, DeleteWatchResponse::new);
    }
}
