/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.put;

import org.elasticsearch.action.Action;

/**
 * This action puts an watch into the watch index and adds it to the scheduler
 */
public class PutWatchAction extends Action<PutWatchResponse> {

    public static final PutWatchAction INSTANCE = new PutWatchAction();
    public static final String NAME = "cluster:admin/xpack/watcher/watch/put";

    private PutWatchAction() {
        super(NAME);
    }

    @Override
    public PutWatchResponse newResponse() {
        return new PutWatchResponse();
    }
}
