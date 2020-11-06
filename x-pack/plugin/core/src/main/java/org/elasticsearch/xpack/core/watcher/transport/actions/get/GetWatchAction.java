/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.get;

import org.elasticsearch.action.ActionType;

/**
 * This action gets an watch by name
 */
public class GetWatchAction extends ActionType<GetWatchResponse> {

    public static final GetWatchAction INSTANCE = new GetWatchAction();
    public static final String NAME = "cluster:monitor/xpack/watcher/watch/get";

    private GetWatchAction() {
        super(NAME, GetWatchResponse::new);
    }
}
