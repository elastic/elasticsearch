/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionType;

public class GetAsyncStatusAction extends ActionType<AsyncStatusResponse> {
    public static final GetAsyncStatusAction INSTANCE = new GetAsyncStatusAction();
    public static final String NAME = "cluster:monitor/async_search/status";

    private GetAsyncStatusAction() {
        super(NAME, AsyncStatusResponse::new);
    }
}
