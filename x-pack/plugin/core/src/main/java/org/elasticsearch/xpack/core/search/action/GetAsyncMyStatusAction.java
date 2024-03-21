/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionType;

/**
 * TODO: DOCUMENT ME
 */
public class GetAsyncMyStatusAction extends ActionType<AsyncStatusResponse> {
    public static final GetAsyncMyStatusAction INSTANCE = new GetAsyncMyStatusAction();
    public static final String NAME = "indices:data/read/async_search_status";

    private GetAsyncMyStatusAction() {
        super(NAME);
    }
}
