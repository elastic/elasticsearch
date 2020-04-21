/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionType;

public final class SubmitAsyncSearchAction extends ActionType<AsyncSearchResponse> {
    public static final SubmitAsyncSearchAction INSTANCE = new SubmitAsyncSearchAction();
    public static final String NAME = "indices:data/read/async_search/submit";

    private SubmitAsyncSearchAction() {
        super(NAME, AsyncSearchResponse::new);
    }
}
