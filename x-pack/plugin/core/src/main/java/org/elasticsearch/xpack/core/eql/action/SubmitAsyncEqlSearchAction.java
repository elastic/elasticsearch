/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.eql.action;

import org.elasticsearch.action.ActionType;

public final class SubmitAsyncEqlSearchAction extends ActionType<AsyncEqlSearchResponse> {
    public static final SubmitAsyncEqlSearchAction INSTANCE = new SubmitAsyncEqlSearchAction();
    public static final String NAME = "indices:data/read/eql/async_search/submit";

    private SubmitAsyncEqlSearchAction() {
        super(NAME, AsyncEqlSearchResponse::new);
    }
}
