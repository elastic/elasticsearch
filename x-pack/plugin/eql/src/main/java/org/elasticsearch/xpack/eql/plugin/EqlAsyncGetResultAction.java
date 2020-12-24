/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.plugin;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.core.eql.EqlAsyncActionNames;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

public class EqlAsyncGetResultAction extends ActionType<EqlSearchResponse> {
    public static final EqlAsyncGetResultAction INSTANCE = new EqlAsyncGetResultAction();

    private EqlAsyncGetResultAction() {
        super(EqlAsyncActionNames.EQL_ASYNC_GET_RESULT_ACTION_NAME, EqlSearchResponse::new);
    }
}
