/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.ql.async.QlStatusResponse;

import static org.elasticsearch.xpack.core.sql.SqlAsyncActionNames.SQL_ASYNC_GET_STATUS_ACTION_NAME;

public class SqlAsyncGetStatusAction extends ActionType<QlStatusResponse> {
    public static final SqlAsyncGetStatusAction INSTANCE = new SqlAsyncGetStatusAction();
    public static final String NAME = SQL_ASYNC_GET_STATUS_ACTION_NAME;

    private SqlAsyncGetStatusAction() {
        super(NAME, QlStatusResponse::new);
    }
}
