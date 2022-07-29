/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;

import static org.elasticsearch.xpack.core.sql.SqlAsyncActionNames.SQL_ASYNC_GET_RESULT_ACTION_NAME;

public class SqlAsyncGetResultsAction extends ActionType<SqlQueryResponse> {
    public static final SqlAsyncGetResultsAction INSTANCE = new SqlAsyncGetResultsAction();
    public static final String NAME = SQL_ASYNC_GET_RESULT_ACTION_NAME;

    private SqlAsyncGetResultsAction() {
        super(NAME, SqlQueryResponse::new);
    }
}
