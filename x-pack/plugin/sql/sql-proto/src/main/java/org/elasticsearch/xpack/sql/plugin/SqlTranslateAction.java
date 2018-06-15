/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.Action;

/**
 * Sql action for translating SQL queries into ES requests
 */
public class SqlTranslateAction extends Action<SqlTranslateRequest, SqlTranslateResponse> {

    public static final SqlTranslateAction INSTANCE = new SqlTranslateAction();
    public static final String NAME = "indices:data/read/sql/translate";

    private SqlTranslateAction() {
        super(NAME);
    }

    @Override
    public SqlTranslateResponse newResponse() {
        return new SqlTranslateResponse();
    }
}