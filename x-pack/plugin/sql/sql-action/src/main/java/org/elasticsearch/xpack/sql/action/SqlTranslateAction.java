/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionType;

/**
 * Sql action for translating SQL queries into ES requests
 */
public class SqlTranslateAction extends ActionType<SqlTranslateResponse> {

    public static final SqlTranslateAction INSTANCE = new SqlTranslateAction();
    public static final String NAME = "indices:data/read/sql/translate";

    private SqlTranslateAction() {
        super(NAME, SqlTranslateResponse::new);
    }
}
