/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.Action;

public class SqlClearCursorAction extends Action<SqlClearCursorResponse> {

    public static final SqlClearCursorAction INSTANCE = new SqlClearCursorAction();
    public static final String NAME = "indices:data/read/sql/close_cursor";

    private SqlClearCursorAction() {
        super(NAME);
    }

    @Override
    public SqlClearCursorResponse newResponse() {
        return new SqlClearCursorResponse();
    }
}
