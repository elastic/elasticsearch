/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.ActionType;

public class SqlQueryAction extends ActionType<SqlQueryResponse> {

    public static final SqlQueryAction INSTANCE = new SqlQueryAction();
    public static final String NAME = "indices:data/read/sql";

    private SqlQueryAction() {
        super(NAME, SqlQueryResponse::new);
    }
}
