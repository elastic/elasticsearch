/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.xpack.ql.async.QlStatusResponse;

public class SqlAsyncStatusAction extends ActionType<QlStatusResponse> {
    public static final SqlAsyncStatusAction INSTANCE = new SqlAsyncStatusAction();
    public static final String NAME = "cluster:monitor/xpack/sql/async/status";

    private SqlAsyncStatusAction() {
        super(NAME, QlStatusResponse::new);
    }
}
