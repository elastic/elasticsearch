/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.logging;

import org.elasticsearch.common.logging.action.ActionLoggerContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;

public class SqlLogContext extends ActionLoggerContext {
    public static final String TYPE = "sql";
    private final SqlQueryRequest request;

    SqlLogContext(Task task, SqlQueryRequest request, long tookInNanos) {
        super(task, TYPE, tookInNanos);
        this.request = request;
    }

    SqlLogContext(Task task, SqlQueryRequest request, Exception error) {
        super(task, TYPE, error);
        this.request = request;
    }

    String getQuery() {
        return request.query();
    }
}
