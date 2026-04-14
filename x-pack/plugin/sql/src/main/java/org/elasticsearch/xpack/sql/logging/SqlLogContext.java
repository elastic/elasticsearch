/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.logging;

import org.elasticsearch.common.logging.activity.QueryLoggerContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;

public class SqlLogContext extends QueryLoggerContext {
    public static final String TYPE = "sql";
    private final SqlQueryRequest request;
    private final SqlQueryResponse response;

    SqlLogContext(Task task, SqlQueryRequest request, long tookInNanos, SqlQueryResponse response) {
        super(task, TYPE, tookInNanos);
        this.request = request;
        this.response = response;
    }

    SqlLogContext(Task task, SqlQueryRequest request, long tookInNanos, Exception error) {
        super(task, TYPE, tookInNanos, error);
        this.request = request;
        this.response = null;
    }

    @Override
    public String getQuery() {
        return request.query();
    }

    @Override
    public int getResultCount() {
        return Math.clamp(response == null ? 0 : response.size(), 0, Integer.MAX_VALUE);
    }

    @Override
    public String[] getIndices() {
        // TODO: figure out how to extract indices for SQL
        return null;
    }
}
