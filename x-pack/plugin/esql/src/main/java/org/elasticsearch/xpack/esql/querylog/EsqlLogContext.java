/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.common.logging.activity.ActivityLoggerContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

public class EsqlLogContext extends ActivityLoggerContext {
    public static final String TYPE = "esql";
    private final EsqlQueryRequest request;
    private final @Nullable EsqlQueryResponse response;

    EsqlLogContext(Task task, EsqlQueryRequest request, EsqlQueryResponse response) {
        super(task, TYPE, response.getExecutionInfo().overallTook().nanos());
        this.request = request;
        this.response = response;
    }

    EsqlLogContext(Task task, EsqlQueryRequest request, long tookInNanos, Exception error) {
        super(task, TYPE, tookInNanos, error);
        this.request = request;
        this.response = null;
    }

    String getQuery() {
        return request.query();
    }
}
