/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.logging;

import org.elasticsearch.common.logging.activity.ActivityCaptureContextBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;

public class SqlCaptureContextBuilder extends ActivityCaptureContextBuilder<SqlCaptureContext, SqlQueryRequest, SqlQueryResponse> {

    public SqlCaptureContextBuilder(Task task, SqlQueryRequest request) {
        super(task, request);
    }

    @Override
    public SqlCaptureContext build(SqlQueryResponse response) {
        return new SqlCaptureContext(task, request, elapsed(), response);
    }

    @Override
    public SqlCaptureContext build(Exception e) {
        return new SqlCaptureContext(task, request, elapsed(), e);
    }
}
