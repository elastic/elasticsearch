/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.StoredAsyncTask;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.util.Map;

import static java.util.Collections.emptyList;

public class SqlQueryTask extends StoredAsyncTask<SqlQueryResponse> {

    private final Mode mode;
    private final SqlVersion sqlVersion;
    private final boolean columnar;

    public SqlQueryTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        Map<String, String> originHeaders,
        AsyncExecutionId asyncExecutionId,
        TimeValue keepAlive,
        Mode mode,
        SqlVersion sqlVersion,
        boolean columnar
    ) {
        super(id, type, action, description, parentTaskId, headers, originHeaders, asyncExecutionId, keepAlive);
        this.mode = mode;
        this.sqlVersion = sqlVersion;
        this.columnar = columnar;
    }

    @Override
    public SqlQueryResponse getCurrentResult() {
        // for Ql searches we never store a search response in the task (neither partial, nor final)
        // we kill the task on final response, so if the task is still present, it means the search is still running
        // NB: the schema is only returned in the actual first (and currently last) response to the query
        return new SqlQueryResponse("", mode, sqlVersion, columnar, null, emptyList(), getExecutionId().getEncoded(), true, true);
    }
}
