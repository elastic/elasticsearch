/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.StoredAsyncTask;

import java.util.List;
import java.util.Map;

public class EsqlQueryTask extends StoredAsyncTask<EsqlQueryResponse> {

    private EsqlExecutionInfo executionInfo;

    public EsqlQueryTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTaskId,
        Map<String, String> headers,
        Map<String, String> originHeaders,
        AsyncExecutionId asyncExecutionId,
        TimeValue keepAlive
    ) {
        super(id, type, action, description, parentTaskId, headers, originHeaders, asyncExecutionId, keepAlive);
        this.executionInfo = null;
    }

    public void setExecutionInfo(EsqlExecutionInfo executionInfo) {
        this.executionInfo = executionInfo;
    }

    public EsqlExecutionInfo executionInfo() {
        return executionInfo;
    }

    @Override
    public EsqlQueryResponse getCurrentResult() {
        // TODO it'd be nice to have the number of documents we've read from completed drivers here
        return new EsqlQueryResponse(List.of(), List.of(), 0, 0, null, false, getExecutionId().getEncoded(), true, true, executionInfo);
    }
}
