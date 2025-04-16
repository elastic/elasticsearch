/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.StoredAsyncTask;

import java.util.Map;

public class EqlSearchTask extends StoredAsyncTask<EqlSearchResponse> {
    public EqlSearchTask(
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
    }

    @Override
    public EqlSearchResponse getCurrentResult() {
        // for eql searches we never store a search response in the task (neither partial, nor final)
        // we kill the task on final response, so if the task is still present, it means the search is still running
        return new EqlSearchResponse(
            EqlSearchResponse.Hits.EMPTY,
            System.currentTimeMillis() - getStartTime(),
            false,
            getExecutionId().getEncoded(),
            true,
            true,
            ShardSearchFailure.EMPTY_ARRAY
        );
    }
}
