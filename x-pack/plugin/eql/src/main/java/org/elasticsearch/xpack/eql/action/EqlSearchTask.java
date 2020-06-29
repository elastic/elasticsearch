/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.eql.async.StoredAsyncTask;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

public class EqlSearchTask extends StoredAsyncTask<EqlSearchResponse> {
    public volatile AtomicReference<EqlSearchResponse> finalResponse = new AtomicReference<>();

    public EqlSearchTask(long id, String type, String action, String description, TaskId parentTaskId, Map<String, String> headers,
                         Map<String, String> originHeaders, AsyncExecutionId asyncExecutionId, TimeValue keepAlive) {
        super(id, type, action, description, parentTaskId, headers, originHeaders, asyncExecutionId, keepAlive);
    }

    @Override
    public EqlSearchResponse getCurrentResult() {
        return Objects.requireNonNullElseGet(finalResponse.get(),
            // we haven't seen the final response yet sending a initial response
            () -> new EqlSearchResponse(EqlSearchResponse.Hits.EMPTY, System.currentTimeMillis() - getStartTime(), false,
            getExecutionId().getEncoded(), true, true));
    }
}
