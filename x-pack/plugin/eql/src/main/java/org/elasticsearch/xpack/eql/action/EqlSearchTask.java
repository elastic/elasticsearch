/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTask;

import java.util.Map;

public class EqlSearchTask extends CancellableTask implements AsyncTask {
    private final String description;
    private final AsyncExecutionId asyncExecutionId;
    private final Map<String, String> originHeaders;

    public EqlSearchTask(long id, String type, String action, String description, TaskId parentTaskId,
                         Map<String, String> headers, Map<String, String> originHeaders, AsyncExecutionId asyncExecutionId) {
        super(id, type, action, null, parentTaskId, headers);
        this.description = description;
        this.asyncExecutionId = asyncExecutionId;
        this.originHeaders = originHeaders;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Map<String, String> getOriginHeaders() {
        return originHeaders;
    }

    @Override
    public AsyncExecutionId getExecutionId() {
        return asyncExecutionId;
    }
}
