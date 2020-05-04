/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;

import java.util.Map;

public class EqlSearchTask extends CancellableTask {
    private final String description;
    private final AsyncExecutionId asyncExecutionId;

    public EqlSearchTask(long id, String type, String action, String description, TaskId parentTaskId,
                         Map<String, String> headers, AsyncExecutionId asyncExecutionId) {
        super(id, type, action, null, parentTaskId, headers);
        this.description = description;
        this.asyncExecutionId = asyncExecutionId;
    }

    public AsyncExecutionId getAsyncExecutionId() {
        return asyncExecutionId;
    }

    @Override
    public boolean shouldCancelChildrenOnCancellation() {
        return true;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
