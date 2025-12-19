/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

class FakeTransformTask extends Task implements PutTransformAction.TransformTaskMatcher {
    FakeTransformTask(long id, String type, String action, String description, TaskId parentTask, Map<String, String> headers) {
        super(id, type, action, description, parentTask, headers);
    }

    FakeTransformTask(
        long id,
        String type,
        String action,
        String description,
        TaskId parentTask,
        long startTime,
        long startTimeNanos,
        Map<String, String> headers
    ) {
        super(id, type, action, description, parentTask, startTime, startTimeNanos, headers);
    }
}
