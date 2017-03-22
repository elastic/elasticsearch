/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

/**
 * Base class for a request for a persistent task
 */
public abstract class PersistentTaskRequest extends ActionRequest implements NamedWriteable, ToXContent {
    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId) {
        return new NodePersistentTask(id, type, action, getDescription(), parentTaskId);
    }
}