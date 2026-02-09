/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.tasks.TaskId;

import java.util.Objects;

/** Reason for a task failing because it's been relocated to another node to continue execution. */
public class TaskRelocatedException extends ElasticsearchException {

    private final TaskId originalTaskId;
    private final TaskId relocatedTaskId;

    public TaskRelocatedException(final TaskId originalTaskId, final TaskId relocatedTaskId) {
        super("Task {} was relocated to {}", Objects.requireNonNull(originalTaskId), Objects.requireNonNull(relocatedTaskId));
        this.originalTaskId = originalTaskId;
        this.relocatedTaskId = relocatedTaskId;
    }

    public TaskId getOriginalTaskId() {
        return originalTaskId;
    }

    public TaskId getRelocatedTaskId() {
        return relocatedTaskId;
    }
}
