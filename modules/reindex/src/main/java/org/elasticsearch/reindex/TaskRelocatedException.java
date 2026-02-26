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

import java.util.List;
import java.util.Optional;

/** Reason for a task failing because it's been relocated to another node to continue execution. */
public class TaskRelocatedException extends ElasticsearchException {

    public TaskRelocatedException() {
        super("Task was relocated");
    }

    public void setOriginalAndRelocatedTaskIdMetadata(final TaskId originalTaskId, final TaskId relocatedTaskId) {
        this.addMetadata("es.original_task_id", originalTaskId.toString()); // implicit nullchecks
        this.addMetadata("es.relocated_task_id", relocatedTaskId.toString());
    }

    public Optional<String> getRelocatedTaskId() {
        return Optional.ofNullable(this.getMetadata("es.relocated_task_id")).filter(taskIds -> taskIds.size() == 1).map(List::getFirst);
    }
}
