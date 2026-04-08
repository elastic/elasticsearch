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
import java.util.Map;
import java.util.Optional;

/** Reason for a task failing because it's been relocated to another node to continue execution. */
public class TaskRelocatedException extends ElasticsearchException {

    private static final String ORIGINAL_TASK_ID_KEY = "original_task_id";
    private static final String ORIGINAL_TASK_ID_METADATA_KEY = "es." + ORIGINAL_TASK_ID_KEY;
    private static final String RELOCATED_TASK_ID_KEY = "relocated_task_id";
    private static final String RELOCATED_TASK_ID_METADATA_KEY = "es." + RELOCATED_TASK_ID_KEY;

    public TaskRelocatedException() {
        super("Task was relocated");
    }

    public TaskRelocatedException(TaskId originalTaskId, TaskId relocatedTaskId) {
        super("Task was relocated");
        assert originalTaskId.isSet() : "original task ID is not set";
        assert relocatedTaskId.isSet() : "relocated task ID is not set";
        this.addMetadata(ORIGINAL_TASK_ID_METADATA_KEY, originalTaskId.toString());
        this.addMetadata(RELOCATED_TASK_ID_METADATA_KEY, relocatedTaskId.toString());
    }

    /** Returns the relocated task ID if the map is a serialized {@link TaskRelocatedException}. */
    public static Optional<TaskId> relocatedTaskIdFromErrorMap(final Map<String, Object> errorMap) {
        if ("task_relocated_exception".equals(errorMap.get("type"))
            && errorMap.get(RELOCATED_TASK_ID_KEY) instanceof String relocatedIdStr) {
            final TaskId relocatedTaskId = new TaskId(relocatedIdStr);
            assert relocatedTaskId.isSet() : "relocated task ID is not set";
            return Optional.of(relocatedTaskId);
        }
        return Optional.empty();
    }

    public Optional<String> getRelocatedTaskId() {
        final List<String> relocatedTaskIds = this.getMetadata(RELOCATED_TASK_ID_METADATA_KEY);
        assert relocatedTaskIds == null || relocatedTaskIds.size() == 1 : "either not present or one value";
        return Optional.ofNullable(relocatedTaskIds).filter(taskIds -> taskIds.size() == 1).map(List::getFirst);
    }
}
