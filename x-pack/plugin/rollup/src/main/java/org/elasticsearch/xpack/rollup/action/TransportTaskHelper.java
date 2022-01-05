/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.action;

import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.xpack.rollup.job.RollupJobTask;

import java.util.function.Consumer;

public class TransportTaskHelper {
    /**
     * Helper method used by Start/Stop TransportActions so that we can ensure only one task is invoked,
     * or none at all.  Should not end up in a situation where there are multiple tasks with the same
     * ID... but if we do, this will help prevent the situation from getting worse.
     */
    static void doProcessTasks(String id, Consumer<RollupJobTask> operation, TaskManager taskManager) {
        RollupJobTask matchingTask = null;
        for (Task task : taskManager.getTasks().values()) {
            if (task instanceof RollupJobTask rollupJobTask && rollupJobTask.getConfig().getId().equals(id)) {
                if (matchingTask != null) {
                    throw new IllegalArgumentException(
                        "Found more than one matching task for rollup job [" + id + "] when " + "there should only be one."
                    );
                }
                matchingTask = (RollupJobTask) task;
            }
        }

        if (matchingTask != null) {
            operation.accept(matchingTask);
        }
    }
}
