/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.tasks;

import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;

/**
 * Listener for task registration/unregistration
 */
public interface MockTaskManagerListener {
    void onTaskRegistered(Task task);

    void onTaskUnregistered(Task task);

    void waitForTaskCompletion(Task task);

    default void subscribeForRemovedTasks(RemovedTaskListener removedTaskListener) {}
}
