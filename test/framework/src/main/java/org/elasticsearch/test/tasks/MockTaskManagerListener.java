/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.tasks;

import org.elasticsearch.tasks.RemovedTaskListener;
import org.elasticsearch.tasks.Task;

/**
 * Listener for task registration/unregistration
 */
public interface MockTaskManagerListener {
    default void onTaskRegistered(Task task) {};

    default void onTaskUnregistered(Task task) {};

    default void onRemovedTaskListenerRegistered(RemovedTaskListener removedTaskListener) {};
}
