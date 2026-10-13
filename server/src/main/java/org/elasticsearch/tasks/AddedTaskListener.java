/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.tasks;

/**
 * Callback invoked on the registering thread immediately after a task is added to {@link TaskManager},
 * before the task's handler runs, with the caller's {@link org.elasticsearch.common.util.concurrent.ThreadContext}
 * still in place. Listeners may therefore read security headers and transients set by the caller.
 *
 * @see TaskManager#registerAddedTaskListener(AddedTaskListener)
 */
@FunctionalInterface
public interface AddedTaskListener {
    void onAdded(Task task);
}
