/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

/**
 * Listener for Task success or failure.
 */
public interface TaskListener<Response> {
    /**
     * Handle task response. This response may constitute a failure or a success
     * but it is up to the listener to make that decision.
     *
     * @param task
     *            the task being executed. May be null if the action doesn't
     *            create a task
     * @param response
     *            the response from the action that executed the task
     */
    void onResponse(Task task, Response response);

    /**
     * A failure caused by an exception at some phase of the task.
     *
     * @param task
     *            the task being executed. May be null if the action doesn't
     *            create a task
     * @param e
     *            the failure
     */
    void onFailure(Task task, Exception e);

}
