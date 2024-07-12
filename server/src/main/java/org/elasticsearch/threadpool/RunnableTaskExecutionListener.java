
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.threadpool;

/**
 * Listener for events when a runnable execution starts or finishes on a thread and is aware of the task for which the
 * runnable is associated to.
 */
public interface RunnableTaskExecutionListener {

    /**
     * Sends an update when ever a task's execution start on a thread
     *
     * @param taskId of task which has started
     * @param threadId of thread which is executing the task
     */
    void taskExecutionStartedOnThread(long taskId, long threadId);

    /**
     *
     * Sends an update when task execution finishes on a thread
     *
     * @param taskId of task which has finished
     * @param threadId of thread which executed the task
     */
    void taskExecutionFinishedOnThread(long taskId, long threadId);
}
