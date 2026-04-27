/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Level;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.TaskRelocatedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.util.Map;

import static org.mockito.Mockito.mock;

public class LoggingReindexTaskListenerTests extends ESTestCase {

    public void testOnResponseLogsAtInfo() {
        var task = randomTask();
        var listener = new LoggingReindexTaskListener(task);
        MockLog.assertThatLogger(
            () -> listener.onResponse(mock(BulkByScrollResponse.class)),
            LoggingReindexTaskListener.class,
            new MockLog.SeenEventExpectation("response logged", LoggingReindexTaskListener.class.getCanonicalName(), Level.INFO, "finished")
        );
    }

    public void testOnFailureWithTaskRelocatedExceptionLogsAtInfo() {
        var task = randomTask();
        var listener = new LoggingReindexTaskListener(task);
        var originalTaskId = randomRealTaskId();
        var relocatedTaskId = randomRealTaskId();
        var exception = new TaskRelocatedException(originalTaskId, relocatedTaskId);
        MockLog.assertThatLogger(
            () -> listener.onFailure(exception),
            LoggingReindexTaskListener.class,
            new MockLog.SeenEventExpectation(
                "relocation logged at info",
                LoggingReindexTaskListener.class.getCanonicalName(),
                Level.INFO,
                "was relocated to"
            ),
            new MockLog.UnseenEventExpectation(
                "relocation not logged at warn",
                LoggingReindexTaskListener.class.getCanonicalName(),
                Level.WARN,
                "was relocated to"
            )
        );
    }

    public void testOnFailureWithOtherExceptionLogsAtWarn() {
        var task = randomTask();
        var listener = new LoggingReindexTaskListener(task);
        var exception = new RuntimeException(randomAlphaOfLength(10));
        MockLog.assertThatLogger(
            () -> listener.onFailure(exception),
            LoggingReindexTaskListener.class,
            new MockLog.SeenEventExpectation(
                "failure logged at warn",
                LoggingReindexTaskListener.class.getCanonicalName(),
                Level.WARN,
                "failed with exception"
            )
        );
    }

    public void testRejectsNullTask() {
        expectThrows(NullPointerException.class, () -> new LoggingReindexTaskListener(null));
    }

    private static Task randomTask() {
        return new Task(
            randomNonNegativeLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
    }

    private static TaskId randomRealTaskId() {
        return new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());
    }
}
