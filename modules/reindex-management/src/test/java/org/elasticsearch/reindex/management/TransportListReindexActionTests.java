/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class TransportListReindexActionTests extends ESTestCase {

    public void testRewriteNonRelocatedTaskIsNoop() {
        final TaskId taskId = new TaskId(randomAlphanumericOfLength(5), randomNonNegativeLong());
        final boolean cancellable = randomBoolean();
        final TaskInfo info = new TaskInfo(
            taskId,
            randomAlphanumericOfLength(10),
            taskId.getNodeId(),
            randomAlphanumericOfLength(10),
            randomAlphanumericOfLength(10),
            null,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            cancellable,
            cancellable && randomBoolean(),
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphanumericOfLength(5), randomNonNegativeLong()),
            Map.of()
        );

        final TaskInfo relocated = TransportListReindexAction.relocatedTaskInfo(info);
        assertThat("nothing is changed for non-relocated tasks", relocated, equalTo(info));
    }

    public void testRewriteRelocatedTask() {
        final TaskId originalId = new TaskId("originalNode", randomNonNegativeLong());
        final TaskId relocatedId = new TaskId("relocatedNode", randomNonNegativeLong());
        final long originalStartMillis = randomLongBetween(0, 100);
        final long relocatedStartMillis = randomLongBetween(originalStartMillis, originalStartMillis + randomLongBetween(0, 100));
        final long relocatedRunningNanos = randomNonNegativeLong();
        final boolean cancellable = randomBoolean();

        final TaskInfo info = new TaskInfo(
            relocatedId,
            randomAlphanumericOfLength(10),
            relocatedId.getNodeId(),
            randomAlphanumericOfLength(10),
            randomAlphanumericOfLength(10),
            null,
            relocatedStartMillis,
            relocatedRunningNanos,
            cancellable,
            cancellable && randomBoolean(),
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphanumericOfLength(5), randomNonNegativeLong()),
            Map.of(),
            originalId,
            originalStartMillis
        );

        final TaskInfo rewritten = TransportListReindexAction.relocatedTaskInfo(info);
        assertThat(rewritten.taskId(), equalTo(originalId));
        assertThat(rewritten.taskId(), equalTo(rewritten.originalTaskId()));
        assertThat(rewritten.node(), equalTo(originalId.getNodeId()));
        assertThat(rewritten.startTime(), equalTo(originalStartMillis));
        assertThat(rewritten.startTime(), equalTo(rewritten.originalStartTimeMillis()));
        long expectedRunningNanos = relocatedRunningNanos + TimeUnit.MILLISECONDS.toNanos(relocatedStartMillis - originalStartMillis);
        assertThat(rewritten.runningTimeNanos(), equalTo(expectedRunningNanos));
    }
}
