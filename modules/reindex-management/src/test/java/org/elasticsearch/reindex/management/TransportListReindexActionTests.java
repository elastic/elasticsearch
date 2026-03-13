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

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class TransportListReindexActionTests extends ESTestCase {

    public void testDeduplicateEmptyList() {
        final List<TaskInfo> result = TransportListReindexAction.deduplicateTasks(List.of());
        assertThat(result, hasSize(0));
    }

    public void testDeduplicateSingleTask() {
        final List<TaskInfo> tasks = List.of(taskInfo(new TaskId("node1", randomNonNegativeLong())));
        final List<TaskInfo> result = TransportListReindexAction.deduplicateTasks(tasks);
        assertThat(result, sameInstance(tasks));
    }

    public void testNoDuplicatesPassesThrough() {
        final TaskInfo a = taskInfo(new TaskId("node1", randomNonNegativeLong()));
        final TaskInfo b = taskInfo(new TaskId("node2", randomNonNegativeLong()));
        final TaskInfo c = taskInfo(new TaskId("node3", randomNonNegativeLong()));
        final List<TaskInfo> tasks = List.of(a, b, c);

        final List<TaskInfo> result = TransportListReindexAction.deduplicateTasks(tasks);
        assertThat(result, sameInstance(tasks));
    }

    public void testDuplicatePairKeepsFirst() {
        final TaskId id = new TaskId("node1", randomNonNegativeLong());
        final TaskInfo first = taskInfo(id);
        final TaskInfo second = taskInfo(id);

        final List<TaskInfo> result = TransportListReindexAction.deduplicateTasks(List.of(first, second));
        assertThat(result, hasSize(1));
        assertThat(result.getFirst(), sameInstance(first));
    }

    public void testMultipleDuplicateGroups() {
        final TaskId id1 = new TaskId("node1", randomNonNegativeLong());
        final TaskId id2 = new TaskId("node2", randomNonNegativeLong());
        final TaskInfo id1First = taskInfo(id1);
        final TaskInfo id1Second = taskInfo(id1);
        final TaskInfo id2First = taskInfo(id2);
        final TaskInfo id2Second = taskInfo(id2);

        final List<TaskInfo> result = TransportListReindexAction.deduplicateTasks(List.of(id1First, id2First, id1Second, id2Second));
        assertThat(result, hasSize(2));
        assertThat(result.get(0), sameInstance(id1First));
        assertThat(result.get(1), sameInstance(id2First));
    }

    public void testDuplicatesWithMixOfUniqueAndDuplicated() {
        final TaskId dupId = new TaskId("node1", randomNonNegativeLong());
        final TaskId uniqueId = new TaskId("node2", randomNonNegativeLong());
        final TaskInfo dupFirst = taskInfo(dupId);
        final TaskInfo dupSecond = taskInfo(dupId);
        final TaskInfo unique = taskInfo(uniqueId);

        final List<TaskInfo> result = TransportListReindexAction.deduplicateTasks(List.of(dupFirst, unique, dupSecond));
        assertThat(result, hasSize(2));
        assertThat(result.get(0), sameInstance(dupFirst));
        assertThat(result.get(1), sameInstance(unique));
    }

    private static TaskInfo taskInfo(final TaskId taskId) {
        final boolean cancellable = randomBoolean();
        final boolean cancelled = cancellable && randomBoolean();
        return new TaskInfo(
            taskId,
            randomAlphanumericOfLength(10),
            taskId.getNodeId(),
            randomAlphanumericOfLength(10),
            randomAlphanumericOfLength(10),
            null,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            cancellable,
            cancelled,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
    }
}
