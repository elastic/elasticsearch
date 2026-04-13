/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction.deduplicateNodeFailures;
import static org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction.deduplicateTaskFailures;
import static org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction.deduplicateTasks;
import static org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction.requestCannotMatchRelocatableTasks;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class TransportListTasksActionTests extends ESTestCase {

    // -- deduplicateTasks --

    public void testDeduplicateTasksEmptyList() {
        assertThat(deduplicateTasks(List.of()), hasSize(0));
    }

    public void testDeduplicateTasksSingle() {
        final List<TaskInfo> tasks = List.of(randomTaskInfo(new TaskId("node1", randomNonNegativeLong())));
        assertThat(deduplicateTasks(tasks), sameInstance(tasks));
    }

    public void testDeduplicateTasksNoDuplicates() {
        final List<TaskInfo> tasks = List.of(
            randomTaskInfo(new TaskId("node1", randomNonNegativeLong())),
            randomTaskInfo(new TaskId("node2", randomNonNegativeLong())),
            randomTaskInfo(new TaskId("node3", randomNonNegativeLong()))
        );
        assertThat(deduplicateTasks(tasks), sameInstance(tasks));
    }

    public void testDeduplicateTasksDuplicatePairKeepsFirst() {
        final TaskId id = new TaskId("node1", randomNonNegativeLong());
        final TaskInfo first = randomTaskInfo(id);
        final TaskInfo second = randomTaskInfo(id);

        final List<TaskInfo> result = deduplicateTasks(List.of(first, second));
        assertThat(result, hasSize(1));
        assertThat(result.getFirst(), sameInstance(first));
    }

    public void testDeduplicateTasksMultipleGroups() {
        final TaskId id1 = new TaskId("node1", randomNonNegativeLong());
        final TaskId id2 = new TaskId("node2", randomNonNegativeLong());
        final TaskInfo id1First = randomTaskInfo(id1);
        final TaskInfo id1Second = randomTaskInfo(id1);
        final TaskInfo id2First = randomTaskInfo(id2);
        final TaskInfo id2Second = randomTaskInfo(id2);

        final List<TaskInfo> result = deduplicateTasks(List.of(id1First, id2First, id1Second, id2Second));
        assertThat(result, hasSize(2));
        assertThat(result.get(0), sameInstance(id1First));
        assertThat(result.get(1), sameInstance(id2First));
    }

    public void testDeduplicateTasksMixedUniqueAndDuplicated() {
        final TaskId dupId = new TaskId("node1", randomNonNegativeLong());
        final TaskId uniqueId = new TaskId("node2", randomNonNegativeLong());
        final TaskInfo dupFirst = randomTaskInfo(dupId);
        final TaskInfo dupSecond = randomTaskInfo(dupId);
        final TaskInfo unique = randomTaskInfo(uniqueId);

        final List<TaskInfo> result = deduplicateTasks(List.of(dupFirst, unique, dupSecond));
        assertThat(result, hasSize(2));
        assertThat(result.get(0), sameInstance(dupFirst));
        assertThat(result.get(1), sameInstance(unique));
    }

    // -- deduplicateTaskFailures --

    public void testDeduplicateTaskFailuresEmptyAndSingle() {
        assertThat(deduplicateTaskFailures(List.of()), hasSize(0));

        final List<TaskOperationFailure> single = List.of(
            new TaskOperationFailure(randomAlphaOfLength(5), randomNonNegativeLong(), new Exception(randomAlphaOfLength(8)))
        );
        assertThat(deduplicateTaskFailures(single), sameInstance(single));
    }

    public void testDeduplicateTaskFailuresNoDuplicates() {
        final List<TaskOperationFailure> failures = List.of(
            new TaskOperationFailure("node1", randomNonNegativeLong(), new Exception(randomAlphaOfLength(8))),
            new TaskOperationFailure("node2", randomNonNegativeLong(), new Exception(randomAlphaOfLength(8)))
        );
        assertThat(deduplicateTaskFailures(failures), sameInstance(failures));
    }

    public void testDeduplicateTaskFailuresDuplicateKeepsLast() {
        final String nodeId = randomAlphaOfLength(5);
        final long taskId = randomNonNegativeLong();
        final TaskOperationFailure first = new TaskOperationFailure(nodeId, taskId, new Exception("first"));
        final TaskOperationFailure second = new TaskOperationFailure(nodeId, taskId, new Exception("second"));

        final List<TaskOperationFailure> result = deduplicateTaskFailures(List.of(first, second));
        assertThat(result, hasSize(1));
        assertThat(result.getFirst(), sameInstance(second));
    }

    public void testDeduplicateTaskFailuresMixed() {
        final String nodeId = randomAlphaOfLength(5);
        final long dupTaskId = randomNonNegativeLong();
        final TaskOperationFailure dup1 = new TaskOperationFailure(nodeId, dupTaskId, new Exception("a"));
        final TaskOperationFailure dup2 = new TaskOperationFailure(nodeId, dupTaskId, new Exception("b"));
        final TaskOperationFailure unique = new TaskOperationFailure("other", randomNonNegativeLong(), new Exception("c"));

        final List<TaskOperationFailure> result = deduplicateTaskFailures(List.of(dup1, unique, dup2));
        assertThat(result, hasSize(2));
    }

    // -- deduplicateNodeFailures --

    public void testDeduplicateNodeFailuresEmptyAndSingle() {
        assertThat(deduplicateNodeFailures(List.of()), hasSize(0));

        final List<ElasticsearchException> single = List.of(new FailedNodeException(randomAlphaOfLength(5), randomAlphaOfLength(8), null));
        assertThat(deduplicateNodeFailures(single), sameInstance(single));
    }

    public void testDeduplicateNodeFailuresNoDuplicates() {
        final List<ElasticsearchException> failures = List.of(
            new FailedNodeException("node1", randomAlphaOfLength(8), null),
            new FailedNodeException("node2", randomAlphaOfLength(8), null)
        );
        assertThat(deduplicateNodeFailures(failures), sameInstance(failures));
    }

    public void testDeduplicateNodeFailuresDuplicateSameNodeKeepsLast() {
        final String nodeId = randomAlphaOfLength(5);
        final FailedNodeException first = new FailedNodeException(nodeId, "first", null);
        final FailedNodeException second = new FailedNodeException(nodeId, "second", null);

        final List<ElasticsearchException> result = deduplicateNodeFailures(List.of(first, second));
        assertThat(result, hasSize(1));
        assertThat(result.getFirst(), sameInstance(second));
    }

    public void testDeduplicateNodeFailuresNonFailedNodeExceptionDedupsByMessage() {
        final String msg = randomAlphaOfLength(10);
        final ElasticsearchException first = new ElasticsearchException(msg);
        final ElasticsearchException second = new ElasticsearchException(msg);

        final List<ElasticsearchException> result = deduplicateNodeFailures(List.of(first, second));
        assertThat(result, hasSize(1));
        assertThat(result.getFirst(), sameInstance(second));
    }

    public void testDeduplicateNodeFailuresMixedTypes() {
        final String nodeId = randomAlphaOfLength(5);
        final FailedNodeException fne1 = new FailedNodeException(nodeId, "msg1", null);
        final FailedNodeException fne2 = new FailedNodeException(nodeId, "msg2", null);
        final ElasticsearchException generic = new ElasticsearchException(randomAlphaOfLength(10));

        final List<ElasticsearchException> result = deduplicateNodeFailures(List.of(fne1, generic, fne2));
        assertThat(result, hasSize(2));
    }

    // -- requestCannotMatchRelocatableTasks --

    public void testRequestCannotMatchRelocatableTasksEmptyActions() {
        assertFalse(requestCannotMatchRelocatableTasks(new ListTasksRequest()));
        assertFalse(requestCannotMatchRelocatableTasks(new ListTasksRequest().setActions()));
    }

    public void testRequestCannotMatchRelocatableTasksExactMatch() {
        assertFalse(requestCannotMatchRelocatableTasks(new ListTasksRequest().setActions(ReindexAction.NAME)));
    }

    public void testRequestCannotMatchRelocatableTasksWildcardMatch() {
        assertFalse(requestCannotMatchRelocatableTasks(new ListTasksRequest().setActions("indices:data/write/*")));
        assertFalse(requestCannotMatchRelocatableTasks(new ListTasksRequest().setActions("*reindex*")));
    }

    public void testRequestCannotMatchRelocatableTasksUnrelated() {
        assertTrue(requestCannotMatchRelocatableTasks(new ListTasksRequest().setActions("indices:data/read/search")));
        assertTrue(requestCannotMatchRelocatableTasks(new ListTasksRequest().setActions("cluster:monitor/tasks/lists")));
    }

    public void testRequestCannotMatchRelocatableTasksMixedWithOneMatching() {
        assertFalse(requestCannotMatchRelocatableTasks(new ListTasksRequest().setActions("indices:data/read/search", ReindexAction.NAME)));
    }

    public void testRequestCannotMatchRelocatableTasksAllUnrelated() {
        assertTrue(
            requestCannotMatchRelocatableTasks(new ListTasksRequest().setActions("indices:data/read/search", "cluster:admin/something"))
        );
    }

    // utils

    private static TaskInfo randomTaskInfo(TaskId taskId) {
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
