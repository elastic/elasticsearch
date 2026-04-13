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
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class TransportListTasksActionTests extends ESTestCase {

    // -- helpers --

    private static TaskId randomTaskId() {
        return new TaskId(randomAlphaOfLength(5), randomNonNegativeLong());
    }

    private static TaskInfo randomTaskInfo() {
        return randomTaskInfoWithTaskId(randomTaskId());
    }

    private static TaskInfo randomTaskInfoWithTaskId(final TaskId taskId) {
        return randomTaskInfoWithTaskId(taskId, taskId, randomNonNegativeLong());
    }

    private static TaskInfo randomTaskInfoWithRunningTimeNanos(final long runningTimeNanos) {
        return randomTaskInfoWithTaskIdAndRunningTimeNanos(randomTaskId(), runningTimeNanos);
    }

    private static TaskInfo randomTaskInfoWithTaskIdAndRunningTimeNanos(final TaskId taskId, final long runningTimeNanos) {
        return randomTaskInfoWithTaskId(taskId, taskId, runningTimeNanos);
    }

    private static TaskInfo randomTaskInfoWithTaskId(final TaskId taskId, final TaskId originalTaskId, final long runningTimeNanos) {
        final boolean cancellable = randomBoolean();
        return new TaskInfo(
            taskId,
            randomAlphaOfLength(10),
            taskId.getNodeId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            null,
            randomNonNegativeLong(),
            runningTimeNanos,
            cancellable,
            cancellable && randomBoolean(),
            randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId(),
            Map.of(),
            originalTaskId,
            randomNonNegativeLong()
        );
    }

    private static TaskOperationFailure taskFailureWithNodeAndTaskId(String nodeId, long taskId) {
        return new TaskOperationFailure(nodeId, taskId, new IllegalStateException(randomAlphaOfLength(10)));
    }

    private static FailedNodeException nodeFailureWithNodeId(String nodeId) {
        return new FailedNodeException(nodeId, randomAlphaOfLength(10), new Exception());
    }

    // -- preferNewer --

    public void testPreferNewerReturnsTaskInfoWithLowerRuntime() {
        final long high = randomLongBetween(1, Long.MAX_VALUE);
        final long low = randomLongBetween(0, high - 1);
        final TaskInfo higher = randomTaskInfoWithRunningTimeNanos(high);
        final TaskInfo lower = randomTaskInfoWithRunningTimeNanos(low);
        assertSame(lower, TransportListTasksAction.preferNewer(higher, lower));
    }

    public void testPreferNewerReturnsExistingWhenEqual() {
        final long runtime = randomNonNegativeLong();
        final TaskInfo firstEqual = randomTaskInfoWithRunningTimeNanos(runtime);
        final TaskInfo secondEqual = randomTaskInfoWithRunningTimeNanos(runtime);
        assertSame(firstEqual, TransportListTasksAction.preferNewer(firstEqual, secondEqual));
    }

    // -- deduplicateTasks --

    public void testDeduplicateTasksNoDuplicates() {
        final TaskInfo a1 = randomTaskInfo();
        final TaskInfo a2 = randomTaskInfo();
        final TaskInfo b1 = randomTaskInfo();
        final TaskInfo b2 = randomTaskInfo();

        final Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasks(List.of(a1, a2), List.of(b1, b2));
        assertEquals(4, result.size());
        assertSame(a1, result.get(a1.originalTaskId()));
        assertSame(a2, result.get(a2.originalTaskId()));
        assertSame(b1, result.get(b1.originalTaskId()));
        assertSame(b2, result.get(b2.originalTaskId()));
    }

    public void testDeduplicateTasksIntraPrimaryCollision() {
        TaskId originalId = randomTaskId();
        long high = randomLongBetween(1, Long.MAX_VALUE);
        long low = randomLongBetween(0, high - 1);
        TaskInfo older = randomTaskInfoWithTaskId(randomTaskId(), originalId, high);
        TaskInfo newer = randomTaskInfoWithTaskId(randomTaskId(), originalId, low);

        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasks(List.of(older, newer), List.of());
        assertEquals(1, result.size());
        assertSame(newer, result.get(originalId));
    }

    public void testDeduplicateTasksIntraSecondaryCollision() {
        TaskId originalId = randomTaskId();
        long high = randomLongBetween(1, Long.MAX_VALUE);
        long low = randomLongBetween(0, high - 1);
        TaskInfo older = randomTaskInfoWithTaskId(randomTaskId(), originalId, high);
        TaskInfo newer = randomTaskInfoWithTaskId(randomTaskId(), originalId, low);

        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasks(List.of(), List.of(older, newer));
        assertEquals(1, result.size());
        assertSame(newer, result.get(originalId));
    }

    public void testDeduplicateTasksCrossListPrimaryWins() {
        TaskId originalId = randomTaskId();
        long highRuntime = randomLongBetween(1, Long.MAX_VALUE);
        long lowRuntime = randomLongBetween(0, highRuntime - 1);

        TaskInfo primaryTask = randomTaskInfoWithTaskId(randomTaskId(), originalId, highRuntime);
        TaskInfo secondaryTask = randomTaskInfoWithTaskId(randomTaskId(), originalId, lowRuntime);

        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasks(List.of(primaryTask), List.of(secondaryTask));
        assertEquals(1, result.size());
        assertSame(primaryTask, result.get(originalId));
    }

    public void testDeduplicateTasksFourWay() {
        TaskId originalId = randomTaskId();
        long r1 = randomLongBetween(100, 200);
        long r2 = randomLongBetween(0, 99);
        long r3 = randomLongBetween(300, 400);
        long r4 = randomLongBetween(0, 50);

        TaskInfo pOlder = randomTaskInfoWithTaskId(randomTaskId(), originalId, r1);
        TaskInfo pNewer = randomTaskInfoWithTaskId(randomTaskId(), originalId, r2);
        TaskInfo sOlder = randomTaskInfoWithTaskId(randomTaskId(), originalId, r3);
        TaskInfo sNewer = randomTaskInfoWithTaskId(randomTaskId(), originalId, r4);

        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasks(List.of(pOlder, pNewer), List.of(sOlder, sNewer));
        assertEquals(1, result.size());
        assertSame(pNewer, result.get(originalId));
    }

    public void testDeduplicateTasksMixedUniqueAndDuplicated() {
        TaskId sharedOriginal = randomTaskId();
        TaskInfo primaryShared = randomTaskInfoWithTaskId(randomTaskId(), sharedOriginal, randomNonNegativeLong());
        TaskInfo secondaryShared = randomTaskInfoWithTaskId(randomTaskId(), sharedOriginal, randomNonNegativeLong());

        TaskInfo primaryUnique = randomTaskInfoWithTaskId(randomTaskId());
        TaskInfo secondaryUnique = randomTaskInfoWithTaskId(randomTaskId());

        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasks(
            List.of(primaryShared, primaryUnique),
            List.of(secondaryShared, secondaryUnique)
        );
        assertEquals(3, result.size());
        assertSame(primaryShared, result.get(sharedOriginal));
        assertSame(primaryUnique, result.get(primaryUnique.originalTaskId()));
        assertSame(secondaryUnique, result.get(secondaryUnique.originalTaskId()));
    }

    public void testDeduplicateTasksEmptyLists() {
        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasks(List.of(), List.of());
        assertTrue(result.isEmpty());
    }

    // -- deduplicateTaskFailures --

    public void testDeduplicateTaskFailuresBasicDedup() {
        String nodeId = randomAlphaOfLength(5);
        long taskNum = randomNonNegativeLong();
        TaskOperationFailure primaryFailure = taskFailureWithNodeAndTaskId(nodeId, taskNum);
        TaskOperationFailure secondaryFailure = taskFailureWithNodeAndTaskId(nodeId, taskNum);

        List<TaskOperationFailure> result = TransportListTasksAction.deduplicateTaskFailures(
            Map.of(),
            List.of(primaryFailure),
            List.of(secondaryFailure)
        );
        assertEquals(1, result.size());
        assertSame(primaryFailure, result.get(0));
    }

    public void testDeduplicateTaskFailuresExclusion() {
        TaskId originalId = randomTaskId();
        TaskInfo captured = randomTaskInfoWithTaskId(randomTaskId(), originalId, randomNonNegativeLong());

        TaskOperationFailure excludable = taskFailureWithNodeAndTaskId(originalId.getNodeId(), originalId.getId());

        TaskOperationFailure unrelated = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());

        List<TaskOperationFailure> result = TransportListTasksAction.deduplicateTaskFailures(
            Map.of(originalId, captured),
            List.of(excludable, unrelated),
            List.of()
        );
        assertEquals(1, result.size());
        assertSame(unrelated, result.get(0));
    }

    public void testDeduplicateTaskFailuresGapCase() {
        TaskId originalId = randomTaskId();
        TaskInfo nonRelocated = randomTaskInfoWithTaskId(originalId);

        TaskId relocatedPhysicalId = randomTaskId();
        TaskOperationFailure failureForRelocated = taskFailureWithNodeAndTaskId(
            relocatedPhysicalId.getNodeId(),
            relocatedPhysicalId.getId()
        );

        List<TaskOperationFailure> result = TransportListTasksAction.deduplicateTaskFailures(
            Map.of(originalId, nonRelocated),
            List.of(failureForRelocated),
            List.of()
        );
        assertEquals(1, result.size());
        assertSame(failureForRelocated, result.get(0));
    }

    public void testDeduplicateTaskFailuresEmptyTasksMap() {
        TaskOperationFailure f1 = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());
        TaskOperationFailure f2 = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());

        List<TaskOperationFailure> result = TransportListTasksAction.deduplicateTaskFailures(Map.of(), List.of(f1), List.of(f2));
        assertEquals(2, result.size());
        assertSame(f1, result.get(0));
        assertSame(f2, result.get(1));
    }

    public void testDeduplicateTaskFailuresDisjoint() {
        TaskOperationFailure pf = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());
        TaskOperationFailure sf = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());

        List<TaskOperationFailure> result = TransportListTasksAction.deduplicateTaskFailures(Map.of(), List.of(pf), List.of(sf));
        assertEquals(2, result.size());
        assertTrue(result.contains(pf));
        assertTrue(result.contains(sf));
    }

    // -- deduplicateNodeFailures --

    public void testDeduplicateNodeFailuresByNodeId() {
        String nodeId = randomAlphaOfLength(5);
        FailedNodeException primaryFne = nodeFailureWithNodeId(nodeId);
        FailedNodeException secondaryFne = nodeFailureWithNodeId(nodeId);

        List<ElasticsearchException> result = TransportListTasksAction.deduplicateNodeFailures(List.of(primaryFne), List.of(secondaryFne));
        assertEquals(1, result.size());
        assertSame(primaryFne, result.get(0));
    }

    public void testDeduplicateNodeFailuresNonFNEByMessage() {
        String message = randomAlphaOfLength(10);
        ElasticsearchException primaryEx = new ElasticsearchException(message);
        ElasticsearchException secondaryEx = new ElasticsearchException(message);

        List<ElasticsearchException> result = TransportListTasksAction.deduplicateNodeFailures(List.of(primaryEx), List.of(secondaryEx));
        assertEquals(1, result.size());
        assertSame(primaryEx, result.get(0));
    }

    public void testDeduplicateNodeFailuresMixedTypes() {
        String nodeId = randomAlphaOfLength(5);
        FailedNodeException fne = nodeFailureWithNodeId(nodeId);
        ElasticsearchException ex = new ElasticsearchException(randomAlphaOfLength(10));

        List<ElasticsearchException> result = TransportListTasksAction.deduplicateNodeFailures(List.of(fne, ex), List.of());
        assertEquals(2, result.size());
        assertSame(fne, result.get(0));
        assertSame(ex, result.get(1));
    }

    public void testDeduplicateNodeFailuresDisjoint() {
        FailedNodeException fne1 = nodeFailureWithNodeId(randomAlphaOfLength(5));
        FailedNodeException fne2 = nodeFailureWithNodeId(randomAlphaOfLength(5));
        ElasticsearchException ex1 = new ElasticsearchException(randomAlphaOfLength(10));
        ElasticsearchException ex2 = new ElasticsearchException(randomAlphaOfLength(10));

        List<ElasticsearchException> result = TransportListTasksAction.deduplicateNodeFailures(List.of(fne1, ex1), List.of(fne2, ex2));
        assertEquals(4, result.size());
    }

    // -- deduplicateAndMerge --

    public void testDeduplicateAndMerge() {
        TaskId sharedOriginal = randomTaskId();
        TaskInfo primaryTask = randomTaskInfoWithTaskId(randomTaskId(), sharedOriginal, randomNonNegativeLong());
        TaskInfo secondaryTask = randomTaskInfoWithTaskId(randomTaskId(), sharedOriginal, randomNonNegativeLong());
        TaskInfo primaryOnly = randomTaskInfoWithTaskId(randomTaskId());
        TaskInfo secondaryOnly = randomTaskInfoWithTaskId(randomTaskId());

        String sharedFailNodeId = randomAlphaOfLength(5);
        long sharedFailTaskNum = randomNonNegativeLong();
        TaskOperationFailure pTaskFailure = taskFailureWithNodeAndTaskId(sharedFailNodeId, sharedFailTaskNum);
        TaskOperationFailure sTaskFailure = taskFailureWithNodeAndTaskId(sharedFailNodeId, sharedFailTaskNum);
        TaskOperationFailure pUniqueTaskFailure = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());

        String sharedNodeFailId = randomAlphaOfLength(5);
        FailedNodeException pNodeFailure = nodeFailureWithNodeId(sharedNodeFailId);
        FailedNodeException sNodeFailure = nodeFailureWithNodeId(sharedNodeFailId);
        FailedNodeException sUniqueNodeFailure = nodeFailureWithNodeId(randomAlphaOfLength(5));

        ListTasksResponse primary = new ListTasksResponse(
            List.of(primaryTask, primaryOnly),
            List.of(pTaskFailure, pUniqueTaskFailure),
            List.of(pNodeFailure)
        );
        ListTasksResponse secondary = new ListTasksResponse(
            List.of(secondaryTask, secondaryOnly),
            List.of(sTaskFailure),
            List.of(sNodeFailure, sUniqueNodeFailure)
        );

        ListTasksResponse merged = TransportListTasksAction.deduplicateAndMerge(primary, secondary);

        assertEquals(3, merged.getTasks().size());
        Map<TaskId, TaskInfo> mergedTasks = TransportListTasksAction.deduplicateTasks(primary.getTasks(), secondary.getTasks());
        for (TaskInfo t : merged.getTasks()) {
            assertSame(mergedTasks.get(t.originalTaskId()), t);
        }

        assertEquals(2, merged.getTaskFailures().size());
        assertSame(pTaskFailure, merged.getTaskFailures().get(0));
        assertSame(pUniqueTaskFailure, merged.getTaskFailures().get(1));

        assertEquals(2, merged.getNodeFailures().size());
        assertSame(pNodeFailure, merged.getNodeFailures().get(0));
        assertSame(sUniqueNodeFailure, merged.getNodeFailures().get(1));
    }
}
