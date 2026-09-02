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
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class TransportListTasksActionTests extends ESTestCase {

    // -- helpers --

    private static TaskId randomTaskId() {
        return new TaskId(randomAlphaOfLength(5), randomNonNegativeLong());
    }

    private static TaskInfo randomTaskInfo() {
        return randomTaskInfoWithTaskId(randomTaskId());
    }

    private static TaskInfo randomTaskInfoWithTaskId(final TaskId taskId) {
        return randomTaskInfoWithTaskIdAndOriginalAndRunningTime(taskId, taskId, randomNonNegativeLong());
    }

    private static TaskInfo randomTaskInfoWithRunningTimeNanos(final long runningTimeNanos) {
        return randomTaskInfoWithTaskIdAndRunningTimeNanos(randomTaskId(), runningTimeNanos);
    }

    private static TaskInfo randomTaskInfoWithTaskIdAndRunningTimeNanos(final TaskId taskId, final long runningTimeNanos) {
        return randomTaskInfoWithTaskIdAndOriginalAndRunningTime(taskId, taskId, runningTimeNanos);
    }

    private static TaskInfo randomTaskInfoWithTaskIdAndOriginalAndRunningTime(
        final TaskId taskId,
        final TaskId originalTaskId,
        final long runningTimeNanos
    ) {
        TaskId parentTaskId = randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId();
        return randomTaskInfoWithTaskIdOriginalRunningTimeAndParent(taskId, originalTaskId, runningTimeNanos, parentTaskId);
    }

    private static TaskInfo randomTaskInfoWithTaskIdOriginalRunningTimeAndParent(
        TaskId taskId,
        TaskId originalTaskId,
        long runningTimeNanos,
        TaskId parentTaskId
    ) {
        String action = randomAlphaOfLength(10);
        return randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(taskId, originalTaskId, runningTimeNanos, parentTaskId, action);
    }

    private static TaskInfo randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
        TaskId taskId,
        TaskId originalTaskId,
        long runningTimeNanos,
        TaskId parentTaskId,
        String action
    ) {
        final boolean cancellable = randomBoolean();
        return new TaskInfo(
            taskId,
            randomAlphaOfLength(10),
            taskId.getNodeId(),
            action,
            randomAlphaOfLength(10),
            null,
            randomNonNegativeLong(),
            runningTimeNanos,
            cancellable,
            cancellable && randomBoolean(),
            parentTaskId,
            Map.of(),
            originalTaskId,
            randomNonNegativeLong()
        );
    }

    private static TaskInfo randomTaskInfoWithParentId(final TaskId parentTaskId) {
        return randomTaskInfoWithTaskIdActionAndParent(randomTaskId(), randomAlphaOfLength(10), parentTaskId);
    }

    private static TaskInfo randomTaskInfoWithTaskIdActionAndParent(final TaskId taskId, final String action, final TaskId parentTaskId) {
        return randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(taskId, taskId, randomNonNegativeLong(), parentTaskId, action);
    }

    private static GetResponse buildTasksIndexResponse(String docId, TaskResult taskResult) {
        BytesReference source;
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)) {
            taskResult.toXContent(builder, org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS);
            source = BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new AssertionError("failed to serialize task result", e);
        }
        return new GetResponse(
            new GetResult(
                TaskResultsService.TASK_INDEX,
                docId,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                1,
                true,
                source,
                null,
                null
            )
        );
    }

    private static GetResponse notFoundGetResponse() {
        return new GetResponse(
            new GetResult(
                TaskResultsService.TASK_INDEX,
                randomAlphaOfLength(10),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                -1,
                false,
                null,
                null,
                null
            )
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
        final TaskInfo older = randomTaskInfoWithRunningTimeNanos(high);
        final TaskInfo newer = randomTaskInfoWithRunningTimeNanos(low);
        assertSame(newer, TransportListTasksAction.preferNewer(older, newer));
    }

    public void testPreferNewerReturnsExistingWhenEqual() {
        final long runtime = randomNonNegativeLong();
        final TaskInfo existing = randomTaskInfoWithRunningTimeNanos(runtime);
        final TaskInfo newEqual = randomTaskInfoWithRunningTimeNanos(runtime);
        assertSame(existing, TransportListTasksAction.preferNewer(existing, newEqual));
    }

    // -- deduplicateTasks --

    public void testDeduplicateTasksOnOriginalTaskIdNoDuplicates() {
        final List<TaskId> uniqueOriginIds = randomValueOtherThanMany(
            t -> t.size() != 4,
            () -> randomSet(4, 4, TransportListTasksActionTests::randomTaskId)
        ).stream().toList();
        // First pass has two non-child reindex tasks with unique origins. Both should both be included.
        final TaskInfo firstPass1 = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            uniqueOriginIds.get(0),
            randomNonNegativeLong(),
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        final TaskInfo firstPass2 = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            uniqueOriginIds.get(1),
            randomNonNegativeLong(),
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        // Second pass has two random tasks with unique origins. Both should be included.
        final TaskInfo secondPass1 = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            uniqueOriginIds.get(2),
            randomNonNegativeLong(),
            randomBoolean() ? randomTaskId() : TaskId.EMPTY_TASK_ID,
            randomAlphaOfLength(10)
        );
        final TaskInfo secondPass2 = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            uniqueOriginIds.get(3),
            randomNonNegativeLong(),
            randomBoolean() ? randomTaskId() : TaskId.EMPTY_TASK_ID,
            randomAlphaOfLength(10)
        );
        final Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasksOnOriginalTaskId(
            List.of(firstPass1, firstPass2),
            List.of(secondPass1, secondPass2)
        );
        Map<TaskId, TaskInfo> expectedResult = Map.of(
            secondPass1.originalTaskId(),
            secondPass1,
            secondPass2.originalTaskId(),
            secondPass2,
            firstPass1.originalTaskId(),
            firstPass1,
            firstPass2.originalTaskId(),
            firstPass2
        );
        assertEquals(expectedResult, result);
    }

    public void testDeduplicateTasksOnOriginalTaskIdIntraSecondPassCollision() {
        TaskId originalId = randomTaskId();
        long high = randomLongBetween(1, Long.MAX_VALUE);
        long low = randomLongBetween(0, high - 1);
        TaskInfo older = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(randomTaskId(), originalId, high);
        TaskInfo newer = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(randomTaskId(), originalId, low);

        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasksOnOriginalTaskId(List.of(), List.of(older, newer));
        Map<TaskId, TaskInfo> expectedResult = Map.of(originalId, newer);
        assertEquals(expectedResult, result);
    }

    public void testDeduplicateTasksOnOriginalTaskIdIntraFirstPassCollision() {
        TaskId originalId = randomTaskId();
        long high = randomLongBetween(1, Long.MAX_VALUE);
        long low = randomLongBetween(0, high - 1);
        TaskInfo older = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            originalId,
            high,
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        TaskInfo newer = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            originalId,
            low,
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );

        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasksOnOriginalTaskId(List.of(older, newer), List.of());
        Map<TaskId, TaskInfo> expectedResult = Map.of(originalId, newer);
        assertEquals(expectedResult, result);
    }

    public void testDeduplicateTasksOnOriginalTaskIdCrossListSecondPassWins() {
        TaskId originalId = randomTaskId();
        long highRuntime = randomLongBetween(1, Long.MAX_VALUE);
        long lowRuntime = randomLongBetween(0, highRuntime - 1);

        TaskInfo firstPassTask = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(randomTaskId(), originalId, lowRuntime);
        TaskInfo secondPassTask = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(randomTaskId(), originalId, highRuntime);

        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasksOnOriginalTaskId(
            List.of(firstPassTask),
            List.of(secondPassTask)
        );
        Map<TaskId, TaskInfo> expectedResult = Map.of(originalId, secondPassTask);
        assertEquals(expectedResult, result);
    }

    public void testDeduplicateTasksOnOriginalTaskIdFourWay() {
        TaskId sharedParent = randomTaskId();
        long a1 = randomLongBetween(0, 99);
        long a2 = randomLongBetween(100, 200);
        long b1 = randomLongBetween(0, 50);
        long b2 = randomLongBetween(300, 400);

        TaskInfo firstPassNewer = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(randomTaskId(), sharedParent, b1);
        TaskInfo firstPassOlder = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(randomTaskId(), sharedParent, b2);
        TaskInfo secondPassNewer = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(randomTaskId(), sharedParent, a1);
        TaskInfo secondPassOlder = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(randomTaskId(), sharedParent, a2);

        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasksOnOriginalTaskId(
            List.of(firstPassOlder, firstPassNewer),
            List.of(secondPassOlder, secondPassNewer)
        );
        Map<TaskId, TaskInfo> expectedResult = Map.of(sharedParent, secondPassNewer);
        assertEquals(expectedResult, result);
    }

    public void testDeduplicateTasksOnOriginalTaskIdMixedUniqueAndDuplicated() {
        TaskId sharedOriginId = randomTaskId();
        TaskId firstPassParentSharedOriginId = randomTaskId();
        TaskInfo firstPassParentSharedOrigin = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            firstPassParentSharedOriginId,
            sharedOriginId,
            randomNonNegativeLong(),
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        TaskId firstPassChildSharedOriginId = randomTaskId();
        TaskInfo firstPassChildSharedOrigin = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            firstPassChildSharedOriginId,
            firstPassChildSharedOriginId,
            randomNonNegativeLong(),
            firstPassParentSharedOriginId,
            ReindexAction.NAME
        );
        TaskId secondPassParentSharedOriginId = randomTaskId();
        TaskInfo secondPassParentSharedOrigin = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            secondPassParentSharedOriginId,
            sharedOriginId,
            randomNonNegativeLong(),
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        TaskId secondPassChildSharedOriginId = randomTaskId();
        TaskInfo secondPassChildSharedOrigin = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            secondPassChildSharedOriginId,
            secondPassChildSharedOriginId,
            randomNonNegativeLong(),
            secondPassParentSharedOriginId,
            ReindexAction.NAME
        );

        TaskId firstPassParentUniqueOriginId = randomTaskId();
        TaskInfo firstPassParentUniqueOrigin = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            firstPassParentUniqueOriginId,
            randomTaskId(),
            randomNonNegativeLong(),
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        TaskInfo firstPassChildUniqueOrigin = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            randomTaskId(),
            randomNonNegativeLong(),
            firstPassParentUniqueOriginId,
            ReindexAction.NAME
        );
        TaskId secondPassParentUniqueOriginId = randomTaskId();
        TaskInfo secondPassParentUniqueOrigin = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            secondPassParentUniqueOriginId,
            randomTaskId(),
            randomNonNegativeLong(),
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        TaskInfo secondPassChildUniqueOrigin = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            randomTaskId(),
            randomNonNegativeLong(),
            secondPassParentUniqueOriginId,
            ReindexAction.NAME
        );

        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasksOnOriginalTaskId(
            List.of(firstPassParentSharedOrigin, firstPassChildSharedOrigin, firstPassParentUniqueOrigin, firstPassChildUniqueOrigin),
            List.of(secondPassParentSharedOrigin, secondPassChildSharedOrigin, secondPassParentUniqueOrigin, secondPassChildUniqueOrigin)
        );

        Map<TaskId, TaskInfo> expectedResult = Map.of(
            sharedOriginId,
            secondPassParentSharedOrigin,
            secondPassChildSharedOriginId,
            secondPassChildSharedOrigin,
            firstPassParentUniqueOrigin.originalTaskId(),
            firstPassParentUniqueOrigin,
            firstPassChildUniqueOrigin.originalTaskId(),
            firstPassChildUniqueOrigin,
            secondPassParentUniqueOrigin.originalTaskId(),
            secondPassParentUniqueOrigin,
            secondPassChildUniqueOrigin.originalTaskId(),
            secondPassChildUniqueOrigin
        );
        assertEquals(expectedResult, result);
    }

    public void testDeduplicateTasksOnOriginalTaskIdEmptyLists() {
        Map<TaskId, TaskInfo> result = TransportListTasksAction.deduplicateTasksOnOriginalTaskId(List.of(), List.of());
        assertTrue(result.isEmpty());
    }

    // -- deduplicateTaskFailures --

    public void testDeduplicateTaskFailuresBasicDedup() {
        String nodeId = randomAlphaOfLength(5);
        long taskNum = randomNonNegativeLong();
        TaskOperationFailure firstPassFailure = taskFailureWithNodeAndTaskId(nodeId, taskNum);
        TaskOperationFailure secondPassFailure = taskFailureWithNodeAndTaskId(nodeId, taskNum);

        List<TaskOperationFailure> result = TransportListTasksAction.deduplicateTaskFailures(
            Map.of(),
            List.of(firstPassFailure),
            List.of(secondPassFailure)
        );
        assertThat(result, containsInAnyOrder(secondPassFailure));
    }

    public void testDeduplicateTaskFailuresExclusion() {
        TaskId originalId = randomTaskId();
        TaskInfo captured = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(randomTaskId(), originalId, randomNonNegativeLong());

        TaskOperationFailure excluded = taskFailureWithNodeAndTaskId(originalId.getNodeId(), originalId.getId());

        TaskOperationFailure unrelated = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());

        List<TaskOperationFailure> result = TransportListTasksAction.deduplicateTaskFailures(
            Map.of(originalId, captured),
            List.of(),
            List.of(excluded, unrelated)
        );
        assertThat(result, containsInAnyOrder(unrelated));
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
            List.of(),
            List.of(failureForRelocated)
        );
        assertThat(result, containsInAnyOrder(failureForRelocated));
    }

    public void testDeduplicateTaskFailuresEmptyTasksMap() {
        TaskOperationFailure f1 = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());
        TaskOperationFailure f2 = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());

        List<TaskOperationFailure> result = TransportListTasksAction.deduplicateTaskFailures(Map.of(), List.of(f2), List.of(f1));
        assertThat(result, containsInAnyOrder(f1, f2));
    }

    public void testDeduplicateTaskFailuresDisjoint() {
        TaskOperationFailure pf = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());
        TaskOperationFailure sf = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());

        List<TaskOperationFailure> result = TransportListTasksAction.deduplicateTaskFailures(Map.of(), List.of(sf), List.of(pf));
        assertThat(result, containsInAnyOrder(pf, sf));
    }

    // -- deduplicateNodeFailures --

    public void testDeduplicateNodeFailuresByNodeId() {
        String nodeId = randomAlphaOfLength(5);
        FailedNodeException firstPassFailedNode = nodeFailureWithNodeId(nodeId);
        FailedNodeException secondPassFailedNode = nodeFailureWithNodeId(nodeId);

        List<ElasticsearchException> result = TransportListTasksAction.deduplicateNodeFailures(
            List.of(firstPassFailedNode),
            List.of(secondPassFailedNode)
        );
        assertThat(result, containsInAnyOrder(secondPassFailedNode));
    }

    public void testDeduplicateNodeFailuresNonFNEByMessage() {
        String message = randomAlphaOfLength(10);
        ElasticsearchException firstPassException = new ElasticsearchException(message);
        ElasticsearchException secondPassException = new ElasticsearchException(message);

        List<ElasticsearchException> result = TransportListTasksAction.deduplicateNodeFailures(
            List.of(firstPassException),
            List.of(secondPassException)
        );
        assertThat(result, containsInAnyOrder(secondPassException));
    }

    public void testDeduplicateNodeFailuresMixedTypes() {
        String nodeId = randomAlphaOfLength(5);
        FailedNodeException fne = nodeFailureWithNodeId(nodeId);
        ElasticsearchException ex = new ElasticsearchException(randomAlphaOfLength(10));

        List<ElasticsearchException> result = TransportListTasksAction.deduplicateNodeFailures(List.of(), List.of(fne, ex));
        assertThat(result, containsInAnyOrder(fne, ex));
    }

    public void testDeduplicateNodeFailuresDisjoint() {
        FailedNodeException fne1 = nodeFailureWithNodeId(randomAlphaOfLength(5));
        FailedNodeException fne2 = nodeFailureWithNodeId(randomAlphaOfLength(5));
        ElasticsearchException ex1 = new ElasticsearchException(randomAlphaOfLength(10));
        ElasticsearchException ex2 = new ElasticsearchException(randomAlphaOfLength(10));

        List<ElasticsearchException> result = TransportListTasksAction.deduplicateNodeFailures(List.of(fne2, ex2), List.of(fne1, ex1));
        assertEquals(4, result.size());
    }

    // -- deduplicateAndMerge --

    public void testDeduplicateAndMerge() {
        TaskId sharedOriginId = randomTaskId();
        TaskInfo firstPassSharedOriginTask = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            sharedOriginId,
            randomNonNegativeLong(),
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        TaskInfo secondPassSharedOriginTask = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            sharedOriginId,
            randomNonNegativeLong(),
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        TaskInfo firstPassUniqueOriginTask = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            randomTaskId(),
            randomNonNegativeLong(),
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        ;
        TaskInfo secondPassUniqueOriginTask = randomTaskInfoWithTaskIdOriginalRunningTimeParentAndAction(
            randomTaskId(),
            randomTaskId(),
            randomNonNegativeLong(),
            TaskId.EMPTY_TASK_ID,
            ReindexAction.NAME
        );
        ;

        String sharedFailNodeId = randomAlphaOfLength(5);
        long sharedFailTaskNum = randomNonNegativeLong();
        TaskOperationFailure firstPassTaskFailure = taskFailureWithNodeAndTaskId(sharedFailNodeId, sharedFailTaskNum);
        TaskOperationFailure secondPassSharedTaskFailure = taskFailureWithNodeAndTaskId(sharedFailNodeId, sharedFailTaskNum);
        TaskOperationFailure secondPassUniqueTaskFailure = taskFailureWithNodeAndTaskId(randomAlphaOfLength(5), randomNonNegativeLong());

        String sharedNodeFailId = randomAlphaOfLength(5);
        FailedNodeException firstPassSharedNodeFailure = nodeFailureWithNodeId(sharedNodeFailId);
        FailedNodeException secondPassNodeFailure = nodeFailureWithNodeId(sharedNodeFailId);
        FailedNodeException firstPassUniqueNodeFailure = nodeFailureWithNodeId(randomAlphaOfLength(5));

        ListTasksResponse firstPass = new ListTasksResponse(
            List.of(firstPassSharedOriginTask, firstPassUniqueOriginTask),
            List.of(firstPassTaskFailure),
            List.of(firstPassSharedNodeFailure, firstPassUniqueNodeFailure)
        );
        ListTasksResponse secondPass = new ListTasksResponse(
            List.of(secondPassSharedOriginTask, secondPassUniqueOriginTask),
            List.of(secondPassSharedTaskFailure, secondPassUniqueTaskFailure),
            List.of(secondPassNodeFailure)
        );

        ListTasksResponse merged = TransportListTasksAction.deduplicateAndMerge(firstPass, secondPass);

        assertThat(
            merged.getTasks(),
            containsInAnyOrder(firstPassUniqueOriginTask, secondPassUniqueOriginTask, secondPassSharedOriginTask)
        );
        assertThat(merged.getTaskFailures(), containsInAnyOrder(secondPassSharedTaskFailure, secondPassUniqueTaskFailure));
        assertThat(merged.getNodeFailures(), containsInAnyOrder(secondPassNodeFailure, firstPassUniqueNodeFailure));
    }

    // -- copyWithoutWaitForCompletion --

    public void testCopyWithoutWaitForCompletion() {
        ListTasksRequest original = new ListTasksRequest();
        original.setActions(randomAlphaOfLength(5), randomAlphaOfLength(5));
        original.setNodes(randomAlphaOfLength(5), randomAlphaOfLength(5));
        original.setTargetTaskId(randomTaskId());
        original.setTargetParentTaskId(randomTaskId());
        original.setTimeout(randomTimeValue());
        original.setDetailed(randomBoolean());
        original.setWaitForCompletion(randomBoolean());
        original.setDescriptions(randomAlphaOfLength(5), randomAlphaOfLength(5));
        original.setParentTask(randomTaskId());

        ListTasksRequest copy = TransportListTasksAction.copyWithoutWaitForCompletion(original);

        assertArrayEquals(original.getActions(), copy.getActions());
        assertArrayEquals(original.getNodes(), copy.getNodes());
        assertEquals(original.getTargetTaskId(), copy.getTargetTaskId());
        assertEquals(original.getTargetParentTaskId(), copy.getTargetParentTaskId());
        assertEquals(original.getTimeout(), copy.getTimeout());
        assertEquals(original.getDetailed(), copy.getDetailed());
        assertFalse(copy.getWaitForCompletion());
        assertArrayEquals(original.getDescriptions(), copy.getDescriptions());
        assertEquals(original.getParentTask(), copy.getParentTask());
    }

    // -- parseTaskInfoFromIndexResponse --

    public void testParseTaskInfoFromIndexResponseValid() {
        TaskInfo original = randomTaskInfo();
        TaskResult taskResult = new TaskResult(true, original);
        GetResponse response = buildTasksIndexResponse(original.taskId().toString(), taskResult);

        Optional<TaskInfo> parsed = TransportListTasksAction.parseTaskInfoFromIndexResponse(xContentRegistry(), response);

        assertTrue(parsed.isPresent());
        assertEquals(original.taskId(), parsed.get().taskId());
        assertEquals(original.action(), parsed.get().action());
        assertEquals(original.startTime(), parsed.get().startTime());
        assertEquals(original.runningTimeNanos(), parsed.get().runningTimeNanos());
        assertEquals(original.cancellable(), parsed.get().cancellable());
        assertEquals(original.parentTaskId(), parsed.get().parentTaskId());
        assertEquals(original.originalTaskId(), parsed.get().originalTaskId());
    }

    public void testParseTaskInfoFromIndexResponseNotExists() {
        Optional<TaskInfo> parsed = TransportListTasksAction.parseTaskInfoFromIndexResponse(xContentRegistry(), notFoundGetResponse());
        assertTrue(parsed.isEmpty());
    }

    public void testParseTaskInfoFromIndexResponseMalformed() {
        GetResponse malformed = new GetResponse(
            new GetResult(
                TaskResultsService.TASK_INDEX,
                randomAlphaOfLength(10),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
                1,
                true,
                new BytesArray("{\"garbage\": true}"),
                null,
                null
            )
        );
        Optional<TaskInfo> parsed = TransportListTasksAction.parseTaskInfoFromIndexResponse(xContentRegistry(), malformed);
        assertTrue(parsed.isEmpty());
    }

    // -- findMissedRelocations --

    public void testFindMissedRelocationsNoMissing() {
        TaskInfo snapshotTask = randomTaskInfoWithTaskIdActionAndParent(randomTaskId(), ReindexAction.NAME, TaskId.EMPTY_TASK_ID);
        TaskInfo wfcTask = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(
            randomTaskId(),
            snapshotTask.originalTaskId(),
            randomNonNegativeLong()
        );

        TransportListTasksAction.MissedRelocations result = TransportListTasksAction.findMissedRelocations(
            List.of(snapshotTask),
            List.of(wfcTask)
        );
        assertTrue(result.parents().isEmpty());
        assertTrue(result.children().isEmpty());
    }

    public void testFindMissedRelocationsParentWithChildren() {
        TaskId parentId = randomTaskId();
        TaskInfo parent = randomTaskInfoWithTaskIdActionAndParent(parentId, ReindexAction.NAME, TaskId.EMPTY_TASK_ID);
        TaskInfo child1 = randomTaskInfoWithTaskIdActionAndParent(randomTaskId(), randomAlphaOfLength(10), parentId);
        TaskInfo child2 = randomTaskInfoWithTaskIdActionAndParent(randomTaskId(), randomAlphaOfLength(10), parentId);
        TaskInfo unrelatedTask = randomTaskInfoWithTaskIdActionAndParent(randomTaskId(), ReindexAction.NAME, TaskId.EMPTY_TASK_ID);

        TaskInfo wfcUnrelated = randomTaskInfoWithTaskIdAndOriginalAndRunningTime(
            randomTaskId(),
            unrelatedTask.originalTaskId(),
            randomNonNegativeLong()
        );

        TransportListTasksAction.MissedRelocations result = TransportListTasksAction.findMissedRelocations(
            List.of(parent, child1, child2, unrelatedTask),
            List.of(wfcUnrelated)
        );
        assertThat(result.parents(), containsInAnyOrder(parent));
        assertThat(result.children(), containsInAnyOrder(child1, child2));
    }

    public void testFindMissedRelocationsNonReindexIgnored() {
        TaskInfo nonReindexParent = randomTaskInfoWithTaskIdActionAndParent(randomTaskId(), randomAlphaOfLength(10), TaskId.EMPTY_TASK_ID);

        TransportListTasksAction.MissedRelocations result = TransportListTasksAction.findMissedRelocations(
            List.of(nonReindexParent),
            List.of()
        );
        assertTrue(result.parents().isEmpty());
        assertTrue(result.children().isEmpty());
    }

    public void testFindMissedRelocationsChildTaskIgnored() {
        TaskInfo childReindex = randomTaskInfoWithTaskIdActionAndParent(randomTaskId(), ReindexAction.NAME, randomTaskId());

        TransportListTasksAction.MissedRelocations result = TransportListTasksAction.findMissedRelocations(
            List.of(childReindex),
            List.of()
        );
        assertTrue(result.parents().isEmpty());
        assertTrue(result.children().isEmpty());
    }

    public void testFindMissedRelocationsMultipleParents() {
        TaskId parent1Id = randomTaskId();
        TaskId parent2Id = randomTaskId();
        TaskInfo parent1 = randomTaskInfoWithTaskIdActionAndParent(parent1Id, ReindexAction.NAME, TaskId.EMPTY_TASK_ID);
        TaskInfo parent2 = randomTaskInfoWithTaskIdActionAndParent(parent2Id, ReindexAction.NAME, TaskId.EMPTY_TASK_ID);
        TaskInfo child1 = randomTaskInfoWithTaskIdActionAndParent(randomTaskId(), randomAlphaOfLength(10), parent1Id);
        TaskInfo child2 = randomTaskInfoWithTaskIdActionAndParent(randomTaskId(), randomAlphaOfLength(10), parent2Id);

        TransportListTasksAction.MissedRelocations result = TransportListTasksAction.findMissedRelocations(
            List.of(parent1, parent2, child1, child2),
            List.of()
        );
        assertThat(result.parents(), containsInAnyOrder(parent1, parent2));
        assertThat(result.children(), containsInAnyOrder(child1, child2));
    }
}
