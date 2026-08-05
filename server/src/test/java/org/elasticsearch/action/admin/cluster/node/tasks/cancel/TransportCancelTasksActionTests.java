/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

public class TransportCancelTasksActionTests extends ESTestCase {

    public void testMergeTasksCombinesDistinctOriginalIds() {
        final TaskInfo first = taskInfo(randomTaskId(), randomTaskId(), randomNonNegativeLong());
        final TaskInfo second = taskInfo(randomTaskId(), randomTaskId(), randomNonNegativeLong());
        final Map<TaskId, TaskInfo> result = TransportCancelTasksAction.mergeTasksByOriginalTaskId(List.of(first), List.of(second));
        assertEquals(2, result.size());
        assertSame(first, result.get(first.originalTaskId()));
        assertSame(second, result.get(second.originalTaskId()));
    }

    public void testMergeTasksSecondPassWinsOnSameOriginalId() {
        final TaskId originalId = randomTaskId();
        final TaskInfo first = taskInfo(randomTaskId(), originalId, randomNonNegativeLong());
        final TaskInfo second = taskInfo(randomTaskId(), originalId, randomNonNegativeLong());
        final Map<TaskId, TaskInfo> result = TransportCancelTasksAction.mergeTasksByOriginalTaskId(List.of(first), List.of(second));
        assertEquals(1, result.size());
        assertSame(second, result.get(originalId));
    }

    public void testMergeTasksWithinPassPicksRelocatedSuccessor() {
        // Both physical tasks visible in pass 1 (relocation race observed simultaneously). preferNewer picks the lower-runtime one.
        final TaskId originalId = randomTaskId();
        final TaskInfo source = taskInfo(randomTaskId(), originalId, 1000L);
        final TaskInfo relocated = taskInfo(randomTaskId(), originalId, 100L);
        final Map<TaskId, TaskInfo> result = TransportCancelTasksAction.mergeTasksByOriginalTaskId(List.of(source, relocated), List.of());
        assertEquals(1, result.size());
        assertSame(relocated, result.get(originalId));
    }

    public void testMergeTaskFailuresSecondPassWinsAndExcludesCaptured() {
        // Source-side 409 must be dropped once the relocated successor was captured; unrelated failure passes through.
        final TaskId sourceTaskId = randomTaskId();
        final TaskInfo capturedRelocated = taskInfo(randomTaskId(), sourceTaskId, randomNonNegativeLong());
        final TaskOperationFailure conflictForSource = taskFailure(sourceTaskId.getNodeId(), sourceTaskId.getId());
        final TaskOperationFailure unrelated = taskFailure(randomAlphaOfLength(5), randomNonNegativeLong());

        final List<TaskOperationFailure> result = TransportCancelTasksAction.mergeTaskFailures(
            Map.of(sourceTaskId, capturedRelocated),
            List.of(conflictForSource),
            List.of(unrelated)
        );
        assertThat(result, contains(unrelated));
    }

    public void testMergeNodeFailuresSecondPassWins() {
        final String nodeId = randomAlphaOfLength(5);
        final FailedNodeException first = nodeFailure(nodeId);
        final FailedNodeException second = nodeFailure(nodeId);
        final List<ElasticsearchException> result = TransportCancelTasksAction.mergeNodeFailures(List.of(first), List.of(second));
        assertThat(result, contains(second));
    }

    public void testDropStaleResourceNotFoundDropsAnyRnfWhenCaptured() {
        // With a target set and at least one capture, every RNF-bearing failure is stale; non-RNF failures are kept.
        final TaskId target = randomTaskId();
        final FailedNodeException synth = syntheticNotFound(target);
        final FailedNodeException otherRnf = new FailedNodeException(
            randomAlphaOfLength(5),
            "msg",
            new ResourceNotFoundException("something else missing")
        );
        final FailedNodeException nonRnf = nodeFailure(randomAlphaOfLength(5));
        final Map<TaskId, TaskInfo> captured = Map.of(target, taskInfo(target, target, randomNonNegativeLong()));

        final List<ElasticsearchException> result = TransportCancelTasksAction.dropStaleResourceNotFound(
            List.of(synth, otherRnf, nonRnf),
            target,
            captured
        );
        assertThat(result, contains(nonRnf));
    }

    public void testDropStaleResourceNotFoundKeepsAllWhenNothingCaptured() {
        final TaskId target = randomTaskId();
        final FailedNodeException synth = syntheticNotFound(target);
        assertThat(TransportCancelTasksAction.dropStaleResourceNotFound(List.of(synth), target, Map.of()), contains(synth));
    }

    public void testDropStaleResourceNotFoundIgnoresUnsetTarget() {
        final TaskId target = randomTaskId();
        final FailedNodeException synth = syntheticNotFound(target);
        final Map<TaskId, TaskInfo> captured = Map.of(target, taskInfo(target, target, randomNonNegativeLong()));
        assertThat(TransportCancelTasksAction.dropStaleResourceNotFound(List.of(synth), TaskId.EMPTY_TASK_ID, captured), contains(synth));
    }

    public void testCopyWithoutWaitForCompletionPreservesFieldsButDropsWfc() {
        final CancelTasksRequest original = new CancelTasksRequest();
        original.setTargetTaskId(randomTaskId());
        original.setTargetParentTaskId(randomTaskId());
        original.setActions(randomAlphaOfLength(5), randomAlphaOfLength(5));
        original.setTimeout(randomTimeValue());
        original.setReason(randomAlphaOfLength(10));
        original.setWaitForCompletion(true);

        final CancelTasksRequest copy = TransportCancelTasksAction.copyWithoutWaitForCompletion(original);

        assertEquals(original.getTargetTaskId(), copy.getTargetTaskId());
        assertEquals(original.getTargetParentTaskId(), copy.getTargetParentTaskId());
        assertArrayEquals(original.getActions(), copy.getActions());
        assertEquals(original.getTimeout(), copy.getTimeout());
        assertEquals(original.getReason(), copy.getReason());
        assertFalse("waitForCompletion must be dropped", copy.waitForCompletion());
    }

    /// Pass 1 cancelled the task; pass 2 came back empty (task is gone). Caller must see the task, not a 404.
    public void testMergeRespFirstPassCancelledSecondPassEmpty() {
        final TaskId target = randomTaskId();
        final TaskInfo cancelled = taskInfo(target, target, randomNonNegativeLong());
        final ListTasksResponse firstPass = new ListTasksResponse(List.of(cancelled), List.of(), List.of());
        final ListTasksResponse secondPass = new ListTasksResponse(List.of(), List.of(), List.of(syntheticNotFound(target)));

        final ListTasksResponse merged = TransportCancelTasksAction.mergeResponses(firstPass, secondPass, target);
        assertThat(merged.getTasks(), contains(cancelled));
        assertThat(merged.getTaskFailures(), is(empty()));
        assertThat(merged.getNodeFailures(), is(empty()));
    }

    /// Pass 1 missed the task (e.g. destination not yet ready); pass 2 found it.
    public void testMergeRespFirstPassEmptySecondPassFound() {
        final TaskId target = randomTaskId();
        final TaskInfo cancelled = taskInfo(randomTaskId(), target, randomNonNegativeLong());
        final ListTasksResponse firstPass = new ListTasksResponse(List.of(), List.of(), List.of(syntheticNotFound(target)));
        final ListTasksResponse secondPass = new ListTasksResponse(List.of(cancelled), List.of(), List.of());

        final ListTasksResponse merged = TransportCancelTasksAction.mergeResponses(firstPass, secondPass, target);
        assertThat(merged.getTasks(), contains(cancelled));
        assertThat(merged.getNodeFailures(), is(empty()));
    }

    /// Both passes empty: real 404 must surface.
    public void testMergeRespBothEmptyKeeps404() {
        final TaskId target = randomTaskId();
        final FailedNodeException synth1 = syntheticNotFound(target);
        final FailedNodeException synth2 = syntheticNotFound(target);
        final ListTasksResponse firstPass = new ListTasksResponse(List.of(), List.of(), List.of(synth1));
        final ListTasksResponse secondPass = new ListTasksResponse(List.of(), List.of(), List.of(synth2));

        final ListTasksResponse merged = TransportCancelTasksAction.mergeResponses(firstPass, secondPass, target);
        assertThat(merged.getTasks(), is(empty()));
        assertThat("second pass wins on identical synthetic 404s", merged.getNodeFailures(), contains(synth2));
    }

    /// Same logical task observed pre- and post-relocation across passes; merge collapses to the relocated successor (pass 2).
    public void testMergeRespCollapsesRelocationCrossPass() {
        final TaskId originalId = randomTaskId();
        final TaskInfo source = taskInfo(originalId, originalId, 1000L);
        final TaskInfo relocated = taskInfo(randomTaskId(), originalId, 100L);
        final ListTasksResponse firstPass = new ListTasksResponse(List.of(source), List.of(), List.of());
        final ListTasksResponse secondPass = new ListTasksResponse(List.of(relocated), List.of(), List.of());

        final ListTasksResponse merged = TransportCancelTasksAction.mergeResponses(firstPass, secondPass, originalId);
        assertThat(merged.getTasks(), contains(relocated));
    }

    /// Genuine transport failures on unrelated nodes survive; the synthetic 404 on the target node does not.
    public void testMergeRespPreservesUnrelatedFailuresButDropsSynth404() {
        final TaskId target = randomTaskId();
        final TaskInfo cancelled = taskInfo(target, target, randomNonNegativeLong());
        final FailedNodeException unrelated1 = nodeFailure(randomAlphaOfLength(5));
        final FailedNodeException unrelated2 = nodeFailure(randomAlphaOfLength(6));
        final ListTasksResponse firstPass = new ListTasksResponse(List.of(cancelled), List.of(), List.of(unrelated1));
        final ListTasksResponse secondPass = new ListTasksResponse(List.of(), List.of(), List.of(unrelated2, syntheticNotFound(target)));

        final ListTasksResponse merged = TransportCancelTasksAction.mergeResponses(firstPass, secondPass, target);
        assertThat(merged.getTasks(), contains(cancelled));
        assertThat(merged.getNodeFailures(), containsInAnyOrder(unrelated1, unrelated2));
    }

    private static TaskId randomTaskId() {
        return new TaskId(randomAlphaOfLength(5), randomNonNegativeLong());
    }

    private static TaskInfo taskInfo(final TaskId taskId, final TaskId originalTaskId, final long runningTimeNanos) {
        return new TaskInfo(
            taskId,
            randomAlphaOfLength(5),
            taskId.getNodeId(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            null,
            randomNonNegativeLong(),
            runningTimeNanos,
            true,
            true,
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            originalTaskId,
            randomNonNegativeLong()
        );
    }

    private static FailedNodeException syntheticNotFound(final TaskId target) {
        return new FailedNodeException(
            target.getNodeId(),
            "Failed node [" + target.getNodeId() + "]",
            new ResourceNotFoundException("task [{}] is not found", target)
        );
    }

    private static FailedNodeException nodeFailure(final String nodeId) {
        return new FailedNodeException(nodeId, randomAlphaOfLength(10), new RuntimeException("boom"));
    }

    private static TaskOperationFailure taskFailure(final String nodeId, final long taskId) {
        return new TaskOperationFailure(nodeId, taskId, new IllegalStateException(randomAlphaOfLength(5)));
    }
}
