/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Math.min;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class BulkByPaginatedSearchTaskTests extends ESTestCase {

    /**
     * Creates a minimal {@link BulkByPaginatedSearchTask} with random id, type, action, description and optional relocation eligibility.
     * The task is neither a leader nor a worker until {@link BulkByPaginatedSearchTask#setWorkerCount(int, float)} or
     * {@link BulkByPaginatedSearchTask#setWorker(float, Integer)} is called.
     */
    private static BulkByPaginatedSearchTask createTask(boolean eligibleForRelocationOnShutdown) {
        return createTask(eligibleForRelocationOnShutdown, randomBoolean());
    }

    private static BulkByPaginatedSearchTask createTask(boolean eligibleForRelocationOnShutdown, boolean isRelocated) {
        return createTask(eligibleForRelocationOnShutdown, isRelocated, randomTaskId());
    }

    private static BulkByPaginatedSearchTask createTask(boolean eligibleForRelocationOnShutdown, boolean isRelocated, TaskId parentTaskId) {
        TaskId taskId = randomTaskId();
        String type = randomAlphaOfLengthBetween(1, 10);
        String action = randomAlphaOfLengthBetween(1, 10);
        String description = randomAlphaOfLengthBetween(0, 20);
        Map<String, String> headers = randomBoolean() ? Collections.emptyMap() : Map.of("header", randomAlphaOfLength(5));
        ResumeInfo.RelocationOrigin origin = isRelocated
            ? new ResumeInfo.RelocationOrigin(new TaskId(randomAlphaOfLength(5), randomNonNegativeLong()), randomNonNegativeLong())
            : null;
        return new BulkByPaginatedSearchTask(
            taskId,
            type,
            action,
            description,
            parentTaskId,
            headers,
            eligibleForRelocationOnShutdown,
            origin
        );
    }

    public void testStatusHatesNegatives() {
        checkStatusNegatives(-1, 0, 0, 0, 0, 0, 0, 0, 0, 0, "sliceId");
        checkStatusNegatives(null, -1, 0, 0, 0, 0, 0, 0, 0, 0, "total");
        checkStatusNegatives(null, 0, -1, 0, 0, 0, 0, 0, 0, 0, "updated");
        checkStatusNegatives(null, 0, 0, -1, 0, 0, 0, 0, 0, 0, "created");
        checkStatusNegatives(null, 0, 0, 0, -1, 0, 0, 0, 0, 0, "deleted");
        checkStatusNegatives(null, 0, 0, 0, 0, -1, 0, 0, 0, 0, "batches");
        checkStatusNegatives(null, 0, 0, 0, 0, 0, -1, 0, 0, 0, "versionConflicts");
        checkStatusNegatives(null, 0, 0, 0, 0, 0, 0, -1, 0, 0, "noops");
        checkStatusNegatives(null, 0, 0, 0, 0, 0, 0, 0, -1, 0, "bulkRetries");
        checkStatusNegatives(null, 0, 0, 0, 0, 0, 0, 0, 0, -1, "searchRetries");
    }

    /**
     * Build a task status with only some values. Used for testing negative values.
     */
    private void checkStatusNegatives(
        Integer sliceId,
        long total,
        long updated,
        long created,
        long deleted,
        int batches,
        long versionConflicts,
        long noops,
        long bulkRetries,
        long searchRetries,
        String fieldName
    ) {
        TimeValue throttle = randomPositiveTimeValue();
        TimeValue throttledUntil = randomPositiveTimeValue();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new BulkByPaginatedSearchTask.Status(
                sliceId,
                total,
                updated,
                created,
                deleted,
                batches,
                versionConflicts,
                noops,
                bulkRetries,
                searchRetries,
                throttle,
                0f,
                null,
                throttledUntil
            )
        );
        assertEquals(e.getMessage(), fieldName + " must be greater than 0 but was [-1]");
    }

    public void testXContentRepresentationOfUnlimitedRequestsPerSecond() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        BulkByPaginatedSearchTask.Status status = new BulkByPaginatedSearchTask.Status(
            null,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            timeValueMillis(0),
            Float.POSITIVE_INFINITY,
            null,
            timeValueMillis(0)
        );
        status.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), containsString("\"requests_per_second\":-1"));
    }

    public void testXContentRepresentationOfUnfinishedSlices() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        BulkByPaginatedSearchTask.Status completedStatus = new BulkByPaginatedSearchTask.Status(
            2,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            timeValueMillis(0),
            Float.POSITIVE_INFINITY,
            null,
            timeValueMillis(0)
        );
        BulkByPaginatedSearchTask.Status status = new BulkByPaginatedSearchTask.Status(
            Arrays.asList(null, null, new BulkByPaginatedSearchTask.StatusOrException(completedStatus)),
            null,
            0f
        );
        status.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), containsString("\"slices\":[null,null,{\"slice_id\":2"));
    }

    public void testXContentRepresentationOfSliceFailures() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        Exception e = new Exception();
        BulkByPaginatedSearchTask.Status status = new BulkByPaginatedSearchTask.Status(
            Arrays.asList(null, null, new BulkByPaginatedSearchTask.StatusOrException(e)),
            null,
            0f
        );
        status.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), containsString("\"slices\":[null,null,{\"type\":\"exception\""));
    }

    public void testMergeStatuses() {
        BulkByPaginatedSearchTask.StatusOrException[] statuses = new BulkByPaginatedSearchTask.StatusOrException[between(2, 100)];
        boolean containsNullStatuses = randomBoolean();
        int mergedTotal = 0;
        int mergedUpdated = 0;
        int mergedCreated = 0;
        int mergedDeleted = 0;
        int mergedBatches = 0;
        int mergedVersionConflicts = 0;
        int mergedNoops = 0;
        int mergedBulkRetries = 0;
        int mergedSearchRetries = 0;
        TimeValue mergedThrottled = timeValueNanos(0);
        float mergedRequestsPerSecond = 0;
        TimeValue mergedThrottledUntil = timeValueNanos(Integer.MAX_VALUE);
        for (int i = 0; i < statuses.length; i++) {
            if (containsNullStatuses && rarely()) {
                continue;
            }
            int total = between(0, 10000);
            int updated = between(0, total);
            int created = between(0, total - updated);
            int deleted = between(0, total - updated - created);
            int batches = between(0, 10);
            int versionConflicts = between(0, 100);
            int noops = total - updated - created - deleted;
            int bulkRetries = between(0, 100);
            int searchRetries = between(0, 100);
            TimeValue throttled = timeValueNanos(between(0, 10000));
            float requestsPerSecond = randomValueOtherThanMany(r -> r <= 0, () -> randomFloat());
            String reasonCancelled = randomBoolean() ? null : "test";
            TimeValue throttledUntil = timeValueNanos(between(0, 1000));
            statuses[i] = new BulkByPaginatedSearchTask.StatusOrException(
                new BulkByPaginatedSearchTask.Status(
                    i,
                    total,
                    updated,
                    created,
                    deleted,
                    batches,
                    versionConflicts,
                    noops,
                    bulkRetries,
                    searchRetries,
                    throttled,
                    requestsPerSecond,
                    reasonCancelled,
                    throttledUntil
                )
            );
            mergedTotal += total;
            mergedUpdated += updated;
            mergedCreated += created;
            mergedDeleted += deleted;
            mergedBatches += batches;
            mergedVersionConflicts += versionConflicts;
            mergedNoops += noops;
            mergedBulkRetries += bulkRetries;
            mergedSearchRetries += searchRetries;
            mergedThrottled = timeValueNanos(mergedThrottled.nanos() + throttled.nanos());
            mergedRequestsPerSecond += requestsPerSecond;
            mergedThrottledUntil = timeValueNanos(min(mergedThrottledUntil.nanos(), throttledUntil.nanos()));
        }
        String reasonCancelled = randomBoolean() ? randomAlphaOfLength(10) : null;
        BulkByPaginatedSearchTask.Status merged = new BulkByPaginatedSearchTask.Status(
            Arrays.asList(statuses),
            reasonCancelled,
            mergedRequestsPerSecond
        );
        assertEquals(mergedTotal, merged.getTotal());
        assertEquals(mergedUpdated, merged.getUpdated());
        assertEquals(mergedCreated, merged.getCreated());
        assertEquals(mergedDeleted, merged.getDeleted());
        assertEquals(mergedBatches, merged.getBatches());
        assertEquals(mergedVersionConflicts, merged.getVersionConflicts());
        assertEquals(mergedNoops, merged.getNoops());
        assertEquals(mergedBulkRetries, merged.getBulkRetries());
        assertEquals(mergedSearchRetries, merged.getSearchRetries());
        assertEquals(mergedThrottled, merged.getThrottled());
        assertEquals(mergedRequestsPerSecond, merged.getRequestsPerSecond(), 0.0001f);
        assertEquals(mergedThrottledUntil, merged.getThrottledUntil());
        assertEquals(reasonCancelled, merged.getReasonCancelled());
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#getStatus()} returns an empty status (merged from empty slice list)
     * when the task is neither a leader nor a worker.
     */
    public void testGetStatusReturnsEmptyStatusWhenNeitherLeaderNorWorker() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        assertFalse(task.isLeader());
        assertFalse(task.isWorker());
        BulkByPaginatedSearchTask.Status status = task.getStatus();
        assertEquals(0, status.getTotal());
        assertEquals(0, status.getUpdated());
        assertEquals(0, status.getCreated());
        assertEquals(0, status.getDeleted());
        assertEquals(0, status.getBatches());
        assertTrue(status.getSliceStatuses().isEmpty());
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#isLeader()} returns false for a freshly created task and true after
     * {@link BulkByPaginatedSearchTask#setWorkerCount(int, float)} is called.
     */
    public void testIsLeader() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        assertFalse(task.isLeader());
        int slices = between(2, 20);
        task.setWorkerCount(slices, Float.POSITIVE_INFINITY);
        assertTrue(task.isLeader());
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#isWorker()} returns false for a freshly created task and true after
     * {@link BulkByPaginatedSearchTask#setWorker(float, Integer)} is called.
     */
    public void testIsWorker() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        assertFalse(task.isWorker());
        float requestsPerSecond = randomFloatBetween(0.1f, 1000f);
        Integer sliceId = randomBoolean() ? null : between(0, 10);
        task.setWorker(requestsPerSecond, sliceId);
        assertTrue(task.isWorker());
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#setWorkerCount(int, float)} throws when the task is already a leader.
     */
    public void testSetWorkerCountThrowsWhenAlreadyLeader() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        task.setWorkerCount(between(2, 10), Float.POSITIVE_INFINITY);
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> task.setWorkerCount(between(2, 10), Float.POSITIVE_INFINITY)
        );
        assertThat(exception.getMessage(), containsString("already a leader"));
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#setWorkerCount(int, float)} throws when the task is already a worker.
     */
    public void testSetWorkerCountThrowsWhenAlreadyWorker() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        task.setWorker(randomFloatBetween(0.1f, 100f), null);
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> task.setWorkerCount(between(2, 10), Float.POSITIVE_INFINITY)
        );
        assertThat(exception.getMessage(), containsString("already a worker"));
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#setWorker(float, Integer)} throws when the task is already a worker.
     */
    public void testSetWorkerThrowsWhenAlreadyWorker() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        task.setWorker(randomFloatBetween(0.1f, 100f), null);
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> task.setWorker(randomFloatBetween(0.1f, 100f), between(0, 5))
        );
        assertThat(exception.getMessage(), containsString("already a worker"));
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#setWorker(float, Integer)} throws when the task is already a leader.
     */
    public void testSetWorkerThrowsWhenAlreadyLeader() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        task.setWorkerCount(between(2, 10), Float.POSITIVE_INFINITY);
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> task.setWorker(randomFloatBetween(0.1f, 100f), between(0, 5))
        );
        assertThat(exception.getMessage(), containsString("already a leader"));
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#getLeaderState()} returns the leader state after
     * {@link BulkByPaginatedSearchTask#setWorkerCount(int, float)} and throws when the task is not a leader.
     */
    public void testGetLeaderState() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> task.getLeaderState());
        assertThat(exception.getMessage(), containsString("not set to be a leader"));

        int slices = between(2, 20);
        task.setWorkerCount(slices, Float.POSITIVE_INFINITY);
        LeaderBulkByScrollTaskState leaderState = task.getLeaderState();
        assertNotNull(leaderState);
        assertEquals(slices, leaderState.getSlices());
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#getWorkerState()} returns the worker state after
     * {@link BulkByPaginatedSearchTask#setWorker(float, Integer)} and throws when the task is not a worker.
     */
    public void testGetWorkerState() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> task.getWorkerState());
        assertThat(exception.getMessage(), containsString("not set to be a worker"));

        float requestsPerSecond = randomFloatBetween(0.1f, 100f);
        Integer sliceId = randomBoolean() ? null : between(0, 10);
        task.setWorker(requestsPerSecond, sliceId);
        WorkerBulkByScrollTaskState workerState = task.getWorkerState();
        assertNotNull(workerState);
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#onCancelled()} does not throw when the task is a worker
     * (it delegates to the worker state's handleCancel).
     */
    public void testOnCancelledWhenWorkerDoesNotThrow() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        task.setWorker(randomFloatBetween(0.1f, 100f), null);
        task.onCancelled();
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#onCancelled()} does not throw when the task is neither leader nor worker.
     */
    public void testOnCancelledWhenNeitherLeaderNorWorkerDoesNotThrow() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        task.onCancelled();
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#isEligibleForRelocationOnShutdown()} returns the value passed to the constructor.
     */
    public void testIsEligibleForRelocationOnShutdown() {
        boolean eligible = randomBoolean();
        BulkByPaginatedSearchTask task = createTask(eligible);
        assertEquals(eligible, task.isEligibleForRelocationOnShutdown());
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#requestRelocation()} sets the relocation-requested flag when the task
     * is eligible for relocation, and that {@link BulkByPaginatedSearchTask#isRelocationRequested()} reflects it.
     */
    public void testRequestRelocationWhenEligible() {
        BulkByPaginatedSearchTask task = createTask(true);
        assertFalse(task.isRelocationRequested());
        task.requestRelocation();
        assertTrue(task.isRelocationRequested());
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#requestRelocation()} throws when the task is not eligible for relocation.
     */
    public void testRequestRelocationThrowsWhenNotEligible() {
        BulkByPaginatedSearchTask task = createTask(false);
        IllegalStateException exception = expectThrows(IllegalStateException.class, task::requestRelocation);
        assertThat(exception.getMessage(), containsString("eligibleForRelocationOnShutdown is false"));
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#taskInfoGivenSubtaskInfo(String, List)} builds a combined
     * {@link TaskInfo} from the given slice task infos when the task is a leader.
     */
    public void testTaskInfoGivenSubtaskInfo() {
        int slices = between(2, 8);
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        task.setWorkerCount(slices, Float.POSITIVE_INFINITY);

        String localNodeId = randomAlphaOfLength(5);
        List<TaskInfo> sliceInfoList = Arrays.asList(new TaskInfo[slices]);
        for (int sliceIndex = 0; sliceIndex < slices; sliceIndex++) {
            BulkByPaginatedSearchTask.Status sliceStatus = new BulkByPaginatedSearchTask.Status(
                sliceIndex,
                between(0, 100),
                between(0, 50),
                between(0, 50),
                between(0, 50),
                between(0, 10),
                between(0, 10),
                between(0, 10),
                between(0, 10),
                between(0, 10),
                timeValueMillis(0),
                randomFloatBetween(0.1f, 100f),
                randomBoolean() ? null : randomAlphaOfLength(5),
                timeValueMillis(0)
            );
            TaskId sliceTaskId = new TaskId(localNodeId, randomLong());
            TaskInfo sliceTaskInfo = new TaskInfo(
                sliceTaskId,
                task.getType(),
                localNodeId,
                task.getAction(),
                task.getDescription(),
                sliceStatus,
                randomLong(),
                randomLong(),
                true,
                false,
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap()
            );
            sliceInfoList.set(sliceIndex, sliceTaskInfo);
        }

        TaskInfo combinedTaskInfo = task.taskInfoGivenSubtaskInfo(localNodeId, sliceInfoList);
        assertNotNull(combinedTaskInfo);
        assertEquals(localNodeId, combinedTaskInfo.node());
        assertNotNull(combinedTaskInfo.status());
        assertTrue(combinedTaskInfo.status() instanceof BulkByPaginatedSearchTask.Status);
    }

    /**
     * Verifies that {@link BulkByPaginatedSearchTask#taskInfoGivenSubtaskInfo(String, List)} throws when the task is not a leader.
     */
    public void testTaskInfoGivenSubtaskInfoThrowsWhenNotLeader() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        String localNodeId = randomAlphaOfLength(5);
        List<TaskInfo> sliceInfoList = Collections.emptyList();
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> task.taskInfoGivenSubtaskInfo(localNodeId, sliceInfoList)
        );
        assertThat(exception.getMessage(), containsString("not set to be a leader"));
    }

    public void testTaskIsRelocatedIfRelocationOriginIsSet() {
        final boolean isRelocated = randomBoolean();
        final BulkByPaginatedSearchTask task = createTask(randomBoolean(), isRelocated);
        assertThat(task.isRelocatedTask(), equalTo(isRelocated));
    }

    public void testTaskInfo_notRelocated() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean(), false);
        String localNodeId = randomAlphaOfLength(5);
        TaskInfo info = task.taskInfo(localNodeId, true);
        assertThat(info.originalTaskId(), equalTo(new TaskId(localNodeId, task.getId())));
        assertThat(info.originalStartTimeMillis(), equalTo(task.getStartTime()));
    }

    public void testTaskInfo_relocatedParent() {
        BulkByPaginatedSearchTask task = createTask(true, true, TaskId.EMPTY_TASK_ID);
        String localNodeId = randomAlphaOfLength(5);
        TaskInfo info = task.taskInfo(localNodeId, true);
        assertThat(info.originalTaskId(), equalTo(task.relocationOrigin().originalTaskId()));
        assertThat(info.originalStartTimeMillis(), equalTo(task.relocationOrigin().originalStartTimeMillis()));
    }

    public void testTaskInfo_relocatedSliceDoesNotExposeOriginalTaskInfo() {
        final TaskId parentTaskId = new TaskId(randomAlphaOfLength(5), randomNonNegativeLong());
        final BulkByPaginatedSearchTask task = createTask(true, true, parentTaskId);
        final String localNodeId = randomAlphaOfLength(5);
        final TaskInfo info = task.taskInfo(localNodeId, true);
        // Falls back to the task's own identity, not the parent's relocation origin
        assertThat(info.originalTaskId(), equalTo(new TaskId(localNodeId, task.getId())));
        assertThat(info.originalTaskId(), not(equalTo(task.relocationOrigin().originalTaskId())));
        assertThat(info.originalStartTimeMillis(), equalTo(task.getStartTime()));
    }

    /** A freshly created task's relocation progress is in the NOT_STARTED state. */
    public void testRelocationProgressStartsNotStarted() {
        BulkByPaginatedSearchTask task = createTask(true);
        assertEquals(BulkByPaginatedSearchTask.RelocationProgress.State.NOT_STARTED, task.getRelocationProgress().current());
        assertFalse(task.useCreateSemanticsForResultStorage());
    }

    /**
     * {@link BulkByPaginatedSearchTask#tryInitiateRelocationHandoff()} transitions NOT_STARTED -> HANDOFF_INITIATED on first
     * call, is idempotent on subsequent calls, and flips {@link BulkByPaginatedSearchTask#useCreateSemanticsForResultStorage()}.
     */
    public void testTryInitiateRelocationHandoff() {
        BulkByPaginatedSearchTask task = createTask(true);
        assertTrue(task.tryInitiateRelocationHandoff());
        assertEquals(BulkByPaginatedSearchTask.RelocationProgress.State.HANDOFF_INITIATED, task.getRelocationProgress().current());
        assertTrue(task.useCreateSemanticsForResultStorage());
        // idempotent
        assertTrue(task.tryInitiateRelocationHandoff());
        assertEquals(BulkByPaginatedSearchTask.RelocationProgress.State.HANDOFF_INITIATED, task.getRelocationProgress().current());
    }

    /**
     * {@link BulkByPaginatedSearchTask#ensureCancellable()} transitions NOT_STARTED -> TASK_CANCELLED on first call and is idempotent.
     */
    public void testEnsureCancellable() {
        BulkByPaginatedSearchTask task = createTask(randomBoolean());
        task.ensureCancellable();
        assertEquals(BulkByPaginatedSearchTask.RelocationProgress.State.TASK_CANCELLED, task.getRelocationProgress().current());
        // idempotent
        task.ensureCancellable();
        assertEquals(BulkByPaginatedSearchTask.RelocationProgress.State.TASK_CANCELLED, task.getRelocationProgress().current());
    }

    /**
     * Once relocation handoff is initiated, a subsequent cancellation attempt must be rejected because the
     * continuation of this task is being resumed on the destination node; cancelling the source here would leave
     * the resumed task running on the destination unaware.
     */
    public void testCancellationRejectedAfterHandoffInitiated() {
        BulkByPaginatedSearchTask task = createTask(true);
        assertTrue(task.tryInitiateRelocationHandoff());
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, task::ensureCancellable);
        assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        assertThat(e.getMessage(), equalTo("cannot cancel task [" + task.getId() + "] because it is being relocated"));
        assertEquals(BulkByPaginatedSearchTask.RelocationProgress.State.HANDOFF_INITIATED, task.getRelocationProgress().current());
        assertTrue(task.useCreateSemanticsForResultStorage());
    }

    /**
     * Once cancellation has begun, the relocation handoff must be aborted so the task is not resumed on the
     * destination node while the source is being cancelled.
     */
    public void testHandoffRejectedAfterCancellationBegan() {
        BulkByPaginatedSearchTask task = createTask(true);
        task.ensureCancellable();
        assertFalse("relocation handoff must be rejected after cancellation began", task.tryInitiateRelocationHandoff());
        assertEquals(BulkByPaginatedSearchTask.RelocationProgress.State.TASK_CANCELLED, task.getRelocationProgress().current());
        assertFalse(task.useCreateSemanticsForResultStorage());
    }

    /**
     * Stresses the CAS gate under concurrent relocation and cancellation attempts: exactly one side wins and the
     * other observes a refusal, never both.
     */
    public void testConcurrentHandoffVsCancellationMutuallyExclusive() throws Exception {
        final BulkByPaginatedSearchTask task = createTask(true);
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicBoolean handoffResult = new AtomicBoolean();
        final AtomicBoolean cancelResult = new AtomicBoolean();
        Thread t1 = new Thread(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            handoffResult.set(task.tryInitiateRelocationHandoff());
        });
        Thread t2 = new Thread(() -> {
            try {
                start.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                task.ensureCancellable();
                cancelResult.set(true);
            } catch (ElasticsearchStatusException e) {
                cancelResult.set(false);
            }
        });
        t1.start();
        t2.start();
        start.countDown();
        t1.join();
        t2.join();
        assertNotSame(
            "at least one side must win (state cannot remain NOT_STARTED)",
            BulkByPaginatedSearchTask.RelocationProgress.State.NOT_STARTED,
            task.getRelocationProgress().current()
        );
        assertTrue("handoff and cancellation are mutually exclusive", handoffResult.get() ^ cancelResult.get());
    }

    private static float randomFloatBetween(float min, float max) {
        return min + (max - min) * random().nextFloat();
    }

    private static TaskId randomTaskId() {
        return randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());
    }
}
