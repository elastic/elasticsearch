/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
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

import static java.lang.Math.min;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueNanos;
import static org.hamcrest.Matchers.containsString;

public class BulkByScrollTaskTests extends ESTestCase {

    /**
     * Creates a minimal {@link BulkByScrollTask} with random id, type, action, description and optional relocation eligibility.
     * The task is neither a leader nor a worker until {@link BulkByScrollTask#setWorkerCount(int)} or
     * {@link BulkByScrollTask#setWorker(float, Integer)} is called.
     */
    private static BulkByScrollTask createTask(boolean eligibleForRelocationOnShutdown) {
        long taskId = randomLong();
        String type = randomAlphaOfLengthBetween(1, 10);
        String action = randomAlphaOfLengthBetween(1, 10);
        String description = randomAlphaOfLengthBetween(0, 20);
        TaskId parentTaskId = randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(5), randomLong());
        Map<String, String> headers = randomBoolean() ? Collections.emptyMap() : Map.of("header", randomAlphaOfLength(5));
        return new BulkByScrollTask(taskId, type, action, description, parentTaskId, headers, eligibleForRelocationOnShutdown);
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
            () -> new BulkByScrollTask.Status(
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
        BulkByScrollTask.Status status = new BulkByScrollTask.Status(
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
        BulkByScrollTask.Status completedStatus = new BulkByScrollTask.Status(
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
        BulkByScrollTask.Status status = new BulkByScrollTask.Status(
            Arrays.asList(null, null, new BulkByScrollTask.StatusOrException(completedStatus)),
            null
        );
        status.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), containsString("\"slices\":[null,null,{\"slice_id\":2"));
    }

    public void testXContentRepresentationOfSliceFailures() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        Exception e = new Exception();
        BulkByScrollTask.Status status = new BulkByScrollTask.Status(
            Arrays.asList(null, null, new BulkByScrollTask.StatusOrException(e)),
            null
        );
        status.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), containsString("\"slices\":[null,null,{\"type\":\"exception\""));
    }

    public void testMergeStatuses() {
        BulkByScrollTask.StatusOrException[] statuses = new BulkByScrollTask.StatusOrException[between(2, 100)];
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
            statuses[i] = new BulkByScrollTask.StatusOrException(
                new BulkByScrollTask.Status(
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
        BulkByScrollTask.Status merged = new BulkByScrollTask.Status(Arrays.asList(statuses), reasonCancelled);
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
     * Verifies that {@link BulkByScrollTask#getStatus()} returns an empty status (merged from empty slice list)
     * when the task is neither a leader nor a worker.
     */
    public void testGetStatusReturnsEmptyStatusWhenNeitherLeaderNorWorker() {
        BulkByScrollTask task = createTask(randomBoolean());
        assertFalse(task.isLeader());
        assertFalse(task.isWorker());
        BulkByScrollTask.Status status = task.getStatus();
        assertEquals(0, status.getTotal());
        assertEquals(0, status.getUpdated());
        assertEquals(0, status.getCreated());
        assertEquals(0, status.getDeleted());
        assertEquals(0, status.getBatches());
        assertTrue(status.getSliceStatuses().isEmpty());
    }

    /**
     * Verifies that {@link BulkByScrollTask#isLeader()} returns false for a freshly created task and true after
     * {@link BulkByScrollTask#setWorkerCount(int)} is called.
     */
    public void testIsLeader() {
        BulkByScrollTask task = createTask(randomBoolean());
        assertFalse(task.isLeader());
        int slices = between(2, 20);
        task.setWorkerCount(slices);
        assertTrue(task.isLeader());
    }

    /**
     * Verifies that {@link BulkByScrollTask#isWorker()} returns false for a freshly created task and true after
     * {@link BulkByScrollTask#setWorker(float, Integer)} is called.
     */
    public void testIsWorker() {
        BulkByScrollTask task = createTask(randomBoolean());
        assertFalse(task.isWorker());
        float requestsPerSecond = randomFloatBetween(0.1f, 1000f);
        Integer sliceId = randomBoolean() ? null : between(0, 10);
        task.setWorker(requestsPerSecond, sliceId);
        assertTrue(task.isWorker());
    }

    /**
     * Verifies that {@link BulkByScrollTask#setWorkerCount(int)} throws when the task is already a leader.
     */
    public void testSetWorkerCountThrowsWhenAlreadyLeader() {
        BulkByScrollTask task = createTask(randomBoolean());
        task.setWorkerCount(between(2, 10));
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> task.setWorkerCount(between(2, 10)));
        assertThat(exception.getMessage(), containsString("already a leader"));
    }

    /**
     * Verifies that {@link BulkByScrollTask#setWorkerCount(int)} throws when the task is already a worker.
     */
    public void testSetWorkerCountThrowsWhenAlreadyWorker() {
        BulkByScrollTask task = createTask(randomBoolean());
        task.setWorker(randomFloatBetween(0.1f, 100f), null);
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> task.setWorkerCount(between(2, 10)));
        assertThat(exception.getMessage(), containsString("already a worker"));
    }

    /**
     * Verifies that {@link BulkByScrollTask#setWorker(float, Integer)} throws when the task is already a worker.
     */
    public void testSetWorkerThrowsWhenAlreadyWorker() {
        BulkByScrollTask task = createTask(randomBoolean());
        task.setWorker(randomFloatBetween(0.1f, 100f), null);
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> task.setWorker(randomFloatBetween(0.1f, 100f), between(0, 5))
        );
        assertThat(exception.getMessage(), containsString("already a worker"));
    }

    /**
     * Verifies that {@link BulkByScrollTask#setWorker(float, Integer)} throws when the task is already a leader.
     */
    public void testSetWorkerThrowsWhenAlreadyLeader() {
        BulkByScrollTask task = createTask(randomBoolean());
        task.setWorkerCount(between(2, 10));
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> task.setWorker(randomFloatBetween(0.1f, 100f), between(0, 5))
        );
        assertThat(exception.getMessage(), containsString("already a leader"));
    }

    /**
     * Verifies that {@link BulkByScrollTask#getLeaderState()} returns the leader state after
     * {@link BulkByScrollTask#setWorkerCount(int)} and throws when the task is not a leader.
     */
    public void testGetLeaderState() {
        BulkByScrollTask task = createTask(randomBoolean());
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> task.getLeaderState());
        assertThat(exception.getMessage(), containsString("not set to be a leader"));

        int slices = between(2, 20);
        task.setWorkerCount(slices);
        LeaderBulkByScrollTaskState leaderState = task.getLeaderState();
        assertNotNull(leaderState);
        assertEquals(slices, leaderState.getSlices());
    }

    /**
     * Verifies that {@link BulkByScrollTask#getWorkerState()} returns the worker state after
     * {@link BulkByScrollTask#setWorker(float, Integer)} and throws when the task is not a worker.
     */
    public void testGetWorkerState() {
        BulkByScrollTask task = createTask(randomBoolean());
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> task.getWorkerState());
        assertThat(exception.getMessage(), containsString("not set to be a worker"));

        float requestsPerSecond = randomFloatBetween(0.1f, 100f);
        Integer sliceId = randomBoolean() ? null : between(0, 10);
        task.setWorker(requestsPerSecond, sliceId);
        WorkerBulkByScrollTaskState workerState = task.getWorkerState();
        assertNotNull(workerState);
    }

    /**
     * Verifies that {@link BulkByScrollTask#onCancelled()} does not throw when the task is a worker
     * (it delegates to the worker state's handleCancel).
     */
    public void testOnCancelledWhenWorkerDoesNotThrow() {
        BulkByScrollTask task = createTask(randomBoolean());
        task.setWorker(randomFloatBetween(0.1f, 100f), null);
        task.onCancelled();
    }

    /**
     * Verifies that {@link BulkByScrollTask#onCancelled()} does not throw when the task is neither leader nor worker.
     */
    public void testOnCancelledWhenNeitherLeaderNorWorkerDoesNotThrow() {
        BulkByScrollTask task = createTask(randomBoolean());
        task.onCancelled();
    }

    /**
     * Verifies that {@link BulkByScrollTask#isEligibleForRelocationOnShutdown()} returns the value passed to the constructor.
     */
    public void testIsEligibleForRelocationOnShutdown() {
        boolean eligible = randomBoolean();
        BulkByScrollTask task = createTask(eligible);
        assertEquals(eligible, task.isEligibleForRelocationOnShutdown());
    }

    /**
     * Verifies that {@link BulkByScrollTask#requestRelocation()} sets the relocation-requested flag when the task
     * is eligible for relocation, and that {@link BulkByScrollTask#isRelocationRequested()} reflects it.
     */
    public void testRequestRelocationWhenEligible() {
        BulkByScrollTask task = createTask(true);
        assertFalse(task.isRelocationRequested());
        task.requestRelocation();
        assertTrue(task.isRelocationRequested());
    }

    /**
     * Verifies that {@link BulkByScrollTask#requestRelocation()} throws when the task is not eligible for relocation.
     */
    public void testRequestRelocationThrowsWhenNotEligible() {
        BulkByScrollTask task = createTask(false);
        IllegalStateException exception = expectThrows(IllegalStateException.class, task::requestRelocation);
        assertThat(exception.getMessage(), containsString("eligibleForRelocationOnShutdown is false"));
    }

    /**
     * Verifies that {@link BulkByScrollTask#taskInfoGivenSubtaskInfo(String, List)} builds a combined
     * {@link TaskInfo} from the given slice task infos when the task is a leader.
     */
    public void testTaskInfoGivenSubtaskInfo() {
        int slices = between(2, 8);
        BulkByScrollTask task = createTask(randomBoolean());
        task.setWorkerCount(slices);

        String localNodeId = randomAlphaOfLength(5);
        List<TaskInfo> sliceInfoList = Arrays.asList(new TaskInfo[slices]);
        for (int sliceIndex = 0; sliceIndex < slices; sliceIndex++) {
            BulkByScrollTask.Status sliceStatus = new BulkByScrollTask.Status(
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
        assertTrue(combinedTaskInfo.status() instanceof BulkByScrollTask.Status);
    }

    /**
     * Verifies that {@link BulkByScrollTask#taskInfoGivenSubtaskInfo(String, List)} throws when the task is not a leader.
     */
    public void testTaskInfoGivenSubtaskInfoThrowsWhenNotLeader() {
        BulkByScrollTask task = createTask(randomBoolean());
        String localNodeId = randomAlphaOfLength(5);
        List<TaskInfo> sliceInfoList = Collections.emptyList();
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> task.taskInfoGivenSubtaskInfo(localNodeId, sliceInfoList)
        );
        assertThat(exception.getMessage(), containsString("not set to be a leader"));
    }

    private static float randomFloatBetween(float min, float max) {
        return min + (max - min) * random().nextFloat();
    }
}
