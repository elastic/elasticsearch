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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.Collections.emptyList;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.test.ActionListenerUtils.neverCalledListener;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class LeaderBulkByScrollTaskStateTests extends ESTestCase {
    private int slices;
    private BulkByPaginatedSearchTask task;
    private LeaderBulkByScrollTaskState taskState;

    @Before
    public void createTask() {
        slices = between(2, 50);
        task = new BulkByPaginatedSearchTask(
            new TaskId(randomAlphaOfLength(10), 1),
            "test_type",
            "test_action",
            "test",
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            false,
            randomBoolean() ? null : randomOrigin()
        );
        task.setWorkerCount(slices, Float.POSITIVE_INFINITY);
        taskState = task.getLeaderState();
    }

    public void testBasicData() {
        assertEquals(1, task.getId());
        assertEquals("test_type", task.getType());
        assertEquals("test_action", task.getAction());
        assertEquals("test", task.getDescription());
    }

    public void testProgress() {
        long total = 0;
        long created = 0;
        long updated = 0;
        long deleted = 0;
        long noops = 0;
        long versionConflicts = 0;
        int batches = 0;
        List<BulkByPaginatedSearchTask.StatusOrException> sliceStatuses = Arrays.asList(
            new BulkByPaginatedSearchTask.StatusOrException[slices]
        );
        BulkByPaginatedSearchTask.Status status = task.getStatus();
        assertEquals(total, status.getTotal());
        assertEquals(created, status.getCreated());
        assertEquals(updated, status.getUpdated());
        assertEquals(deleted, status.getDeleted());
        assertEquals(noops, status.getNoops());
        assertEquals(versionConflicts, status.getVersionConflicts());
        assertEquals(batches, status.getBatches());
        assertEquals(sliceStatuses, status.getSliceStatuses());

        for (int slice = 0; slice < slices; slice++) {
            int thisTotal = between(10, 10000);
            int thisCreated = between(0, thisTotal);
            int thisUpdated = between(0, thisTotal - thisCreated);
            int thisDeleted = between(0, thisTotal - thisCreated - thisUpdated);
            int thisNoops = thisTotal - thisCreated - thisUpdated - thisDeleted;
            int thisVersionConflicts = between(0, 1000);
            int thisBatches = between(1, 100);
            BulkByPaginatedSearchTask.Status sliceStatus = new BulkByPaginatedSearchTask.Status(
                slice,
                thisTotal,
                thisUpdated,
                thisCreated,
                thisDeleted,
                thisBatches,
                thisVersionConflicts,
                thisNoops,
                0,
                0,
                timeValueMillis(0),
                0,
                null,
                timeValueMillis(0)
            );
            total += thisTotal;
            created += thisCreated;
            updated += thisUpdated;
            deleted += thisDeleted;
            noops += thisNoops;
            versionConflicts += thisVersionConflicts;
            batches += thisBatches;
            sliceStatuses.set(slice, new BulkByPaginatedSearchTask.StatusOrException(sliceStatus));

            @SuppressWarnings("unchecked")
            ActionListener<BulkByScrollResponse> listener = slice < slices - 1 ? neverCalledListener() : mock(ActionListener.class);
            taskState.onSliceResponse(
                listener,
                slice,
                new BulkByScrollResponse(timeValueMillis(10), sliceStatus, emptyList(), emptyList(), false)
            );

            status = task.getStatus();
            assertEquals(total, status.getTotal());
            assertEquals(created, status.getCreated());
            assertEquals(updated, status.getUpdated());
            assertEquals(deleted, status.getDeleted());
            assertEquals(versionConflicts, status.getVersionConflicts());
            assertEquals(batches, status.getBatches());
            assertEquals(noops, status.getNoops());
            assertEquals(sliceStatuses, status.getSliceStatuses());

            if (slice == slices - 1) {
                // The whole thing succeeded so we should have got the success
                status = captureResponse(BulkByScrollResponse.class, listener).getStatus();
                assertEquals(total, status.getTotal());
                assertEquals(created, status.getCreated());
                assertEquals(updated, status.getUpdated());
                assertEquals(deleted, status.getDeleted());
                assertEquals(versionConflicts, status.getVersionConflicts());
                assertEquals(batches, status.getBatches());
                assertEquals(noops, status.getNoops());
                assertEquals(sliceStatuses, status.getSliceStatuses());
            }
        }
    }

    // --- relocation tests ---

    public void testRelocationAllSlicesHaveResumeInfo() {
        final int sliceCount = between(2, 5);
        final var leaderTask = createRelocationLeaderTask(sliceCount);
        final var leaderState = leaderTask.getLeaderState();

        final BulkByScrollResponse[] sliceResponses = new BulkByScrollResponse[sliceCount];
        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        for (int i = 0; i < sliceCount; i++) {
            sliceResponses[i] = resumeSliceResponse(i);
            final ActionListener<BulkByScrollResponse> listener = i < sliceCount - 1 ? neverCalledListener() : future;
            leaderState.onSliceResponse(listener, i, sliceResponses[i]);
        }

        assertTrue(future.isDone());
        final BulkByScrollResponse response = future.actionGet();
        assertTrue(response.getTaskResumeInfo().isPresent());
        final ResumeInfo resumeInfo = response.getTaskResumeInfo().get();
        assertNull(resumeInfo.worker());
        assertNotNull(resumeInfo.slices());
        assertEquals(sliceCount, resumeInfo.slices().size());
        for (int i = 0; i < sliceCount; i++) {
            final ResumeInfo.SliceStatus sliceStatus = resumeInfo.slices().get(i);
            assertEquals(i, sliceStatus.sliceId());
            assertNull("slice " + i + " should not have a result", sliceStatus.result());
            // resume info should be the same object from the original slice response
            assertSame(sliceResponses[i].getTaskResumeInfo().get().worker(), sliceStatus.resumeInfo());
        }
    }

    public void testRelocationMixedCompletedAndResumeInfo() {
        final int sliceCount = between(3, 6);
        final var leaderTask = createRelocationLeaderTask(sliceCount);
        final var leaderState = leaderTask.getLeaderState();

        // the first slice completes normally, the rest have resume info
        final BulkByScrollResponse completedResponse = completedSliceResponse(0);
        final BulkByScrollResponse[] resumeResponses = new BulkByScrollResponse[sliceCount - 1];
        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        leaderState.onSliceResponse(neverCalledListener(), 0, completedResponse);
        for (int i = 1; i < sliceCount; i++) {
            resumeResponses[i - 1] = resumeSliceResponse(i);
            final ActionListener<BulkByScrollResponse> listener = i < sliceCount - 1 ? neverCalledListener() : future;
            leaderState.onSliceResponse(listener, i, resumeResponses[i - 1]);
        }

        assertTrue(future.isDone());
        final BulkByScrollResponse response = future.actionGet();
        assertTrue(response.getTaskResumeInfo().isPresent());
        final ResumeInfo resumeInfo = response.getTaskResumeInfo().get();
        assertEquals(sliceCount, resumeInfo.slices().size());

        // slice 0 completed normally — should have WorkerResult with the original response, no resumeInfo
        final ResumeInfo.SliceStatus slice0 = resumeInfo.slices().get(0);
        assertNull(slice0.resumeInfo());
        assertNotNull(slice0.result());
        assertSame(completedResponse, slice0.result().response());
        assertNull(slice0.result().failure());

        // the remaining slices have resume info matching the original responses
        for (int i = 1; i < sliceCount; i++) {
            final ResumeInfo.SliceStatus sliceStatus = resumeInfo.slices().get(i);
            assertNull("slice " + i + " should not have a result", sliceStatus.result());
            assertSame(resumeResponses[i - 1].getTaskResumeInfo().get().worker(), sliceStatus.resumeInfo());
        }
    }

    public void testRelocationAllSlicesCompletedNoResumeInfo() {
        final int sliceCount = between(2, 5);
        final var leaderTask = createRelocationLeaderTask(sliceCount);
        final var leaderState = leaderTask.getLeaderState();

        // all slices complete normally — relocation should be skipped even though requested
        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        for (int i = 0; i < sliceCount; i++) {
            final ActionListener<BulkByScrollResponse> listener = i < sliceCount - 1 ? neverCalledListener() : future;
            leaderState.onSliceResponse(listener, i, completedSliceResponse(i));
        }

        assertTrue(future.isDone());
        final BulkByScrollResponse response = future.actionGet();
        // should be a normal merged response, no ResumeInfo
        assertFalse(response.getTaskResumeInfo().isPresent());
    }

    /**
     * When slices complete with different PIT IDs, the merged response uses the PIT ID from the last-completing slice.
     */
    public void testMergedResponseUsesLatestPitIdFromLastCompletingSlice() {
        final int sliceCount = 3;
        final var leaderTask = createLeaderTask(sliceCount);
        final var leaderState = leaderTask.getLeaderState();

        final BytesReference pitIdSlice0 = new BytesArray("pit-slice-0");
        final BytesReference pitIdSlice1 = new BytesArray("pit-slice-1");
        final BytesReference pitIdSlice2 = new BytesArray("pit-slice-2");

        // Complete slices in reverse order so slice 2 completes last — its pitId should be used
        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        leaderState.onSliceResponse(neverCalledListener(), 0, completedSliceResponseWithPitId(0, pitIdSlice0));
        leaderState.onSliceResponse(neverCalledListener(), 1, completedSliceResponseWithPitId(1, pitIdSlice1));
        leaderState.onSliceResponse(future, 2, completedSliceResponseWithPitId(2, pitIdSlice2));

        assertTrue(future.isDone());
        final BulkByScrollResponse response = future.actionGet();
        assertTrue(response.getPitId().isPresent());
        assertThat(response.getPitId().get(), equalTo(pitIdSlice2));
    }

    private static BulkByPaginatedSearchTask createLeaderTask(final int sliceCount) {
        final BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomBoolean() ? null : randomOrigin()
        );
        task.setWorkerCount(sliceCount, Float.POSITIVE_INFINITY);
        return task;
    }

    private static BulkByScrollResponse completedSliceResponseWithPitId(final int sliceId, BytesReference pitId) {
        final var sliceStatus = statusForSlice(sliceId);
        return new BulkByScrollResponse(randomTimeValue(), sliceStatus, List.of(), List.of(), false, null, pitId);
    }

    public void testRelocationMixedFailuresAndResumeInfo() {
        final int sliceCount = between(3, 6);
        final var leaderTask = createRelocationLeaderTask(sliceCount);
        final var leaderState = leaderTask.getLeaderState();

        // the first slice fails, the remaining have resume info
        final RuntimeException sliceFailure = new RuntimeException("slice 0 failed");
        final BulkByScrollResponse[] resumeResponses = new BulkByScrollResponse[sliceCount - 1];
        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        leaderState.onSliceFailure(neverCalledListener(), 0, sliceFailure);
        for (int i = 1; i < sliceCount; i++) {
            resumeResponses[i - 1] = resumeSliceResponse(i);
            final ActionListener<BulkByScrollResponse> listener = i < sliceCount - 1 ? neverCalledListener() : future;
            leaderState.onSliceResponse(listener, i, resumeResponses[i - 1]);
        }

        assertTrue(future.isDone());
        final BulkByScrollResponse response = future.actionGet();
        assertTrue(response.getTaskResumeInfo().isPresent());
        final ResumeInfo resumeInfo = response.getTaskResumeInfo().get();
        assertEquals(sliceCount, resumeInfo.slices().size());

        // slice 0 failed — should have WorkerResult with the original failure
        final ResumeInfo.SliceStatus slice0 = resumeInfo.slices().get(0);
        assertNull(slice0.resumeInfo());
        assertNotNull(slice0.result());
        assertNull(slice0.result().response());
        assertSame(sliceFailure, slice0.result().failure());

        // the remaining slices have resume info matching the original responses
        for (int i = 1; i < sliceCount; i++) {
            final ResumeInfo.SliceStatus sliceStatus = resumeInfo.slices().get(i);
            assertNull("slice " + i + " should not have a result", sliceStatus.result());
            assertSame(resumeResponses[i - 1].getTaskResumeInfo().get().worker(), sliceStatus.resumeInfo());
        }
    }

    public void testFinalResponseUsesLeaderStoredRps() {
        final float leaderRps = randomFloatBetween(1f, 1000f, true);
        final int sliceCount = between(2, 5);
        final BulkByPaginatedSearchTask leaderTask = new BulkByPaginatedSearchTask(
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            null
        );
        leaderTask.setWorkerCount(sliceCount, leaderRps);
        final LeaderBulkByScrollTaskState state = leaderTask.getLeaderState();

        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        for (int i = 0; i < sliceCount; i++) {
            float childRps = randomFloatBetween(0.1f, 50f, true);
            BulkByPaginatedSearchTask.Status childStatus = new BulkByPaginatedSearchTask.Status(
                i,
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
                childRps,
                null,
                timeValueMillis(0)
            );
            ActionListener<BulkByScrollResponse> listener = i < sliceCount - 1 ? neverCalledListener() : future;
            state.onSliceResponse(
                listener,
                i,
                new BulkByScrollResponse(timeValueMillis(randomNonNegativeLong()), childStatus, emptyList(), emptyList(), false)
            );
        }

        assertTrue(future.isDone());
        final BulkByScrollResponse response = future.actionGet();
        assertThat(response.getStatus().getRequestsPerSecond(), equalTo(leaderRps));
    }

    public void testGetStatusReportsStoredRpsNotSumOfChildren() {
        final float leaderRps = randomFloatBetween(1f, 1000f, true);
        final int sliceCount = between(2, 10);
        final BulkByPaginatedSearchTask leaderTask = new BulkByPaginatedSearchTask(
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            null
        );
        leaderTask.setWorkerCount(sliceCount, leaderRps);
        final LeaderBulkByScrollTaskState state = leaderTask.getLeaderState();

        // Simulate children completing with various per-child RPS that don't sum to leaderRps
        for (int i = 0; i < sliceCount; i++) {
            float childRps = randomFloatBetween(0.1f, 50f, true);
            BulkByPaginatedSearchTask.Status childStatus = new BulkByPaginatedSearchTask.Status(
                i,
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
                childRps,
                null,
                timeValueMillis(0)
            );
            state.onSliceResponse(
                i < sliceCount - 1 ? neverCalledListener() : ActionListener.noop(),
                i,
                new BulkByScrollResponse(timeValueMillis(randomNonNegativeLong()), childStatus, emptyList(), emptyList(), false)
            );
        }

        assertThat(leaderTask.getStatus().getRequestsPerSecond(), equalTo(leaderRps));

        // After rethrottle, getStatus reports the new value
        final float rethrottledRps = randomFloatBetween(1f, 2000f, true);
        state.setRequestsPerSecondWithRelocationGuard(rethrottledRps);
        assertThat(leaderTask.getStatus().getRequestsPerSecond(), equalTo(rethrottledRps));
    }

    public void testCaptureRequestsPerSecondForRelocation() {
        final float initialRPS = randomFloatBetween(0.1f, 1000f, true);
        final BulkByPaginatedSearchTask leaderTask = new BulkByPaginatedSearchTask(
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomBoolean() ? null : randomOrigin()
        );
        leaderTask.setWorkerCount(between(2, 10), initialRPS);
        final LeaderBulkByScrollTaskState state = leaderTask.getLeaderState();

        assertThat(state.captureRequestsPerSecondForRelocation(), equalTo(initialRPS));
    }

    public void testSetRequestsPerSecondWithRelocationGuardUpdatesValue() {
        final float newRPS = randomFloatBetween(0.1f, 1000f, true);
        taskState.setRequestsPerSecondWithRelocationGuard(newRPS);
        assertThat(taskState.captureRequestsPerSecondForRelocation(), equalTo(newRPS));
    }

    public void testSetRequestsPerSecondWithRelocationGuardThrows503AfterCapture() {
        taskState.captureRequestsPerSecondForRelocation();
        final float rps = randomFloatBetween(0.1f, 1000f, true);
        final ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> taskState.setRequestsPerSecondWithRelocationGuard(rps)
        );
        assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testCaptureAndRethrottleRaceCondition() throws Exception {
        final float initialRps = randomFloatBetween(0.1f, 500f, true);
        final float rethrottledRps = randomFloatBetween(501f, 1000f, true);
        taskState.setRequestsPerSecondWithRelocationGuard(initialRps);

        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch go = new CountDownLatch(1);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            final Future<Float> captureFuture = executor.submit(() -> {
                ready.countDown();
                safeAwait(go);
                return taskState.captureRequestsPerSecondForRelocation();
            });
            final Future<ElasticsearchStatusException> rethrottleFuture = executor.submit(() -> {
                ready.countDown();
                safeAwait(go);
                try {
                    taskState.setRequestsPerSecondWithRelocationGuard(rethrottledRps);
                    return null;
                } catch (ElasticsearchStatusException e) {
                    return e;
                }
            });

            safeAwait(ready);
            go.countDown();

            final float captured = safeGet(captureFuture);
            final ElasticsearchStatusException rethrottleError = safeGet(rethrottleFuture);

            // check mutual exclusion of race, we either:
            // 1. fail to rethrottle, RPS doesn't change, and we get an exception
            // 2. or we succeed to rethrottle, RPS changes, and we get no exception
            if (rethrottleError != null) {
                assertThat(captured, equalTo(initialRps));
                assertThat(rethrottleError.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            } else {
                assertThat(captured, equalTo(rethrottledRps));
            }
        } finally {
            terminate(executor);
        }
    }

    private static BulkByPaginatedSearchTask createRelocationLeaderTask(final int sliceCount) {
        final BulkByPaginatedSearchTask task = new BulkByPaginatedSearchTask(
            new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            true,
            randomBoolean() ? null : randomOrigin()
        );
        task.setWorkerCount(sliceCount, Float.POSITIVE_INFINITY);
        task.requestRelocation();
        task.getLeaderState().setNodeToRelocateToSupplier(() -> Optional.of("target-node"));
        return task;
    }

    private static BulkByPaginatedSearchTask.Status statusForSlice(final int sliceId) {
        return new BulkByPaginatedSearchTask.Status(
            sliceId,
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomTimeValue(),
            randomFloat(),
            randomBoolean() ? randomAlphaOfLength(5) : null,
            randomTimeValue()
        );

    }

    private static BulkByScrollResponse resumeSliceResponse(final int sliceId) {
        final var workerStatus = statusForSlice(sliceId);
        final var workerResumeInfo = new ResumeInfo.ScrollWorkerResumeInfo(
            "scroll-" + sliceId,
            randomNonNegativeLong(),
            workerStatus,
            null
        );
        return new BulkByScrollResponse(
            randomTimeValue(),
            new BulkByPaginatedSearchTask.Status(List.of(), null, 0f),
            List.of(),
            List.of(),
            false,
            new ResumeInfo(randomOrigin(), workerResumeInfo, null)
        );
    }

    private static BulkByScrollResponse completedSliceResponse(final int sliceId) {
        final var sliceStatus = statusForSlice(sliceId);
        return new BulkByScrollResponse(randomTimeValue(), sliceStatus, List.of(), List.of(), false);
    }

    private static ResumeInfo.RelocationOrigin randomOrigin() {
        return new ResumeInfo.RelocationOrigin(
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            randomNonNegativeLong()
        );
    }

    private <T> T captureResponse(Class<T> responseClass, ActionListener<T> listener) {
        ArgumentCaptor<Exception> failure = ArgumentCaptor.forClass(Exception.class);
        // Rethrow any failures just so we get a nice exception if there were any. We don't expect any though.
        verify(listener, atMost(1)).onFailure(failure.capture());
        if (false == failure.getAllValues().isEmpty()) {
            throw new AssertionError(failure.getValue());
        }
        ArgumentCaptor<T> response = ArgumentCaptor.forClass(responseClass);
        verify(listener).onResponse(response.capture());
        return response.getValue();
    }

}
