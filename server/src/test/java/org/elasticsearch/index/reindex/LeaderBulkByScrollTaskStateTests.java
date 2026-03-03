/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class LeaderBulkByScrollTaskStateTests extends ESTestCase {
    private int slices;
    private BulkByScrollTask task;
    private LeaderBulkByScrollTaskState taskState;

    @Before
    public void createTask() {
        slices = between(2, 50);
        task = new BulkByScrollTask(1, "test_type", "test_action", "test", TaskId.EMPTY_TASK_ID, Collections.emptyMap(), false);
        task.setWorkerCount(slices);
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
        List<BulkByScrollTask.StatusOrException> sliceStatuses = Arrays.asList(new BulkByScrollTask.StatusOrException[slices]);
        BulkByScrollTask.Status status = task.getStatus();
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
            BulkByScrollTask.Status sliceStatus = new BulkByScrollTask.Status(
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
            sliceStatuses.set(slice, new BulkByScrollTask.StatusOrException(sliceStatus));

            @SuppressWarnings("unchecked")
            ActionListener<BulkByScrollResponse> listener = slice < slices - 1 ? neverCalled() : mock(ActionListener.class);
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
            final ActionListener<BulkByScrollResponse> listener = i < sliceCount - 1 ? neverCalled() : future;
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
        leaderState.onSliceResponse(neverCalled(), 0, completedResponse);
        for (int i = 1; i < sliceCount; i++) {
            resumeResponses[i - 1] = resumeSliceResponse(i);
            final ActionListener<BulkByScrollResponse> listener = i < sliceCount - 1 ? neverCalled() : future;
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
            final ActionListener<BulkByScrollResponse> listener = i < sliceCount - 1 ? neverCalled() : future;
            leaderState.onSliceResponse(listener, i, completedSliceResponse(i));
        }

        assertTrue(future.isDone());
        final BulkByScrollResponse response = future.actionGet();
        // should be a normal merged response, no ResumeInfo
        assertFalse(response.getTaskResumeInfo().isPresent());
    }

    public void testRelocationMixedFailuresAndResumeInfo() {
        final int sliceCount = between(3, 6);
        final var leaderTask = createRelocationLeaderTask(sliceCount);
        final var leaderState = leaderTask.getLeaderState();

        // the first slice fails, the remaining have resume info
        final RuntimeException sliceFailure = new RuntimeException("slice 0 failed");
        final BulkByScrollResponse[] resumeResponses = new BulkByScrollResponse[sliceCount - 1];
        final PlainActionFuture<BulkByScrollResponse> future = new PlainActionFuture<>();
        leaderState.onSliceFailure(neverCalled(), 0, sliceFailure);
        for (int i = 1; i < sliceCount; i++) {
            resumeResponses[i - 1] = resumeSliceResponse(i);
            final ActionListener<BulkByScrollResponse> listener = i < sliceCount - 1 ? neverCalled() : future;
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

    private static BulkByScrollTask createRelocationLeaderTask(final int sliceCount) {
        final BulkByScrollTask task = new BulkByScrollTask(
            randomNonNegativeLong(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            true
        );
        task.setWorkerCount(sliceCount);
        task.requestRelocation();
        task.getLeaderState().setNodeToRelocateToSupplier(() -> Optional.of("target-node"));
        return task;
    }

    private static BulkByScrollTask.Status statusForSlice(final int sliceId) {
        return new BulkByScrollTask.Status(
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
            new BulkByScrollTask.Status(List.of(), null),
            List.of(),
            List.of(),
            false,
            new ResumeInfo(workerResumeInfo, null)
        );
    }

    private static BulkByScrollResponse completedSliceResponse(final int sliceId) {
        final var sliceStatus = statusForSlice(sliceId);
        return new BulkByScrollResponse(randomTimeValue(), sliceStatus, List.of(), List.of(), false);
    }

    private <T> ActionListener<T> neverCalled() {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T response) {
                throw new RuntimeException("Expected no interactions but got [" + response + "]");
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException("Expected no interations but was received a failure", e);
            }
        };
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
