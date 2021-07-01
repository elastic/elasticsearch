/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
        task = new BulkByScrollTask(1, "test_type", "test_action", "test", TaskId.EMPTY_TASK_ID, Collections.emptyMap());
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
            BulkByScrollTask.Status sliceStatus = new BulkByScrollTask.Status(slice, thisTotal, thisUpdated, thisCreated, thisDeleted,
                    thisBatches, thisVersionConflicts, thisNoops, 0, 0, timeValueMillis(0), 0, null, timeValueMillis(0));
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
            taskState.onSliceResponse(listener, slice,
                    new BulkByScrollResponse(timeValueMillis(10), sliceStatus, emptyList(), emptyList(), false));

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
