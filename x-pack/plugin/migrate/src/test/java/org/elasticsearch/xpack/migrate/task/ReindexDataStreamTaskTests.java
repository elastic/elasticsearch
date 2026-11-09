/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReindexDataStreamTaskTests extends ESTestCase {

    /**
     * {@code GetMigrationReindexStatusTransportAction} streams over {@code getStatus().inProgress()} on a transport thread while the
     * reindex task adds and removes indices from another thread. The status must be safe to iterate in that situation.
     */
    public void testStatusInProgressCanBeIteratedWhileIndicesAreUpdated() throws Exception {
        // The status only needs an (empty) cluster state to be built, so a mock is enough here and avoids starting a real cluster service.
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        ReindexDataStreamTask task = new ReindexDataStreamTask(
            ProjectId.DEFAULT,
            clusterService,
            System.currentTimeMillis(),
            0,
            0,
            randomNonNegativeLong(),
            "type",
            "action",
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );

        // A reasonably large set makes it likely that an iteration overlaps with a modification.
        for (int i = 0; i < 200; i++) {
            task.incrementInProgressIndicesCount("index-" + i);
        }

        final int readerIterations = 20_000;
        final AtomicBoolean readerDone = new AtomicBoolean();
        final AtomicReference<Throwable> readerFailure = new AtomicReference<>();
        final AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        final CyclicBarrier startBarrier = new CyclicBarrier(2);

        Thread writer = new Thread(() -> {
            try {
                startBarrier.await();
                for (int i = 0; readerDone.get() == false; i++) {
                    String index = "churn-" + (i % 50);
                    task.incrementInProgressIndicesCount(index);
                    task.reindexSucceeded(index);
                }
            } catch (Throwable t) {
                writerFailure.set(t);
            }
        }, "writer");

        Thread reader = new Thread(() -> {
            try {
                startBarrier.await();
                for (int i = 0; i < readerIterations; i++) {
                    // Same access pattern as GetMigrationReindexStatusTransportAction#fetchInProgressStatsAndReportStatus
                    task.getStatus().inProgress().stream().map(index -> index + "-dest").toList();
                }
            } catch (Throwable t) {
                readerFailure.set(t);
            } finally {
                readerDone.set(true);
            }
        }, "reader");

        writer.start();
        reader.start();
        writer.join();
        reader.join();

        assertNull("writer failed", writerFailure.get());
        assertNull("iterating the status' in-progress indices failed while the task was updating them", readerFailure.get());
    }
}
