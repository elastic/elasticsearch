/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.is;

public class ShutdownPrepareServiceTests extends ESTestCase {

    public void testMaybeRequestRelocationForBulkByScroll_nonRelocatableLeader() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomInt(),
            "transport",
            "test:action/name",
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false
        );
        task.setWorkerCount(randomIntBetween(1, 20));
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task);
        assertThat(task.isRelocationRequested(), is(false));
    }

    public void testMaybeRequestRelocationForBulkByScroll_relocatableLeader() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomInt(),
            "transport",
            "test:action/name",
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            true
        );
        task.setWorkerCount(randomIntBetween(1, 20));
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task);
        assertThat(task.isRelocationRequested(), is(true));
    }

    public void testMaybeRequestRelocationForBulkByScroll_nonRelocatableWorker() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomInt(),
            "transport",
            "test:action/name",
            "description",
            new TaskId("localNode", randomLong()),
            Map.of(),
            false
        );
        task.setWorker(randomFloat(), randomInt());
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task);
        assertThat(task.isRelocationRequested(), is(false));
    }

    public void testMaybeRequestRelocationForBulkByScroll_relocatableWorker() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomInt(),
            "transport",
            "test:action/name",
            "description",
            new TaskId("localNode", randomLong()),
            Map.of(),
            true
        );
        task.setWorker(randomFloat(), randomInt());
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task);
        assertThat(task.isRelocationRequested(), is(true));
    }

    public void testMaybeRequestRelocationForBulkByScroll_notBulkByScroll() {
        Task task = new Task(randomInt(), "transport", "test:action/name", "description", new TaskId("localNode", randomLong()), Map.of());
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task);
        // No assertion, just check it doesn't blow up
    }
}
