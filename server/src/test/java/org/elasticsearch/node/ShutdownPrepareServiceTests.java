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
import org.elasticsearch.index.reindex.ResumeInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;

public class ShutdownPrepareServiceTests extends ESTestCase {

    public void testMaybeRequestRelocationForBulkByScroll_nonRelocatableLeader() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomTaskId(),
            "transport",
            "test:action/name",
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            false,
            randomOrigin()
        );
        task.setWorkerCount(randomIntBetween(1, 20));
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task);
        assertThat(task.isRelocationRequested(), is(false));
    }

    public void testMaybeRequestRelocationForBulkByScroll_relocatableLeader() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomTaskId(),
            "transport",
            "test:action/name",
            "description",
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            true,
            null
        );
        task.setWorkerCount(randomIntBetween(1, 20));
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task);
        assertThat(task.isRelocationRequested(), is(true));
    }

    public void testMaybeRequestRelocationForBulkByScroll_nonRelocatableWorker() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomTaskId(),
            "transport",
            "test:action/name",
            "description",
            new TaskId("localNode", randomLong()),
            Map.of(),
            false,
            randomOrigin()
        );
        task.setWorker(randomFloat(), randomInt());
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task);
        assertThat(task.isRelocationRequested(), is(false));
    }

    public void testMaybeRequestRelocationForBulkByScroll_relocatableWorker() {
        BulkByScrollTask task = new BulkByScrollTask(
            randomTaskId(),
            "transport",
            "test:action/name",
            "description",
            new TaskId("localNode", randomLong()),
            Map.of(),
            true,
            null
        );
        task.setWorker(randomFloat(), randomInt());
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task);
        assertThat(task.isRelocationRequested(), is(true));
    }

    public void testMaybeRequestRelocationForBulkByScroll_skipsRecentlyRelocatedTask() {
        final BulkByScrollTask task = new BulkByScrollTask(
            randomTaskId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            Map.of(),
            true,
            new ResumeInfo.RelocationOrigin(new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()), randomNonNegativeLong())
        );
        final boolean isLeader = randomBoolean();
        if (isLeader) {
            task.setWorkerCount(between(2, 20));
        } else {
            task.setWorker(randomFloat(), randomInt());
        }
        assertThat(task.isRelocatedTask(), is(true));
        assertThat(task.isRelocationRequested(), is(false));
        // task exists for less than required time, skip relocation
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task, TimeUnit.SECONDS.toNanos(between(30, 120)));
        assertThat(task.isRelocationRequested(), is(false));
    }

    public void testMaybeRequestRelocationForBulkByScroll_allowsOldRelocatedTask() {
        final BulkByScrollTask task = new BulkByScrollTask(
            randomTaskId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            Map.of(),
            true,
            new ResumeInfo.RelocationOrigin(new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()), randomNonNegativeLong())
        );
        final boolean isLeader = randomBoolean();
        if (isLeader) {
            task.setWorkerCount(between(2, 20));
        } else {
            task.setWorker(randomFloat(), randomInt());
        }
        assertThat(task.isRelocatedTask(), is(true));
        assertThat(task.isRelocationRequested(), is(false));
        // task always exists longer than 0 nanos, relocate
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task, TimeUnit.SECONDS.toNanos(0));
        assertThat(task.isRelocationRequested(), is(true));
    }

    public void testMaybeRequestRelocationForBulkByScroll_cooldownDoesNotApplyToNonRelocatedTask() {
        final BulkByScrollTask task = new BulkByScrollTask(
            randomTaskId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()),
            Map.of(),
            true,
            null
        );
        final boolean isLeader = randomBoolean();
        if (isLeader) {
            task.setWorkerCount(between(2, 20));
        } else {
            task.setWorker(randomFloat(), randomInt());
        }
        assertThat(task.isRelocatedTask(), is(false));
        assertThat(task.isRelocationRequested(), is(false));
        // regardless of value, should relocate
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task, TimeUnit.SECONDS.toNanos(between(0, 120)));
        assertThat(task.isRelocationRequested(), is(true));
    }

    public void testMaybeRequestRelocationForBulkByScroll_notBulkByScroll() {
        Task task = new Task(randomInt(), "transport", "test:action/name", "description", new TaskId("localNode", randomLong()), Map.of());
        ShutdownPrepareService.maybeRequestRelocationForBulkByScroll(task);
        // No assertion, just check it doesn't blow up
    }

    private static TaskId randomTaskId() {
        return randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());

    }

    private static ResumeInfo.RelocationOrigin randomOrigin() {
        return randomBoolean()
            ? null
            : new ResumeInfo.RelocationOrigin(new TaskId(randomAlphaOfLength(10), randomNonNegativeLong()), randomNonNegativeLong());
    }
}
