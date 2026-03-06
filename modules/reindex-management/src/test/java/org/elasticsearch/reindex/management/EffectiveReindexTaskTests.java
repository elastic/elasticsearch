/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

public class EffectiveReindexTaskTests extends AbstractWireSerializingTestCase<EffectiveReindexTask> {

    @Override
    protected Writeable.Reader<EffectiveReindexTask> instanceReader() {
        return EffectiveReindexTask::new;
    }

    @Override
    protected EffectiveReindexTask createTestInstance() {
        TaskResult original = randomTaskResult();
        TaskResult relocated = randomBoolean() ? randomTaskResult() : null;
        return new EffectiveReindexTask(original, relocated);
    }

    @Override
    protected EffectiveReindexTask mutateInstance(EffectiveReindexTask instance) {
        if (instance.relocated() == null) {
            return switch (between(0, 1)) {
                case 0 -> new EffectiveReindexTask(randomValueOtherThan(instance.original(), this::randomTaskResult), null);
                case 1 -> new EffectiveReindexTask(instance.original(), randomTaskResult());
                default -> throw new AssertionError("invalid switch");
            };
        } else {
            return switch (between(0, 2)) {
                case 0 -> new EffectiveReindexTask(randomValueOtherThan(instance.original(), this::randomTaskResult), instance.relocated());
                case 1 -> new EffectiveReindexTask(instance.original(), randomValueOtherThan(instance.relocated(), this::randomTaskResult));
                case 2 -> new EffectiveReindexTask(instance.original(), null);
                default -> throw new AssertionError("invalid switch");
            };
        }
    }

    public void testOriginalOnly() {
        final TaskId taskId = randomTaskId();
        final boolean cancellable = randomBoolean();
        final boolean cancelled = cancellable && randomBoolean();
        final TaskInfo info = new TaskInfo(
            taskId,
            randomAlphaOfLength(10),
            taskId.getNodeId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            null,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            cancellable,
            cancelled,
            randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId(),
            Map.of()
        );
        final TaskResult result = new TaskResult(randomBoolean(), info);
        final EffectiveReindexTask task = new EffectiveReindexTask(result, null);

        assertThat(task.latestTask(), equalTo(result));
        assertNull(task.status());
        assertThat(task.description(), equalTo(info.description()));
        assertThat(task.startTimeMillis(), equalTo(info.startTime()));
        assertThat(task.runningTimeNanos(), equalTo(info.runningTimeNanos()));
        assertThat(task.latestTaskId(), equalTo(taskId));
        assertThat(task.originalTaskId(), equalTo(taskId));
        assertThat(task.isCompleted(), equalTo(result.isCompleted()));
        assertThat(task.isCancelled(), equalTo(cancelled));
        assertNull(task.error());
        assertNull(task.response());
        assertThat(task.relocatedTask(), equalTo(Optional.empty()));
        assertThat(task.original(), equalTo(result));
    }

    public void testWithRelocated() {
        final TaskId originalTaskId = randomTaskId();
        final TaskId relocatedTaskId = randomValueOtherThan(originalTaskId, this::randomTaskId);

        final boolean originalCancellable = randomBoolean();
        final TaskInfo original = new TaskInfo(
            originalTaskId,
            randomAlphaOfLength(10),
            originalTaskId.getNodeId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            null,
            randomLongBetween(0, 1_000), // prevent overflow with this being added to runningTimeInNanos
            randomNonNegativeLong(),
            originalCancellable,
            originalCancellable && randomBoolean(),
            randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId(),
            Map.of()
        );
        final boolean relocatedCancellable = randomBoolean();
        final TaskInfo relocated = new TaskInfo(
            relocatedTaskId,
            randomAlphaOfLength(10),
            relocatedTaskId.getNodeId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            null,
            randomLongBetween(original.startTime(), original.startTime() + 1_000),
            randomNonNegativeLong(),
            relocatedCancellable,
            relocatedCancellable && randomBoolean(),
            randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId(),
            Map.of()
        );
        final TaskResult originalResult = new TaskResult(randomBoolean(), original);
        final TaskResult relocatedResult = new TaskResult(randomBoolean(), relocated);
        final EffectiveReindexTask task = new EffectiveReindexTask(originalResult, relocatedResult);

        assertThat(task.latestTask(), equalTo(relocatedResult));
        assertThat(task.description(), equalTo(original.description()));
        assertThat(task.startTimeMillis(), equalTo(original.startTime()));
        final long expectedRunningTimeNanos = relocated.runningTimeNanos() + TimeUnit.MILLISECONDS.toNanos(
            relocated.startTime() - original.startTime()
        );
        assertThat(task.runningTimeNanos(), equalTo(expectedRunningTimeNanos));
        assertThat(task.latestTaskId(), equalTo(relocatedTaskId));
        assertThat(task.originalTaskId(), equalTo(originalTaskId));
        assertThat(task.isCompleted(), equalTo(relocatedResult.isCompleted()));
        assertThat(task.isCancelled(), equalTo(relocated.cancelled()));
        assertNull(task.error());
        assertNull(task.response());
        assertThat(task.relocatedTask(), equalTo(Optional.of(relocatedResult)));
        assertThat(task.original(), equalTo(originalResult));
    }

    public void testWithLatestRelocatedTask() {
        final TaskResult original = randomTaskResult();
        final TaskResult relocated = randomTaskResult();
        final EffectiveReindexTask task = new EffectiveReindexTask(original, null);

        final EffectiveReindexTask updated = task.withLatestRelocatedTask(relocated);

        assertThat(updated.original(), equalTo(original));
        assertThat(updated.relocatedTask(), equalTo(Optional.of(relocated)));
    }

    public void testWithUpdatedResultNoRelocated() {
        final TaskResult original = randomTaskResult();
        final TaskResult updatedResult = randomTaskResult();
        final EffectiveReindexTask task = new EffectiveReindexTask(original, null);

        final EffectiveReindexTask updated = task.withUpdatedResult(updatedResult);

        assertThat(updated.original(), equalTo(updatedResult));
        assertThat(updated.relocatedTask(), equalTo(Optional.empty()));
    }

    public void testWithUpdatedResultWithRelocated() {
        final TaskResult original = randomTaskResult();
        final TaskResult relocated = randomTaskResult();
        final TaskResult updatedResult = randomTaskResult();
        final EffectiveReindexTask task = new EffectiveReindexTask(original, relocated);

        final EffectiveReindexTask updated = task.withUpdatedResult(updatedResult);

        assertThat(updated.original(), equalTo(original));
        assertThat(updated.relocatedTask(), equalTo(Optional.of(updatedResult)));
    }

    public void testNullOriginalThrows() {
        expectThrows(NullPointerException.class, () -> new EffectiveReindexTask(null, null));
    }

    public void testWithLatestRelocatedTaskNullThrows() {
        final EffectiveReindexTask task = new EffectiveReindexTask(randomTaskResult(), null);
        expectThrows(NullPointerException.class, () -> task.withLatestRelocatedTask(null));
    }

    public void testWithUpdatedResultNullThrows() {
        final EffectiveReindexTask task = new EffectiveReindexTask(randomTaskResult(), null);
        expectThrows(NullPointerException.class, () -> task.withUpdatedResult(null));
    }

    private TaskResult randomTaskResult() {
        return new TaskResult(randomBoolean(), randomTaskInfo());
    }

    private TaskInfo randomTaskInfo() {
        TaskId taskId = randomTaskId();
        boolean cancellable = randomBoolean();
        return new TaskInfo(
            taskId,
            randomAlphaOfLength(10),
            taskId.getNodeId(),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            null,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            cancellable,
            cancellable && randomBoolean(),
            randomBoolean() ? TaskId.EMPTY_TASK_ID : randomTaskId(),
            Map.of()
        );
    }

    private TaskId randomTaskId() {
        return new TaskId(randomAlphaOfLength(10), randomNonNegativeLong());
    }
}
