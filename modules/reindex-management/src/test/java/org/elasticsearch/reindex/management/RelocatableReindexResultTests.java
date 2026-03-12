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

public class RelocatableReindexResultTests extends AbstractWireSerializingTestCase<RelocatableReindexResult> {

    @Override
    protected Writeable.Reader<RelocatableReindexResult> instanceReader() {
        return RelocatableReindexResult::new;
    }

    @Override
    protected RelocatableReindexResult createTestInstance() {
        TaskResult original = randomTaskResult();
        TaskResult relocated = randomBoolean() ? randomTaskResult() : null;
        return new RelocatableReindexResult(original, relocated);
    }

    @Override
    protected RelocatableReindexResult mutateInstance(RelocatableReindexResult instance) {
        if (instance.relocated() == null) {
            return switch (between(0, 1)) {
                case 0 -> new RelocatableReindexResult(randomValueOtherThan(instance.original(), this::randomTaskResult), null);
                case 1 -> new RelocatableReindexResult(instance.original(), randomTaskResult());
                default -> throw new AssertionError("invalid switch");
            };
        } else {
            return switch (between(0, 2)) {
                case 0 -> new RelocatableReindexResult(
                    randomValueOtherThan(instance.original(), this::randomTaskResult),
                    instance.relocated()
                );
                case 1 -> new RelocatableReindexResult(
                    instance.original(),
                    randomValueOtherThan(instance.relocated(), this::randomTaskResult)
                );
                case 2 -> new RelocatableReindexResult(instance.original(), null);
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
        final TaskResult taskResult = new TaskResult(randomBoolean(), info);
        final RelocatableReindexResult result = new RelocatableReindexResult(taskResult, null);

        assertThat(result.latestTask(), equalTo(taskResult));
        assertNull(result.status());
        assertThat(result.description(), equalTo(info.description()));
        assertThat(result.startTimeMillis(), equalTo(info.startTime()));
        assertThat(result.runningTimeNanos(), equalTo(info.runningTimeNanos()));
        assertThat(result.latestTaskId(), equalTo(taskId));
        assertThat(result.originalTaskId(), equalTo(taskId));
        assertThat(result.isCompleted(), equalTo(taskResult.isCompleted()));
        assertThat(result.isCancelled(), equalTo(cancelled));
        assertNull(result.error());
        assertNull(result.response());
        assertThat(result.relocatedTask(), equalTo(Optional.empty()));
        assertThat(result.original(), equalTo(taskResult));
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
        final RelocatableReindexResult result = new RelocatableReindexResult(originalResult, relocatedResult);

        assertThat(result.latestTask(), equalTo(relocatedResult));
        assertThat(result.description(), equalTo(original.description()));
        assertThat(result.startTimeMillis(), equalTo(original.startTime()));
        final long expectedRunningTimeNanos = relocated.runningTimeNanos() + TimeUnit.MILLISECONDS.toNanos(
            relocated.startTime() - original.startTime()
        );
        assertThat(result.runningTimeNanos(), equalTo(expectedRunningTimeNanos));
        assertThat(result.latestTaskId(), equalTo(relocatedTaskId));
        assertThat(result.originalTaskId(), equalTo(originalTaskId));
        assertThat(result.isCompleted(), equalTo(relocatedResult.isCompleted()));
        assertThat(result.isCancelled(), equalTo(relocated.cancelled()));
        assertNull(result.error());
        assertNull(result.response());
        assertThat(result.relocatedTask(), equalTo(Optional.of(relocatedResult)));
        assertThat(result.original(), equalTo(originalResult));
    }

    public void testWithLatestRelocatedTask() {
        final TaskResult original = randomTaskResult();
        final TaskResult relocated = randomTaskResult();
        final RelocatableReindexResult result = new RelocatableReindexResult(original, null);

        final RelocatableReindexResult updated = result.withLatestRelocatedTask(relocated);

        assertThat(updated.original(), equalTo(original));
        assertThat(updated.relocatedTask(), equalTo(Optional.of(relocated)));
    }

    public void testWithUpdatedResultNoRelocated() {
        final TaskResult original = randomTaskResult();
        final TaskResult updatedResult = randomTaskResult();
        final RelocatableReindexResult result = new RelocatableReindexResult(original, null);

        final RelocatableReindexResult updated = result.withUpdatedResult(updatedResult);

        assertThat(updated.original(), equalTo(updatedResult));
        assertThat(updated.relocatedTask(), equalTo(Optional.empty()));
    }

    public void testWithUpdatedResultWithRelocated() {
        final TaskResult original = randomTaskResult();
        final TaskResult relocated = randomTaskResult();
        final TaskResult updatedResult = randomTaskResult();
        final RelocatableReindexResult result = new RelocatableReindexResult(original, relocated);

        final RelocatableReindexResult updated = result.withUpdatedResult(updatedResult);

        assertThat(updated.original(), equalTo(original));
        assertThat(updated.relocatedTask(), equalTo(Optional.of(updatedResult)));
    }

    public void testNullOriginalThrows() {
        expectThrows(NullPointerException.class, () -> new RelocatableReindexResult(null, null));
    }

    public void testWithLatestRelocatedTaskNullThrows() {
        final RelocatableReindexResult result = new RelocatableReindexResult(randomTaskResult(), null);
        expectThrows(NullPointerException.class, () -> result.withLatestRelocatedTask(null));
    }

    public void testWithUpdatedResultNullThrows() {
        final RelocatableReindexResult result = new RelocatableReindexResult(randomTaskResult(), null);
        expectThrows(NullPointerException.class, () -> result.withUpdatedResult(null));
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
