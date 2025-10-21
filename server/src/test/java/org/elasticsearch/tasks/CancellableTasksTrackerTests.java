/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class CancellableTasksTrackerTests extends ESTestCase {

    private static class TestTask {
        private final Thread actionThread;
        private final Thread watchThread;
        private final Thread concurrentRemoveThread;

        // 0 == before put, 1 == during put, 2 == after put, before remove, 3 == during remove, 4 == after remove
        private final AtomicInteger state = new AtomicInteger();
        private final boolean concurrentRemove = randomBoolean();
        private final long requestId = randomIntBetween(-1, 10);

        TestTask(Task task, String item, CancellableTasksTracker<String> tracker, CyclicBarrier startBarrier) {
            if (concurrentRemove) {
                concurrentRemoveThread = new Thread(() -> {
                    safeAwait(startBarrier);
                    for (int i = 0; i < 10; i++) {
                        if (3 <= state.get()) {
                            final String removed = tracker.remove(task);
                            if (removed != null) {
                                assertSame(item, removed);
                            }
                        }
                    }
                });
            } else {
                concurrentRemoveThread = new Thread(() -> safeAwait(startBarrier));
            }

            actionThread = new Thread(() -> {
                safeAwait(startBarrier);

                state.incrementAndGet();
                tracker.put(task, requestId, item);
                state.incrementAndGet();

                Thread.yield();

                state.incrementAndGet();
                final String removed = tracker.remove(task);
                state.incrementAndGet();
                if (concurrentRemove == false || removed != null) {
                    assertSame(item, removed);
                }

                assertNull(tracker.remove(task));
            }, "action-thread-" + item);

            watchThread = new Thread(() -> {
                safeAwait(startBarrier);

                for (int i = 0; i < 10; i++) {
                    final int stateBefore = state.get();
                    final String getResult = tracker.get(task.getId());
                    final Set<String> getByParentResult = tracker.getByParent(task.getParentTaskId()).collect(Collectors.toSet());
                    final Set<String> getByChildrenResult = tracker.getChildrenByRequestId(task.getParentTaskId(), requestId)
                        .collect(Collectors.toSet());
                    final Set<String> values = new HashSet<>(tracker.values());
                    final int stateAfter = state.get();

                    assertThat(stateBefore, lessThanOrEqualTo(stateAfter));

                    if (getResult != null && task.getParentTaskId().isSet() && tracker.get(task.getId()) != null) {
                        assertThat(getByParentResult, hasItem(item));
                        assertThat(getByChildrenResult, hasItem(item));
                    }

                    if (stateAfter == 0) {
                        assertNull(getResult);
                        assertThat(getByParentResult, not(hasItem(item)));
                        assertThat(getByChildrenResult, not(hasItem(item)));
                        assertThat(values, not(hasItem(item)));
                    }

                    if (stateBefore == 2 && stateAfter == 2) {
                        assertSame(item, getResult);
                        if (task.getParentTaskId().isSet()) {
                            assertThat(getByParentResult, hasItem(item));
                            assertThat(getByChildrenResult, hasItem(item));
                        } else {
                            assertThat(getByParentResult, empty());
                            assertThat(getByChildrenResult, empty());
                        }
                        assertThat(values, hasItem(item));
                    }

                    if (stateBefore == 4) {
                        assertNull(getResult);
                        if (concurrentRemove == false) {
                            assertThat(getByParentResult, not(hasItem(item)));
                            assertThat(getByChildrenResult, not(hasItem(item)));
                        } // else our remove might have completed but the concurrent one hasn't updated the parent ID map yet
                        assertThat(values, not(hasItem(item)));
                    }
                }
            }, "watch-thread-" + item);
        }

        void start() {
            watchThread.start();
            concurrentRemoveThread.start();
            actionThread.start();
        }

        void join() throws InterruptedException {
            actionThread.join();
            concurrentRemoveThread.join();
            watchThread.join();
        }
    }

    public void testCancellableTasksTracker() throws InterruptedException {

        final TaskId[] parentTaskIds = randomArray(
            10,
            10,
            TaskId[]::new,
            () -> new TaskId(randomAlphaOfLength(5), randomNonNegativeLong())
        );

        final CancellableTasksTracker<String> tracker = new CancellableTasksTracker<>();
        final TestTask[] tasks = new TestTask[between(1, 100)];
        final CyclicBarrier startBarrier = new CyclicBarrier(tasks.length * 3);

        for (int i = 0; i < tasks.length; i++) {
            tasks[i] = new TestTask(
                new Task(
                    randomNonNegativeLong(),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    randomAlphaOfLength(5),
                    rarely() ? TaskId.EMPTY_TASK_ID : randomFrom(parentTaskIds),
                    Collections.emptyMap()
                ),
                "item-" + i,
                tracker,
                startBarrier
            );
        }

        for (TestTask task : tasks) {
            task.start();
        }

        for (TestTask task : tasks) {
            task.join();
        }

        tracker.assertConsistent();
    }
}
