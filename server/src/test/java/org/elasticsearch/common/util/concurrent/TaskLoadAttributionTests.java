/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.TaskLoadAttribution.SHORT_NANOS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * Drives the task of a pool thread as its pool does, with clocks the tests set, to check the CPU time each charge gets.
 */
public class TaskLoadAttributionTests extends ESTestCase {

    /** Long enough for its CPU time to be read. */
    private static final long TASK_NANOS = 2 * SHORT_NANOS;

    public void testChargesTheCpuTimeTheThreadUsed() {
        onPoolThread(clocks -> {
            Charges charges = new Charges();
            TaskLoadAttribution.taskStarted();
            TaskLoadAttribution.attribute(charges);
            clocks.cpu += 300;
            assertThat(TaskLoadAttribution.taskCompleted(TASK_NANOS, clocks.now), equalTo(TASK_NANOS));
            assertThat(charges, contains(new Charge(TASK_NANOS, 300)));
        });
    }

    public void testCpuTimeIsAtMostTheTime() {
        onPoolThread(clocks -> {
            Charges charges = new Charges();
            TaskLoadAttribution.taskStarted();
            TaskLoadAttribution.attribute(charges);
            clocks.cpu += 2 * TASK_NANOS;
            TaskLoadAttribution.taskCompleted(TASK_NANOS, clocks.now);
            assertThat(charges, contains(new Charge(TASK_NANOS, TASK_NANOS)));
        });
    }

    /**
     * A part of the task charged elsewhere gets the CPU time the thread used until then, and the pool charges the rest of the
     * task with the CPU time used since.
     */
    public void testChargesAPartWithItsOwnCpuTime() {
        onPoolThread(clocks -> {
            Charges task = new Charges();
            Charges part = new Charges();
            TaskLoadAttribution.taskStarted();
            TaskLoadAttribution.attribute(task);
            clocks.cpu += 100;
            TaskLoadAttribution.charge(part, TASK_NANOS, clocks.now);
            clocks.cpu += 200;
            TaskLoadAttribution.taskCompleted(3 * TASK_NANOS, clocks.now);
            assertThat(part, contains(new Charge(TASK_NANOS, 100)));
            assertThat(task, contains(new Charge(2 * TASK_NANOS, 200)));
        });
    }

    /**
     * A part started at the end of the task gets only the CPU time used in it, and the task keeps what it used before.
     */
    public void testPartAtTheEndLeavesTheTaskItsCpuTime() {
        onPoolThread(clocks -> {
            Charges task = new Charges();
            Charges part = new Charges();
            TaskLoadAttribution.taskStarted();
            TaskLoadAttribution.attribute(task);
            clocks.cpu += 300;
            TaskLoadAttribution.startPart();
            clocks.cpu += 100;
            TaskLoadAttribution.charge(part, TASK_NANOS, clocks.now);
            clocks.cpu += 50;
            TaskLoadAttribution.taskCompleted(3 * TASK_NANOS, clocks.now);
            assertThat(part, contains(new Charge(TASK_NANOS, 100)));
            assertThat(task, contains(new Charge(2 * TASK_NANOS, 350)));
        });
    }

    /**
     * Time too short to be worth a read is charged as CPU time, and the next read charges only the CPU time it did not cover.
     */
    public void testChargesShortTimeAsCpuTimeWithoutReading() {
        onPoolThread(clocks -> {
            Charges task = new Charges();
            Charges part = new Charges();
            TaskLoadAttribution.taskStarted();
            TaskLoadAttribution.attribute(task);
            assertThat(clocks.reads, equalTo(1));
            clocks.cpu += 10;
            TaskLoadAttribution.charge(part, SHORT_NANOS - 1, clocks.now);
            assertThat(clocks.reads, equalTo(1));
            clocks.cpu += SHORT_NANOS;
            TaskLoadAttribution.taskCompleted(3 * SHORT_NANOS, clocks.now);
            assertThat(clocks.reads, equalTo(2));
            assertThat(part, contains(new Charge(SHORT_NANOS - 1, SHORT_NANOS - 1)));
            assertThat(task, contains(new Charge(2 * SHORT_NANOS + 1, 11)));
        });
    }

    /**
     * A task reuses the read of the previous task, however long the thread waited in between.
     */
    public void testReusesTheReadOfThePreviousTask() {
        onPoolThread(clocks -> {
            Charges charges = new Charges();
            runTask(clocks, charges, 300);
            assertThat(clocks.reads, equalTo(2));

            clocks.now += TimeUnit.MINUTES.toNanos(1);
            runTask(clocks, charges, 300);
            assertThat(clocks.reads, equalTo(3));
            assertThat(charges, contains(new Charge(TASK_NANOS, 300), new Charge(TASK_NANOS, 300)));
        });
    }

    /**
     * A task with nothing to charge does not read the CPU time, so the next task reads it again rather than charging what that
     * task used.
     */
    public void testTaskWithoutTargetDoesNotReadAndForgetsTheRead() {
        onPoolThread(clocks -> {
            Charges charges = new Charges();
            runTask(clocks, charges, 300);
            int reads = clocks.reads;
            TaskLoadAttribution.taskStarted();
            clocks.cpu += 5000;
            assertThat(TaskLoadAttribution.taskCompleted(TASK_NANOS, clocks.now), equalTo(0L));
            assertThat(clocks.reads, equalTo(reads));
            runTask(clocks, charges, 300);
            assertThat(charges, contains(new Charge(TASK_NANOS, 300), new Charge(TASK_NANOS, 300)));
        });
    }

    /**
     * Where the CPU time of the thread is not known, as while it is disabled, the time counts as CPU time.
     */
    public void testUnknownCpuTimeCountsAsTheTime() {
        onPoolThread(clocks -> {
            clocks.cpu = -1;
            Charges charges = new Charges();
            runTask(clocks, charges, 0);
            assertThat(charges, contains(new Charge(TASK_NANOS, TASK_NANOS)));
        });
    }

    /**
     * A target that throws is charged once, although the pool completes the task again with the failure.
     */
    public void testTargetThatThrowsIsChargedOnce() {
        onPoolThread(clocks -> {
            Charges charges = new Charges();
            TaskLoadAttribution.Target throwing = (nanos, cpuNanos, finishNanos) -> {
                charges.charge(nanos, cpuNanos, finishNanos);
                throw new IllegalStateException("boom");
            };
            TaskLoadAttribution.taskStarted();
            TaskLoadAttribution.attribute(throwing);
            clocks.cpu += 300;
            expectThrows(IllegalStateException.class, () -> TaskLoadAttribution.taskCompleted(TASK_NANOS, clocks.now));
            assertThat(TaskLoadAttribution.taskCompleted(TASK_NANOS, clocks.now), equalTo(0L));
            assertThat(charges, contains(new Charge(TASK_NANOS, 300)));
        });
    }

    /**
     * Outside a pool task nothing measures the CPU time, so the time counts as CPU time.
     */
    public void testOutsideAPoolTaskTheTimeCountsAsCpuTime() {
        Charges charges = new Charges();
        TaskLoadAttribution.charge(charges, 100, System.nanoTime());
        assertThat(charges, contains(new Charge(100, 100)));
    }

    /**
     * Runs a task of {@link #TASK_NANOS} that uses {@code cpu} of CPU time, charged to {@code charges}.
     */
    private static void runTask(Clocks clocks, Charges charges, long cpu) {
        TaskLoadAttribution.taskStarted();
        TaskLoadAttribution.attribute(charges);
        if (clocks.cpu >= 0) {
            clocks.cpu += cpu;
        }
        clocks.now += TASK_NANOS;
        TaskLoadAttribution.taskCompleted(TASK_NANOS, clocks.now);
    }

    /**
     * Runs {@code test} on a thread of the kind pools run their tasks on, with clocks it sets.
     */
    private static void onPoolThread(CheckedConsumer test) {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = EsExecutors.daemonThreadFactory("test", "load").newThread(() -> {
            try {
                Clocks clocks = new Clocks();
                TaskLoadAttribution.Task task = ((EsExecutors.EsThread) Thread.currentThread()).loadTask;
                task.cpuClock = () -> {
                    clocks.reads++;
                    return clocks.cpu;
                };
                test.accept(clocks);
            } catch (Throwable t) {
                failure.set(t);
            }
        });
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
    }

    @FunctionalInterface
    private interface CheckedConsumer {
        void accept(Clocks clocks) throws Exception;
    }

    private static final class Clocks {
        long cpu = 1_000_000;
        long now = 1_000_000;
        int reads;
    }

    private record Charge(long nanos, long cpuNanos) {}

    private static final class Charges extends ArrayList<Charge> implements TaskLoadAttribution.Target {
        @Override
        public void charge(long nanos, long cpuNanos, long finishNanos) {
            add(new Charge(nanos, cpuNanos));
        }
    }
}
