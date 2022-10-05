/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RunningTimeRecorderTests extends ESTestCase {
    public void testTotalOperationTimeIsRecorded() {
        final AtomicLong clock = new AtomicLong();
        final RunningTimeRecorder runningTimeRecorder = new RunningTimeRecorder(clock::get);

        final Releasable tracker = runningTimeRecorder.trackRunningTime();

        // Advance the clock 100ns
        final long totalTimeSinceStart = 100;
        clock.set(totalTimeSinceStart);

        tracker.close();

        assertThat(runningTimeRecorder.totalRunningTimeInNanos(), equalTo(totalTimeSinceStart));
    }

    public void testTotalRunningTimeIncludesElapsedTimeFromRunningOperations() {
        final AtomicLong clock = new AtomicLong();
        final RunningTimeRecorder runningTimeRecorder = new RunningTimeRecorder(clock::get);

        final Releasable tracker = runningTimeRecorder.trackRunningTime();

        // Advance the clock 100ns
        clock.set(100);

        // even though the operation is still running, we get a reading
        assertThat(runningTimeRecorder.totalRunningTimeInNanos(), equalTo(100L));

        // Advance the clock 100ns
        clock.addAndGet(100);

        tracker.close();

        assertThat(runningTimeRecorder.totalRunningTimeInNanos(), equalTo(200L));
    }

    public void testTotalRunningTimeFromMultipleOperationsGetsAccumulated() {
        final AtomicLong clock = new AtomicLong();
        final RunningTimeRecorder runningTimeRecorder = new RunningTimeRecorder(clock::get);

        final List<Releasable> runningOps = randomList(2, 5, runningTimeRecorder::trackRunningTime);

        // Advance the clock 100ns
        clock.addAndGet(100);

        // even though the operation is still running, we get a reading
        assertThat(runningTimeRecorder.totalRunningTimeInNanos(), equalTo(100L * runningOps.size()));

        // Advance the clock 100ns
        clock.addAndGet(100);

        runningOps.forEach(Releasable::close);

        assertThat(runningTimeRecorder.totalRunningTimeInNanos(), equalTo(200L * runningOps.size()));
    }

    public void testConcurrentCloseAndTotalRunningTimeInNanosCallsDoesNotOverCount() throws Exception {
        final AtomicLong clock = new AtomicLong();
        final RunningTimeRecorder runningTimeRecorder = new RunningTimeRecorder(clock::get);
        final List<Releasable> runningOps = randomList(20, 30, runningTimeRecorder::trackRunningTime);

        clock.set(100);

        final List<Thread> closingThreads = new ArrayList<>(runningOps.size());
        final List<Thread> readingThreads = new ArrayList<>(runningOps.size());

        final CyclicBarrier barrier = new CyclicBarrier(runningOps.size() * 2);
        final CountDownLatch finishLatch = new CountDownLatch(runningOps.size() * 2);
        for (Releasable runningOp : runningOps) {
            closingThreads.add(new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                runningOp.close();
                finishLatch.countDown();
            }));
            readingThreads.add(new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                runningTimeRecorder.totalRunningTimeInNanos();
                finishLatch.countDown();
            }));
        }

        closingThreads.forEach(Thread::start);
        readingThreads.forEach(Thread::start);

        finishLatch.await();
        assertThat(runningTimeRecorder.totalRunningTimeInNanos(), is(equalTo(100L * runningOps.size())));
    }
}
