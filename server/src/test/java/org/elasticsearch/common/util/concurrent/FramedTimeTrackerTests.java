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

import java.util.function.Supplier;

import static org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor.FramedTimeTracker;

public class FramedTimeTrackerTests extends ESTestCase {

    private final FramedTimeTracker framedTimeTracker;
    private final FakeTime fakeTime;

    public FramedTimeTrackerTests() {
        fakeTime = new FakeTime();
        framedTimeTracker = new FramedTimeTracker(1L, fakeTime);
    }

    public void testNoTasks() {
        framedTimeTracker.updateFrame();
        assertEquals(0, framedTimeTracker.previousFrameTime());
        fakeTime.time += between(1, 100);
        assertEquals(0, framedTimeTracker.previousFrameTime());
    }

    public void testSingleFrameTask() {
        framedTimeTracker.interval = 100;
        framedTimeTracker.startTask(10);
        framedTimeTracker.endTask(20);
        fakeTime.time += framedTimeTracker.interval;
        assertEquals(10, framedTimeTracker.previousFrameTime());
    }

    public void testTwoFrameTask() {
        framedTimeTracker.interval = 100;
        var startTime = between(0, 100);
        var endTime = startTime + framedTimeTracker.interval;
        framedTimeTracker.startTask(startTime);
        framedTimeTracker.endTask(endTime);
        assertEquals(framedTimeTracker.interval - startTime, framedTimeTracker.previousFrameTime());
    }

    public void testMultiFrameTask() {
        framedTimeTracker.interval = 10;
        framedTimeTracker.startTask(1);
        framedTimeTracker.endTask(between(3, 100) * 10L);
        assertEquals(framedTimeTracker.interval, framedTimeTracker.previousFrameTime());
    }

    public void testOngoingTask() {
        framedTimeTracker.interval = 10;
        framedTimeTracker.startTask(0);
        for (int i = 0; i < between(10, 100); i++) {
            fakeTime.time += framedTimeTracker.interval;
            assertEquals(framedTimeTracker.interval, framedTimeTracker.previousFrameTime());
        }
    }

    static class FakeTime implements Supplier<Long> {
        long time = 0;

        @Override
        public Long get() {
            return time;
        }
    }
}
