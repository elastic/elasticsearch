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
import org.junit.Before;

import java.util.function.Supplier;

import static java.util.stream.IntStream.range;
import static org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor.FramedTimeTracker;

public class FramedTimeTrackerTests extends ESTestCase {

    private FakeTime fakeTime;

    FramedTimeTracker newTracker(long interval) {
        return new FramedTimeTracker(interval, fakeTime);
    }

    @Before
    public void setup() {
        fakeTime = new FakeTime();
    }

    public void testNoTasks() {
        var tracker = newTracker(1);
        tracker.previousFrameTime();
        assertEquals(0, tracker.previousFrameTime());
        fakeTime.time += between(1, 100);
        assertEquals(0, tracker.previousFrameTime());
    }

    public void testSingleFrameTask() {
        var tracker = newTracker(100);
        fakeTime.time += 10;
        tracker.startTask();
        fakeTime.time += 10;
        tracker.endTask();
        fakeTime.time += tracker.interval;
        assertEquals(10, tracker.previousFrameTime());
    }

    public void testTwoFrameTask() {
        var tracker = newTracker(100);
        var startTime = between(0, 100);
        var taskDuration = tracker.interval;
        fakeTime.time += startTime;
        tracker.startTask();
        fakeTime.time += taskDuration;
        tracker.endTask();
        assertEquals(tracker.interval - startTime, tracker.previousFrameTime());
    }

    public void testMultiFrameTask() {
        var interval = 10;
        var tracker = newTracker(interval);
        tracker.startTask();
        var taskDuration = between(3, 100) * interval;
        fakeTime.time += taskDuration;
        tracker.endTask();
        assertEquals(tracker.interval, tracker.previousFrameTime());
    }

    public void testOngoingTask() {
        var interval = 10;
        var tracker = newTracker(interval);
        tracker.startTask();
        for (int i = 0; i < between(10, 100); i++) {
            fakeTime.time += tracker.interval;
            assertEquals(tracker.interval, tracker.previousFrameTime());
        }
    }

    public void testMultipleTasks() {
        var interval = between(1, 100) * 2; // using integer division by 2 below
        var tracker = newTracker(interval);
        var halfIntervalTasks = between(1, 10);
        var notEndingTasks = between(1, 10);

        range(0, halfIntervalTasks + notEndingTasks).forEach(t -> tracker.startTask());
        fakeTime.time += interval / 2;
        range(0, halfIntervalTasks).forEach(t -> tracker.endTask());
        fakeTime.time += interval / 2;
        var firstFrameTotalTime = interval * halfIntervalTasks / 2 + interval * notEndingTasks;
        assertEquals(firstFrameTotalTime, tracker.previousFrameTime());

        fakeTime.time += interval;
        var secondFrameTotalTime = interval * notEndingTasks;
        assertEquals(secondFrameTotalTime, tracker.previousFrameTime());
    }

    static class FakeTime implements Supplier<Long> {
        long time = 0;

        @Override
        public Long get() {
            return time;
        }
    }
}
