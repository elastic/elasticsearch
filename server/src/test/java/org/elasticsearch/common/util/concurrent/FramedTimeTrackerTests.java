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

    private final int frame = 10;
    private final int windowLen = 30;
    private final int window = windowLen * frame;

    /**
     * creates new tracker with frame interval=1 and and windowSize = 100 frames
     */
    FramedTimeTracker newTracker() {
        return new FramedTimeTracker(window, frame, fakeTime);
    }

    @Before
    public void setup() {
        fakeTime = new FakeTime();
    }

    public void testNoTasks() {
        var tracker = newTracker();
        assertEquals(0, tracker.totalTime());
        fakeTime.time += randomNonNegativeInt();
        assertEquals(0, tracker.totalTime());
    }

    public void testSingleWindow() {
        var tracker = newTracker();
        var startOffset = between(0, window / 2);
        fakeTime.time += startOffset;
        tracker.startTask();
        var taskDuration = between(0, window / 2);
        fakeTime.time += taskDuration;
        tracker.endTask();
        fakeTime.time += frame;
        assertEquals(taskDuration, tracker.totalTime());
    }

    public void testMultiWindow() {
        var tracker = newTracker();
        var startTime = between(0, frame);
        var taskDuration = between(1, 10) * window;
        fakeTime.time += startTime;
        tracker.startTask();
        fakeTime.time += taskDuration;
        tracker.endTask();
        fakeTime.time += frame;
        assertEquals("must run for the whole window, except last frame", window - (frame - startTime), (int) tracker.totalTime());
    }

    public void testOngoingTask() {
        var tracker = newTracker();
        tracker.startTask();
        // fill first window
        for (int i = 0; i < windowLen; i++) {
            assertEquals(frame * i, tracker.totalTime());
            fakeTime.time += frame;
        }
        // after first window is filled, it's always full then
        for (int i = 0; i < between(0, 1000); i++) {
            assertEquals(window, tracker.totalTime());
            fakeTime.time += frame;
        }
    }

    public void testMultipleTasks() {
        var tracker = newTracker();
        var halfWindowTasks = between(1, 10);
        var notEndingTasks = between(1, 10);

        range(0, halfWindowTasks + notEndingTasks).forEach(t -> tracker.startTask());
        fakeTime.time += window / 2;
        range(0, halfWindowTasks).forEach(t -> tracker.endTask());
        fakeTime.time += window / 2;
        var firstFrameTotalTime = window * halfWindowTasks / 2 + window * notEndingTasks;
        assertEquals(firstFrameTotalTime, tracker.totalTime());

        fakeTime.time += window;
        var secondFrameTotalTime = window * notEndingTasks;
        assertEquals(secondFrameTotalTime, tracker.totalTime());
    }

    static class FakeTime implements Supplier<Long> {
        long time = 0;

        @Override
        public Long get() {
            return time;
        }
    }
}
