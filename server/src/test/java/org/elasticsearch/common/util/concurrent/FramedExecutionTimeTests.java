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

import static org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor.FramedExecutionTime;

public class FramedExecutionTimeTests extends ESTestCase {

    private final FramedExecutionTime framedExecutionTime;
    private final FakeTime fakeTime;

    public FramedExecutionTimeTests() {
        fakeTime = new FakeTime();
        framedExecutionTime = new FramedExecutionTime(1L, fakeTime);
    }

    public void testNoTasks() {
        framedExecutionTime.updateFrame();
        assertEquals(0, framedExecutionTime.getPreviousFrameExecutionTime());
        fakeTime.time += between(1, 100);
        assertEquals(0, framedExecutionTime.getPreviousFrameExecutionTime());
    }

    public void testSingleFrameTask() {
        framedExecutionTime.interval = 100;
        framedExecutionTime.startTask(10);
        framedExecutionTime.endTask(20);
        fakeTime.time += framedExecutionTime.interval;
        assertEquals(10, framedExecutionTime.getPreviousFrameExecutionTime());
    }

    public void testTwoFrameTask() {
        framedExecutionTime.interval = 100;
        var startTime = between(0, 100);
        var endTime = startTime + framedExecutionTime.interval;
        framedExecutionTime.startTask(startTime);
        framedExecutionTime.endTask(endTime);
        assertEquals(framedExecutionTime.interval - startTime, framedExecutionTime.getPreviousFrameExecutionTime());
    }

    public void testMultiFrameTask() {
        framedExecutionTime.interval = 10;
        framedExecutionTime.startTask(1);
        framedExecutionTime.endTask(between(3, 100) * 10L);
        assertEquals(framedExecutionTime.interval, framedExecutionTime.getPreviousFrameExecutionTime());
    }

    public void testOngoingTask() {
        framedExecutionTime.interval = 10;
        framedExecutionTime.startTask(0);
        for (int i = 0; i < between(10, 100); i++) {
            fakeTime.time += framedExecutionTime.interval;
            assertEquals(framedExecutionTime.interval, framedExecutionTime.getPreviousFrameExecutionTime());
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
