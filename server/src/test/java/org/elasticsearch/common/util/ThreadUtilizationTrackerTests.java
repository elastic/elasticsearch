/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class ThreadUtilizationTrackerTests extends ESTestCase {

    public void testPollingUtilizationCalculation() {
        // Use a controlled time supplier that can be manipulated.
        AtomicLong currentTime = new AtomicLong(0);
        LongAdder executionTimeAdder = new LongAdder();
        int numThreads = 4;

        ThreadUtilizationTracker tracker = new ThreadUtilizationTracker(() -> currentTime.get(), executionTimeAdder, numThreads);

        // First poll at time=0 should return 1.0 (when no time has passed, tracker returns 1.0).
        currentTime.set(0L);
        double utilization = tracker.pollUtilization();
        assertEquals(1.0, utilization, 0.0001);

        // Advance time by 1000ns, add 2000ns of execution time (for 4 threads, maximum execution time is 4000ns).
        // Expected utilization: 2000 / 4000 = 0.5 (50%)
        currentTime.set(1000L);
        executionTimeAdder.add(2000L);
        utilization = tracker.pollUtilization();
        assertEquals(0.5, utilization, 0.0001);

        // Advance time by 2000ns, add 8000ns of execution time (for 4 threads, maximum execution time is 8000ns).
        // Expected utilization: 8000 / 8000 = 1.0 (100%)
        currentTime.set(3000L);
        executionTimeAdder.add(8000L);
        utilization = tracker.pollUtilization();
        assertEquals(1.0, utilization, 0.0001);

        // Advance time by 5000ns, add 5000ns of execution time (for 4 threads, maximum execution time is 20000ns).
        // Expected utilization: 5000 / 20000 = 0.25 (25%)
        currentTime.set(8000L);
        executionTimeAdder.add(5000L);
        utilization = tracker.pollUtilization();
        assertEquals(0.25, utilization, 0.0001);

        // Advance time by 1000ns, add 0ns of execution time (for 4 threads, maximum execution time is 4000ns).
        // Expected utilization: 0 / 4000 = 0.0 (0%)
        currentTime.set(9000L);
        utilization = tracker.pollUtilization();
        assertEquals(0.0, utilization, 0.0001);

        // Advance time by 500ns, add 250ns of execution time (for 4 threads, maximum execution time is 2000ns).
        // Expected utilization: 250 / 2000 = 0.125 (12.5%)
        currentTime.set(9500L);
        executionTimeAdder.add(250L);
        utilization = tracker.pollUtilization();
        assertEquals(0.125, utilization, 0.0001);

        // Advance time by 0ns, so no time has passed. Verify that 1.0 is returned (when no time has passed, tracker returns 1.0).
        currentTime.set(9500L);
        utilization = tracker.pollUtilization();
        assertEquals(1.0, utilization, 0.0001);
    }
}
