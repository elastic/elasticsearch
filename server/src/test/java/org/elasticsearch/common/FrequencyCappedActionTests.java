/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.equalTo;

public class FrequencyCappedActionTests extends ESTestCase {

    public void testFrequencyCapExecution() {

        var executions = new AtomicLong(0);
        var currentTime = new AtomicLong();
        final TimeValue initialDelay = randomBoolean() ? TimeValue.ZERO : TimeValue.timeValueSeconds(between(1, 300));
        var action = new FrequencyCappedAction(currentTime::get, initialDelay);

        var minInterval = timeValueMillis(randomNonNegativeInt());
        action.setMinInterval(minInterval);

        action.maybeExecute(executions::incrementAndGet);
        if (initialDelay != TimeValue.ZERO) {
            // Not executing due to initial delay
            assertThat(executions.get(), equalTo(0L));
            currentTime.addAndGet(randomLongBetween(initialDelay.millis(), initialDelay.millis() * 2));
            action.maybeExecute(executions::incrementAndGet);
        }
        // initial execution should happen
        assertThat(executions.get(), equalTo(1L));

        // should not execute again too soon
        currentTime.addAndGet(randomLongBetween(0, minInterval.millis() - 1));
        action.maybeExecute(executions::incrementAndGet);
        assertThat(executions.get(), equalTo(1L));

        // should execute min interval elapsed
        currentTime.addAndGet(randomLongBetween(minInterval.millis(), Long.MAX_VALUE));
        action.maybeExecute(executions::incrementAndGet);
        assertThat(executions.get(), equalTo(2L));
    }

    public void testMinIntervalFromConstructor() {
        var executions = new AtomicLong(0);
        var currentTime = new AtomicLong();
        var minInterval = TimeValue.timeValueMinutes(1);
        var action = new FrequencyCappedAction(currentTime::get, TimeValue.ZERO, minInterval);

        action.maybeExecute(executions::incrementAndGet);
        assertThat(executions.get(), equalTo(1L));

        currentTime.set(minInterval.millis() - 1);
        action.maybeExecute(executions::incrementAndGet);
        assertThat(executions.get(), equalTo(1L));

        currentTime.set(minInterval.millis());
        action.maybeExecute(executions::incrementAndGet);
        assertThat(executions.get(), equalTo(2L));
    }

    public void testConcurrentCallersExecuteOncePerInterval() {
        final int threads = between(2, 10);
        var executions = new AtomicLong(0);
        var currentTime = new AtomicLong();
        var contend = new AtomicBoolean();
        var barrier = new CyclicBarrier(threads);
        var minInterval = TimeValue.timeValueMinutes(1);
        // Every caller reads the clock before any of them tries to claim the execution, so that they all see the same deadline
        var action = new FrequencyCappedAction(() -> {
            if (contend.get()) {
                safeAwait(barrier);
            }
            return currentTime.get();
        }, TimeValue.ZERO, minInterval);
        contend.set(true);

        final int rounds = between(20, 100);
        for (int round = 1; round <= rounds; round++) {
            startInParallel(threads, i -> action.maybeExecute(executions::incrementAndGet));
            assertThat(executions.get(), equalTo((long) round));
            currentTime.addAndGet(minInterval.millis());
        }
    }
}
