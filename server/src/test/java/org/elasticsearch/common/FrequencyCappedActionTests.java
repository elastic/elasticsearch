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

import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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

    public void testRunIfDue() {
        final var lastExecuted = new AtomicLong();
        final var currentTime = new AtomicLong(System.currentTimeMillis());
        final TimeValue minimumInterval = TimeValue.timeValueSeconds(between(1, 300));
        final var action = mock(Runnable.class);

        // the first call should always run
        FrequencyCappedAction.runIfDue(currentTime::get, lastExecuted, minimumInterval, action);
        verify(action).run();
        reset(action);

        for (int i = 0; i < between(1, 10); i++) {
            // advance time less than the minimum interval, the action should not be run
            currentTime.addAndGet(randomLongBetween(0, minimumInterval.millis() - 1));
            FrequencyCappedAction.runIfDue(currentTime::get, lastExecuted, minimumInterval, action);
            verifyNoInteractions(action);

            // advance time past the minimum interval, the action should be run again
            currentTime.addAndGet(randomLongBetween(minimumInterval.millis(), minimumInterval.millis() * 2));
            FrequencyCappedAction.runIfDue(currentTime::get, lastExecuted, minimumInterval, action);
            verify(action).run();
            reset(action);
        }
    }
}
