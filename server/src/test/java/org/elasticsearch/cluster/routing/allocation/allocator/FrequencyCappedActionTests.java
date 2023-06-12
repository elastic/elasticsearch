/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.equalTo;

public class FrequencyCappedActionTests extends ESTestCase {

    public void testFrequencyCapExecution() {

        var executions = new AtomicLong(0);
        var currentTime = new AtomicLong();
        var action = new FrequencyCappedAction(currentTime::get);

        var minInterval = timeValueMillis(randomNonNegativeInt());
        action.setMinInterval(minInterval);

        // initial execution should happen
        action.maybeExecute(executions::incrementAndGet);
        assertThat(executions.get(), equalTo(1L));

        // should not execute again too soon
        currentTime.set(randomLongBetween(0, minInterval.millis() - 1));
        action.maybeExecute(executions::incrementAndGet);
        assertThat(executions.get(), equalTo(1L));

        // should execute min interval elapsed
        currentTime.set(randomLongBetween(minInterval.millis(), Long.MAX_VALUE));
        action.maybeExecute(executions::incrementAndGet);
        assertThat(executions.get(), equalTo(2L));
    }
}
