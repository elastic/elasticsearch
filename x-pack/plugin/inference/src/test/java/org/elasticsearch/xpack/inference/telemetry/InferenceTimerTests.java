/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.test.ESTestCase;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InferenceTimerTests extends ESTestCase {

    public void testElapsedMillis() {
        var expectedDuration = randomLongBetween(10, 300);

        var startTime = Instant.now();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(startTime).thenReturn(startTime.plus(expectedDuration, ChronoUnit.MILLIS));
        var timer = InferenceTimer.start(clock);

        assertThat(expectedDuration, is(timer.elapsedMillis()));
    }
}
