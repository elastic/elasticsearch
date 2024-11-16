/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.time;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.time.Instant;
import java.util.Arrays;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class RemainingTimeTests extends ESTestCase {
    public void testRemainingTime() {
        var remainingTime = RemainingTime.from(times(Instant.now(), Instant.now().plusSeconds(60)), TimeValue.timeValueSeconds(30));
        assertThat(remainingTime.get(), Matchers.greaterThan(TimeValue.ZERO));
        assertThat(remainingTime.get(), Matchers.equalTo(TimeValue.ZERO));
    }

    public void testRemainingTimeMaxValue() {
        var remainingTime = RemainingTime.from(
            times(Instant.now().minusSeconds(60), Instant.now().plusSeconds(60)),
            TimeValue.timeValueSeconds(30)
        );
        assertThat(remainingTime.get(), Matchers.equalTo(TimeValue.timeValueSeconds(30)));
        assertThat(remainingTime.get(), Matchers.equalTo(TimeValue.ZERO));
    }

    public void testMaxTime() {
        var remainingTime = RemainingTime.from(Instant::now, TimeValue.MAX_VALUE);
        assertThat(remainingTime.get(), Matchers.equalTo(TimeValue.MAX_VALUE));
    }

    // always add the first value, which is read when RemainingTime.from is called, then add the test values
    private Supplier<Instant> times(Instant... instants) {
        var startTime = Stream.of(Instant.now());
        return Stream.concat(startTime, Arrays.stream(instants)).iterator()::next;
    }
}
