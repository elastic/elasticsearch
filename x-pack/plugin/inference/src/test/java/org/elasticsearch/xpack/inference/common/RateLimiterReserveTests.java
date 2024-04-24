/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.core.TimeValue;

import static org.hamcrest.Matchers.is;

public class RateLimiterReserveTests extends BaseRateLimiterTests {

    @Override
    protected TimeValue tokenMethod(RateLimiter limiter, int tokens) {
        return limiter.reserve(tokens);
    }

    @Override
    protected void sleepValidationMethod(
        TimeValue result,
        RateLimiter.Sleeper mockSleeper,
        int numberOfClassToExpect,
        long expectedMicrosecondsToSleep
    ) {
        assertThat(result.getMicros(), is(expectedMicrosecondsToSleep));
    }
}
