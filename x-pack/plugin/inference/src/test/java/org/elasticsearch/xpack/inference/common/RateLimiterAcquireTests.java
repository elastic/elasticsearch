/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.core.TimeValue;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RateLimiterAcquireTests extends RateLimiter2Tests {

    @Override
    protected TimeValue tokenMethod(RateLimiter limiter, int tokens) throws InterruptedException {
        limiter.acquire(tokens);
        return null;
    }

    @Override
    protected void validationMethod(TimeValue result, RateLimiter.Sleeper mockSleeper, int numberOfClassToExpect, long expectedSleepTime)
        throws InterruptedException {
        verify(mockSleeper, times(numberOfClassToExpect)).sleep(expectedSleepTime);
    }
}
