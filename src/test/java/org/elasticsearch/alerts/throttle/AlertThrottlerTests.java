/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.throttle;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class AlertThrottlerTests extends ElasticsearchTestCase {

    @Test
    public void testThrottle_DueToAck() throws Exception {
        PeriodThrottler periodThrottler = mock(PeriodThrottler.class);
        AckThrottler ackThrottler = mock(AckThrottler.class);
        ExecutionContext ctx = mock(ExecutionContext.class);
        when(periodThrottler.throttle(ctx)).thenReturn(Throttler.Result.NO);
        Throttler.Result expectedResult = Throttler.Result.throttle("_reason");
        when(ackThrottler.throttle(ctx)).thenReturn(expectedResult);
        AlertThrottler throttler = new AlertThrottler(periodThrottler, ackThrottler);
        Throttler.Result result = throttler.throttle(ctx);
        assertThat(result, notNullValue());
        assertThat(result, is(expectedResult));
    }

    @Test
    public void testThrottle_DueToPeriod() throws Exception {
        PeriodThrottler periodThrottler = mock(PeriodThrottler.class);
        AckThrottler ackThrottler = mock(AckThrottler.class);
        ExecutionContext ctx = mock(ExecutionContext.class);
        Throttler.Result expectedResult = Throttler.Result.throttle("_reason");
        when(periodThrottler.throttle(ctx)).thenReturn(expectedResult);
        when(ackThrottler.throttle(ctx)).thenReturn(Throttler.Result.NO);
        AlertThrottler throttler = new AlertThrottler(periodThrottler, ackThrottler);
        Throttler.Result result = throttler.throttle(ctx);
        assertThat(result, notNullValue());
        assertThat(result, is(expectedResult));
    }

    @Test
    public void testThrottle_DueBoth() throws Exception {
        PeriodThrottler periodThrottler = mock(PeriodThrottler.class);
        AckThrottler ackThrottler = mock(AckThrottler.class);
        ExecutionContext ctx = mock(ExecutionContext.class);
        Throttler.Result periodResult = Throttler.Result.throttle("_reason_period");
        when(periodThrottler.throttle(ctx)).thenReturn(periodResult);
        Throttler.Result ackResult = Throttler.Result.throttle("_reason_ack");
        when(ackThrottler.throttle(ctx)).thenReturn(ackResult);
        AlertThrottler throttler = new AlertThrottler(periodThrottler, ackThrottler);
        Throttler.Result result = throttler.throttle(ctx);
        assertThat(result, notNullValue());
        // we always check the period first... so the result will come for the period throttler
        assertThat(result, is(periodResult));
    }

    @Test
    public void testNoThrottle() throws Exception {
        PeriodThrottler periodThrottler = mock(PeriodThrottler.class);
        AckThrottler ackThrottler = mock(AckThrottler.class);
        ExecutionContext ctx = mock(ExecutionContext.class);
        when(periodThrottler.throttle(ctx)).thenReturn(Throttler.Result.NO);
        when(ackThrottler.throttle(ctx)).thenReturn(Throttler.Result.NO);
        AlertThrottler throttler = new AlertThrottler(periodThrottler, ackThrottler);
        Throttler.Result result = throttler.throttle(ctx);
        assertThat(result, notNullValue());
        assertThat(result, is(Throttler.Result.NO));
    }

    @Test
    public void testWithoutPeriod() throws Exception {
        AckThrottler ackThrottler = mock(AckThrottler.class);
        ExecutionContext ctx = mock(ExecutionContext.class);
        Throttler.Result ackResult = mock(Throttler.Result.class);
        when(ackThrottler.throttle(ctx)).thenReturn(ackResult);
        AlertThrottler throttler = new AlertThrottler(null, ackThrottler);
        Throttler.Result result = throttler.throttle(ctx);
        assertThat(result, notNullValue());
        assertThat(result, sameInstance(ackResult));
    }
}
