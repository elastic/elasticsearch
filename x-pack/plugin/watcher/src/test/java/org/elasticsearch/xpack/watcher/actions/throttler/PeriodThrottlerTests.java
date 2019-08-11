/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.throttler;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.actions.ActionStatus;
import org.elasticsearch.xpack.core.watcher.actions.throttler.PeriodThrottler;
import org.elasticsearch.xpack.core.watcher.actions.throttler.Throttler;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.core.watcher.watch.WatchStatus;

import java.time.Clock;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.elasticsearch.xpack.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PeriodThrottlerTests extends ESTestCase {
    public void testBelowPeriodSuccessful() throws Exception {
        TimeValue period = TimeValue.timeValueSeconds(randomIntBetween(2, 5));
        PeriodThrottler throttler = new PeriodThrottler(Clock.systemUTC(), period);

        WatchExecutionContext ctx = mockExecutionContext("_name", Payload.EMPTY);
        ActionStatus actionStatus = mock(ActionStatus.class);
        ZonedDateTime now = Clock.systemUTC().instant().atZone(ZoneOffset.UTC);
        when(actionStatus.lastSuccessfulExecution())
                .thenReturn(ActionStatus.Execution.successful(now.minusSeconds((int) period.seconds() - 1)));
        WatchStatus status = mock(WatchStatus.class);
        when(status.actionStatus("_action")).thenReturn(actionStatus);
        when(ctx.watch().status()).thenReturn(status);

        Throttler.Result result = throttler.throttle("_action", ctx);
        assertThat(result, notNullValue());
        assertThat(result.throttle(), is(true));
        assertThat(result.reason(), notNullValue());
        assertThat(result.reason(), startsWith("throttling interval is set to [" + period + "]"));
        assertThat(result.type(), is(Throttler.Type.PERIOD));
    }

    public void testAbovePeriod() throws Exception {
        TimeValue period = TimeValue.timeValueSeconds(randomIntBetween(2, 5));
        PeriodThrottler throttler = new PeriodThrottler(Clock.systemUTC(), period);

        WatchExecutionContext ctx = mockExecutionContext("_name", Payload.EMPTY);
        ActionStatus actionStatus = mock(ActionStatus.class);
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        when(actionStatus.lastSuccessfulExecution())
                .thenReturn(ActionStatus.Execution.successful(now.minusSeconds((int) period.seconds() + 1)));
        WatchStatus status = mock(WatchStatus.class);
        when(status.actionStatus("_action")).thenReturn(actionStatus);
        when(ctx.watch().status()).thenReturn(status);

        Throttler.Result result = throttler.throttle("_action", ctx);
        assertThat(result, notNullValue());
        assertThat(result.throttle(), is(false));
        assertThat(result.reason(), nullValue());
    }
}
