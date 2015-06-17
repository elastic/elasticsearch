/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.throttler;

import org.joda.time.DateTime;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.actions.ActionStatus;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.watch.WatchStatus;
import org.junit.Test;

import static org.elasticsearch.watcher.support.WatcherDateTimeUtils.formatDate;
import static org.elasticsearch.watcher.test.WatcherTestUtils.EMPTY_PAYLOAD;
import static org.elasticsearch.watcher.test.WatcherTestUtils.mockExecutionContext;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class AckThrottlerTests extends ElasticsearchTestCase {

    @Test
    public void testWhenAcked() throws Exception {
        DateTime timestamp = new DateTime();
        WatchExecutionContext ctx = mockExecutionContext("_watch", EMPTY_PAYLOAD);
        Watch watch = ctx.watch();
        ActionStatus actionStatus = mock(ActionStatus.class);
        when(actionStatus.ackStatus()).thenReturn(new ActionStatus.AckStatus(timestamp, ActionStatus.AckStatus.State.ACKED));
        WatchStatus watchStatus = mock(WatchStatus.class);
        when(watchStatus.actionStatus("_action")).thenReturn(actionStatus);
        when(watch.status()).thenReturn(watchStatus);
        AckThrottler throttler = new AckThrottler();
        Throttler.Result result = throttler.throttle("_action", ctx);
        assertThat(result.throttle(), is(true));
        assertThat(result.reason(), is("action [_action] was acked at [" + formatDate(timestamp) + "]"));
    }

    @Test
    public void testThrottle_When_AwaitsSuccessfulExecution() throws Exception {
        DateTime timestamp = new DateTime();
        WatchExecutionContext ctx = mockExecutionContext("_watch", EMPTY_PAYLOAD);
        Watch watch = ctx.watch();
        ActionStatus actionStatus = mock(ActionStatus.class);
        when(actionStatus.ackStatus()).thenReturn(new ActionStatus.AckStatus(timestamp, ActionStatus.AckStatus.State.AWAITS_SUCCESSFUL_EXECUTION));
        WatchStatus watchStatus = mock(WatchStatus.class);
        when(watchStatus.actionStatus("_action")).thenReturn(actionStatus);
        when(watch.status()).thenReturn(watchStatus);
        AckThrottler throttler = new AckThrottler();
        Throttler.Result result = throttler.throttle("_action", ctx);
        assertThat(result.throttle(), is(false));
        assertThat(result.reason(), nullValue());
    }

    @Test
    public void testThrottle_When_Ackable() throws Exception {
        DateTime timestamp = new DateTime();
        WatchExecutionContext ctx = mockExecutionContext("_watch", EMPTY_PAYLOAD);
        Watch watch = ctx.watch();
        ActionStatus actionStatus = mock(ActionStatus.class);
        when(actionStatus.ackStatus()).thenReturn(new ActionStatus.AckStatus(timestamp, ActionStatus.AckStatus.State.ACKABLE));
        WatchStatus watchStatus = mock(WatchStatus.class);
        when(watchStatus.actionStatus("_action")).thenReturn(actionStatus);
        when(watch.status()).thenReturn(watchStatus);
        AckThrottler throttler = new AckThrottler();
        Throttler.Result result = throttler.throttle("_action", ctx);
        assertThat(result.throttle(), is(false));
        assertThat(result.reason(), nullValue());
    }
}
