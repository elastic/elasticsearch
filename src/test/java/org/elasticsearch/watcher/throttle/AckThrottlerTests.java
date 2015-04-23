/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.throttle;

import org.elasticsearch.watcher.watch.Watch;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.elasticsearch.watcher.support.WatcherDateUtils.formatDate;
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
        Watch.Status status = mock(Watch.Status.class);
        when(status.ackStatus()).thenReturn(new Watch.Status.AckStatus(Watch.Status.AckStatus.State.ACKED, timestamp));
        when(watch.status()).thenReturn(status);
        when(watch.id()).thenReturn("_watch");
        when(watch.acked()).thenReturn(true);
        AckThrottler throttler = new AckThrottler();
        Throttler.Result result = throttler.throttle(ctx);
        assertThat(result.throttle(), is(true));
        assertThat(result.reason(), is("watch [_watch] was acked at [" + formatDate(timestamp) + "]"));
    }

    @Test
    public void testWhenNotAcked() throws Exception {
        DateTime timestamp = new DateTime();
        WatchExecutionContext ctx = mockExecutionContext("_watch", EMPTY_PAYLOAD);
        Watch watch = ctx.watch();
        Watch.Status status = mock(Watch.Status.class);
        Watch.Status.AckStatus.State state = randomFrom(Watch.Status.AckStatus.State.AWAITS_EXECUTION, Watch.Status.AckStatus.State.ACKABLE);
        when(status.ackStatus()).thenReturn(new Watch.Status.AckStatus(state, timestamp));
        when(watch.status()).thenReturn(status);
        when(watch.acked()).thenReturn(false);
        AckThrottler throttler = new AckThrottler();
        Throttler.Result result = throttler.throttle(ctx);
        assertThat(result.throttle(), is(false));
        assertThat(result.reason(), nullValue());
    }
}
