/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.engine.RefreshThrottler.Request;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RefreshBurstableThrottlerTests extends ESTestCase {

    public void testGetIntervalNo() {
        var now = randomNonNegativeLong(); // Millis
        var throttler = new RefreshBurstableThrottler(request -> {}, TimeValue.timeValueSeconds(5), 1, 0, 10, () -> now, null);
        assertThat(throttler.getIntervalNo(now + randomLongBetween(0, 4999)), equalTo(0L));
        assertThat(throttler.getIntervalNo(now + randomLongBetween(5000, 9999)), equalTo(1L));
        assertThat(throttler.getIntervalNo(now + randomLongBetween(25000, 25999)), equalTo(5L));
    }

    public void testBasicUpdateCredit() {
        ThreadPool threadPool = mock(ThreadPool.class);
        long startTimeMillis = randomNonNegativeLong();
        AtomicLong timeMillis = new AtomicLong(startTimeMillis);
        AtomicLong refreshCalls = new AtomicLong();
        var throttler = new RefreshBurstableThrottler(
            request -> refreshCalls.incrementAndGet(),
            TimeValue.timeValueSeconds(5),
            1,
            1,
            25,
            timeMillis::get,
            threadPool
        );
        assertThat(throttler.getCredit(), equalTo(2L));  // the initial credit + 1
        assertFalse(throttler.maybeThrottle(new Request("api", ActionListener.noop())));
        assertThat(throttler.getCredit(), equalTo(1L));
        timeMillis.set(startTimeMillis + randomLongBetween(100_000, 104_999));  // Should add 20
        assertFalse(throttler.maybeThrottle(new Request("get", ActionListener.noop())));
        assertThat(throttler.getCredit(), equalTo(20L));
        assertFalse(throttler.maybeThrottle(new Request("get", ActionListener.noop())));
        assertThat(throttler.getCredit(), equalTo(19L));
        assertFalse(throttler.maybeThrottle(new Request("get", ActionListener.noop())));
        assertThat(throttler.getCredit(), equalTo(18L));
        timeMillis.set(startTimeMillis + randomLongBetween(200_000, 205_000));
        assertFalse(throttler.maybeThrottle(new Request("get", ActionListener.noop())));
        assertThat(throttler.getCredit(), equalTo(24L));
        assertThat(refreshCalls.get(), equalTo(5L));
        assertTrue(throttler.getThrottledPerSourceStats().isEmpty());
        assertThat(throttler.getAcceptedPerSourceStats().size(), equalTo(2));
        assertThat(throttler.getAcceptedPerSourceStats().get("api"), equalTo(1L));
        assertThat(throttler.getAcceptedPerSourceStats().get("get"), equalTo(4L));
        verify(threadPool, never()).scheduleUnlessShuttingDown(any(), any(), any());
    }

    public void testThrottle() {
        ThreadPool threadPool = mock(ThreadPool.class);
        long startTimeMillis = randomNonNegativeLong();
        AtomicLong timeMillis = new AtomicLong(startTimeMillis);
        AtomicLong refreshCalls = new AtomicLong();
        TimeValue throttlingInterval = TimeValue.timeValueSeconds(5);
        AtomicBoolean shouldRefresh = new AtomicBoolean(true);
        var throttler = new RefreshBurstableThrottler(request -> {
            refreshCalls.incrementAndGet();
            request.listener().onResponse(new Engine.RefreshResult(shouldRefresh.get()));
        }, throttlingInterval, 1, 0, 25, timeMillis::get, threadPool);
        timeMillis.addAndGet(randomLongBetween(0, 1000));
        // First one uses the only credit of the interval
        assertFalse(throttler.maybeThrottle(new Request("api", ActionListener.noop())));
        assertThat(refreshCalls.get(), equalTo(1L));
        assertThat(throttler.getCredit(), equalTo(0L));
        // Following three are throttled since they are in the same interval
        timeMillis.addAndGet(randomLongBetween(0, 1000));
        assertTrue(throttler.maybeThrottle(new Request("api", ActionListener.noop())));
        assertThat(throttler.getCredit(), equalTo(0L));
        timeMillis.addAndGet(randomLongBetween(0, 1000));
        assertTrue(throttler.maybeThrottle(new Request("get", ActionListener.noop())));
        assertThat(throttler.getCredit(), equalTo(0L));
        if (randomBoolean()) {
            timeMillis.addAndGet(randomLongBetween(0, 1000));
        }
        assertTrue(throttler.maybeThrottle(new Request("api", ActionListener.noop())));
        assertThat(throttler.getCredit(), equalTo(0L));
        ArgumentCaptor<Runnable> executorCaptor = ArgumentCaptor.forClass(Runnable.class);
        // There should only be one scheduled call to handle pending throttled reqeuests
        verify(threadPool, times(1)).scheduleUnlessShuttingDown(
            eq(throttlingInterval),
            matches(ThreadPool.Names.REFRESH),
            executorCaptor.capture()
        );
        // Manually call the schedule Runnable
        shouldRefresh.set(randomBoolean());
        timeMillis.addAndGet(throttlingInterval.millis());
        executorCaptor.getValue().run();
        assertThat(refreshCalls.get(), equalTo(2L));
        if (shouldRefresh.get()) {
            assertThat(throttler.getCredit(), equalTo(0L));
        } else {
            assertThat("unused credit due to noop refresh must be returned", throttler.getCredit(), equalTo(1L));
        }
        assertThat(throttler.getAcceptedPerSourceStats().size(), equalTo(1));
        assertThat(throttler.getAcceptedPerSourceStats().get("api"), equalTo(1L));
        assertThat(throttler.getThrottledPerSourceStats().size(), equalTo(2));
        assertThat(throttler.getThrottledPerSourceStats().get("api"), equalTo(2L));
        assertThat(throttler.getThrottledPerSourceStats().get("get"), equalTo(1L));
    }
}
