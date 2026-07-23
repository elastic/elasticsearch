/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class FillCacheMemoryPressureTests extends ESTestCase {

    /**
     * Grants run on the calling thread so that tests observe FIFO drain deterministically; production forks them to the generic pool.
     */
    private static FillCacheMemoryPressure pressureWithLimit(long limitBytes) {
        return new FillCacheMemoryPressure(
            Settings.builder().put(FillCacheMemoryPressure.FILL_BYTES_LIMIT.getKey(), ByteSizeValue.ofBytes(limitBytes)).build(),
            MeterRegistry.NOOP,
            Runnable::run
        );
    }

    private static ActionListener<Releasable> collectTo(List<Releasable> granted) {
        return ActionListener.wrap(granted::add, e -> fail(e, "unexpected failure"));
    }

    public void testGrantsImmediatelyWithinLimit() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(60, collectTo(granted));
        pressure.acquire(40, collectTo(granted));
        assertThat(granted, hasSize(2));
        assertThat(pressure.getCurrentBytes(), equalTo(100L));
        assertThat(pressure.getWaiterCount(), equalTo(0));
        granted.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testQueuesWhenOverLimitAndDrainsFifoOnRelease() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(80, collectTo(granted));
        assertThat(granted, hasSize(1));

        List<String> grantOrder = new CopyOnWriteArrayList<>();
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(50, ActionListener.wrap(r -> {
            grantOrder.add("first");
            queuedGrants.add(r);
        }, e -> fail(e, "unexpected failure")));
        pressure.acquire(30, ActionListener.wrap(r -> {
            grantOrder.add("second");
            queuedGrants.add(r);
        }, e -> fail(e, "unexpected failure")));
        assertThat(grantOrder, empty());
        assertThat(pressure.getWaiterCount(), equalTo(2));

        // the second waiter (30 bytes) would fit alongside the 80 in flight, but must not jump the 50-byte head of the queue
        granted.get(0).close();
        assertThat(grantOrder, contains("first", "second"));
        assertThat(pressure.getWaiterCount(), equalTo(0));
        assertThat(pressure.getCurrentBytes(), equalTo(80L));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testLaterAcquirersQueueBehindExistingWaiters() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(90, collectTo(granted));
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(50, collectTo(queuedGrants));
        // 5 bytes would fit right now, but granting it would starve the 50-byte waiter at the head
        pressure.acquire(5, collectTo(queuedGrants));
        assertThat(queuedGrants, empty());
        assertThat(pressure.getWaiterCount(), equalTo(2));
        granted.get(0).close();
        assertThat(queuedGrants, hasSize(2));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testOversizedRequestGrantedWhenNothingInFlight() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        // larger than the whole limit: granted immediately because nothing is in flight
        pressure.acquire(500, collectTo(granted));
        assertThat(granted, hasSize(1));
        assertThat(pressure.getCurrentBytes(), equalTo(500L));

        // everything else waits until the oversized read completes
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(10, collectTo(queuedGrants));
        assertThat(queuedGrants, empty());
        granted.get(0).close();
        assertThat(queuedGrants, hasSize(1));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testOversizedWaiterGrantedOnceInFlightDrains() {
        var pressure = pressureWithLimit(100);
        List<Releasable> granted = new ArrayList<>();
        pressure.acquire(60, collectTo(granted));
        List<Releasable> queuedGrants = new CopyOnWriteArrayList<>();
        pressure.acquire(500, collectTo(queuedGrants));
        assertThat(queuedGrants, empty());
        granted.get(0).close();
        assertThat(queuedGrants, hasSize(1));
        assertThat(pressure.getCurrentBytes(), equalTo(500L));
        queuedGrants.forEach(Releasable::close);
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }

    public void testRandomizedAcquireReleaseNeverExceedsLimitAndFullyDrains() {
        final long limit = randomLongBetween(100, 1000);
        var pressure = pressureWithLimit(limit);
        List<Releasable> outstanding = new CopyOnWriteArrayList<>();
        int acquires = randomIntBetween(50, 200);
        for (int i = 0; i < acquires; i++) {
            long bytes = randomLongBetween(1, limit / 2);
            pressure.acquire(bytes, ActionListener.wrap(outstanding::add, e -> fail(e, "unexpected failure")));
            assertThat("in-flight bytes exceed limit", pressure.getCurrentBytes(), lessThanOrEqualTo(limit));
            if (outstanding.isEmpty() == false && randomBoolean()) {
                outstanding.remove(randomIntBetween(0, outstanding.size() - 1)).close();
                assertThat("in-flight bytes exceed limit", pressure.getCurrentBytes(), lessThanOrEqualTo(limit));
            }
        }
        // closing an outstanding grant can synchronously grant a waiter, which appends to the list; loop until it is truly drained
        while (outstanding.isEmpty() == false) {
            outstanding.remove(0).close();
        }
        assertThat(pressure.getWaiterCount(), equalTo(0));
        assertThat(pressure.getCurrentBytes(), equalTo(0L));
    }
}
