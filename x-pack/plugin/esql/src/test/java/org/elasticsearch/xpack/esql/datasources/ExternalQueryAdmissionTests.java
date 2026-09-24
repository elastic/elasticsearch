/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class ExternalQueryAdmissionTests extends ESTestCase {

    private final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
    private final ThreadPool threadPool = taskQueue.getThreadPool();

    private ExternalQueryAdmission gate(int maxConcurrent, int maxQueued, TimeValue timeout) {
        return new ExternalQueryAdmission(threadPool, threadPool.generic(), maxConcurrent, maxQueued, timeout);
    }

    /** Records how one acquire ended. */
    private static class Outcome implements ActionListener<Releasable> {
        final AtomicReference<Releasable> slot = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final AtomicBoolean cancelled = new AtomicBoolean();

        @Override
        public void onResponse(Releasable releasable) {
            assertTrue("completed twice", slot.compareAndSet(null, releasable) && failure.get() == null);
        }

        @Override
        public void onFailure(Exception e) {
            assertTrue("completed twice", failure.compareAndSet(null, e) && slot.get() == null);
        }

        boolean admitted() {
            return slot.get() != null;
        }
    }

    private Outcome acquire(ExternalQueryAdmission gate) {
        Outcome outcome = new Outcome();
        gate.acquire(outcome.cancelled::get, outcome);
        return outcome;
    }

    public void testAdmitsUpToLimitOnTheCallingThread() {
        ExternalQueryAdmission gate = gate(2, 4, TimeValue.timeValueSeconds(30));
        Outcome first = acquire(gate);
        Outcome second = acquire(gate);
        assertTrue(first.admitted());
        assertTrue(second.admitted());
        assertFalse("admitted without forking", taskQueue.hasRunnableTasks());
        assertThat(gate.running(), equalTo(2));
    }

    public void testQueuedQueriesAreAdmittedInArrivalOrderAsSlotsFree() {
        ExternalQueryAdmission gate = gate(1, 3, TimeValue.timeValueSeconds(30));
        Outcome running = acquire(gate);
        List<Integer> order = new ArrayList<>();
        List<Outcome> waiting = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            int id = i;
            Outcome outcome = new Outcome() {
                @Override
                public void onResponse(Releasable releasable) {
                    super.onResponse(releasable);
                    order.add(id);
                }
            };
            gate.acquire(outcome.cancelled::get, outcome);
            waiting.add(outcome);
        }
        assertThat(gate.queued(), equalTo(3));

        running.slot.get().close();
        taskQueue.runAllRunnableTasks();
        assertTrue(waiting.get(0).admitted());
        assertFalse(waiting.get(1).admitted());
        assertThat(gate.running(), equalTo(1));

        waiting.get(0).slot.get().close();
        taskQueue.runAllRunnableTasks();
        waiting.get(1).slot.get().close();
        taskQueue.runAllRunnableTasks();
        assertThat(order, contains(0, 1, 2));
        assertThat(gate.queued(), equalTo(0));
    }

    public void testFullQueueIsRefusedWithoutWaiting() {
        ExternalQueryAdmission gate = gate(1, 1, TimeValue.timeValueSeconds(30));
        acquire(gate);
        acquire(gate);
        Outcome refused = acquire(gate);
        assertThat(refused.failure.get(), instanceOf(EsRejectedExecutionException.class));
        assertThat(refused.failure.get().getMessage(), containsString(ExternalSourceSettings.ADMISSION_MAX_CONCURRENT_QUERIES.getKey()));
        assertThat(refused.failure.get().getMessage(), containsString(ExternalSourceSettings.ADMISSION_MAX_QUEUED_QUERIES.getKey()));
        assertThat(gate.refusedQueueFull(), equalTo(1L));
    }

    public void testZeroQueueRefusesAsSoonAsSlotsAreTaken() {
        ExternalQueryAdmission gate = gate(1, 0, TimeValue.timeValueSeconds(30));
        acquire(gate);
        assertThat(acquire(gate).failure.get(), instanceOf(EsRejectedExecutionException.class));
    }

    public void testWaitingPastTheTimeoutIsRefused() {
        ExternalQueryAdmission gate = gate(1, 4, TimeValue.timeValueSeconds(5));
        Outcome holder = acquire(gate);
        long queuedAt = taskQueue.getCurrentTimeMillis();
        Outcome waiter = acquire(gate);
        while (waiter.failure.get() == null) {
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }
        assertThat(waiter.failure.get(), instanceOf(EsRejectedExecutionException.class));
        assertThat(waiter.failure.get().getMessage(), containsString("waited"));
        assertThat(taskQueue.getCurrentTimeMillis() - queuedAt, equalTo(5000L));
        assertThat(gate.queued(), equalTo(0));
        assertThat(gate.refusedTimeout(), equalTo(1L));
        holder.slot.get().close();
        assertThat(gate.running(), equalTo(0));
    }

    public void testCancelledQueryLeavesTheQueue() {
        ExternalQueryAdmission gate = gate(1, 4, TimeValue.timeValueSeconds(30));
        Outcome holder = acquire(gate);
        Outcome waiter = acquire(gate);
        waiter.cancelled.set(true);
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(waiter.failure.get(), instanceOf(TaskCancelledException.class));
        assertThat(gate.queued(), equalTo(0));
        holder.slot.get().close();
        assertThat(gate.running(), equalTo(0));
    }

    public void testClosingASlotTwiceReturnsItOnce() {
        ExternalQueryAdmission gate = gate(2, 0, TimeValue.timeValueSeconds(30));
        Outcome first = acquire(gate);
        acquire(gate);
        first.slot.get().close();
        first.slot.get().close();
        assertThat(gate.running(), equalTo(1));
    }

    public void testLimitZeroAdmitsEverythingUncounted() {
        ExternalQueryAdmission gate = gate(0, 0, TimeValue.timeValueSeconds(30));
        List<Outcome> outcomes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            outcomes.add(acquire(gate));
        }
        outcomes.forEach(o -> assertTrue(o.admitted()));
        assertThat(gate.running(), equalTo(0));

        // Turning the gate back on counts only slots handed out while it is on; closing the uncounted ones changes nothing.
        gate.setMaxConcurrentQueries(1);
        Outcome counted = acquire(gate);
        assertTrue(counted.admitted());
        outcomes.forEach(o -> o.slot.get().close());
        assertThat(gate.running(), equalTo(1));
    }

    public void testTurningTheGateOffAdmitsEveryoneWaiting() {
        ExternalQueryAdmission gate = gate(1, 4, TimeValue.timeValueSeconds(30));
        Outcome holder = acquire(gate);
        Outcome a = acquire(gate);
        Outcome b = acquire(gate);
        gate.setMaxConcurrentQueries(0);
        taskQueue.runAllRunnableTasks();
        assertTrue(a.admitted());
        assertTrue(b.admitted());
        assertThat(gate.running(), equalTo(1));
        holder.slot.get().close();
        a.slot.get().close();
        b.slot.get().close();
        assertThat(gate.running(), equalTo(0));
    }

    public void testRaisingTheLimitAdmitsWaitingQueries() {
        ExternalQueryAdmission gate = gate(1, 4, TimeValue.timeValueSeconds(30));
        acquire(gate);
        Outcome waiter = acquire(gate);
        gate.setMaxConcurrentQueries(2);
        taskQueue.runAllRunnableTasks();
        assertTrue(waiter.admitted());
        assertThat(gate.running(), equalTo(2));
    }

    public void testLoweringTheLimitCancelsNothingAndHoldsNewArrivals() {
        ExternalQueryAdmission gate = gate(3, 4, TimeValue.timeValueSeconds(30));
        Outcome a = acquire(gate);
        Outcome b = acquire(gate);
        acquire(gate);
        gate.setMaxConcurrentQueries(1);
        assertThat(gate.running(), equalTo(3));
        Outcome waiter = acquire(gate);
        assertThat(gate.queued(), equalTo(1));

        a.slot.get().close();
        b.slot.get().close();
        taskQueue.runAllRunnableTasks();
        assertFalse("still at the lowered limit", waiter.admitted());
    }

    public void testPromotedWaiterKeepsTheSlotAgainstANewArrival() {
        // The freed slot goes to the waiter at the head of the queue under the gate's lock, before its continuation even runs,
        // so a query arriving in between cannot take it.
        ExternalQueryAdmission gate = gate(1, 4, TimeValue.timeValueSeconds(30));
        Outcome holder = acquire(gate);
        Outcome waiter = acquire(gate);
        holder.slot.get().close();
        // waiter now holds the slot, resuming on the executor.
        Outcome arrival = acquire(gate);
        taskQueue.runAllRunnableTasks();
        assertTrue(waiter.admitted());
        assertFalse(arrival.admitted());
        assertThat(arrival.failure.get(), nullValue());
    }

    public void testResumedQueryKeepsItsThreadContext() {
        ExternalQueryAdmission gate = gate(1, 4, TimeValue.timeValueSeconds(30));
        Outcome holder = acquire(gate);
        AtomicReference<String> seen = new AtomicReference<>();
        try (var ignored = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader("who", "the-waiting-query");
            gate.acquire(() -> false, ActionListener.wrap(slot -> seen.set(threadPool.getThreadContext().getHeader("who")), e -> {
                throw new AssertionError(e);
            }));
        }
        try (var ignored = threadPool.getThreadContext().stashContext()) {
            threadPool.getThreadContext().putHeader("who", "the-releasing-query");
            holder.slot.get().close();
        }
        taskQueue.runAllRunnableTasks();
        assertThat(seen.get(), equalTo("the-waiting-query"));
    }

    public void testManyWaitersExpireInOneSweepThatThenStops() {
        ExternalQueryAdmission gate = gate(1, 1000, TimeValue.timeValueSeconds(5));
        Outcome holder = acquire(gate);
        List<Outcome> waiters = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            waiters.add(acquire(gate));
        }
        assertTrue(gate.sweepScheduled());
        while (waiters.get(999).failure.get() == null) {
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }
        waiters.forEach(w -> assertThat(w.failure.get(), instanceOf(EsRejectedExecutionException.class)));
        assertThat(gate.refusedTimeout(), equalTo(1000L));
        assertFalse("the sweep stops once nobody waits", gate.sweepScheduled());
        assertFalse("nothing left scheduled", taskQueue.hasDeferredTasks());
        holder.slot.get().close();
    }

    public void testSweepStopsWhenTheLastWaiterIsAdmitted() {
        ExternalQueryAdmission gate = gate(1, 4, TimeValue.timeValueSeconds(30));
        Outcome holder = acquire(gate);
        acquire(gate);
        assertTrue(gate.sweepScheduled());
        holder.slot.get().close();
        assertFalse(gate.sweepScheduled());
    }

    public void testZeroTimeoutRefusesAsSoonAsSlotsAreTaken() {
        ExternalQueryAdmission gate = gate(1, 4, TimeValue.ZERO);
        acquire(gate);
        assertThat(acquire(gate).failure.get(), instanceOf(EsRejectedExecutionException.class));
        assertThat(gate.queued(), equalTo(0));
    }

    public void testRejectedResumeGivesTheSlotBack() {
        EsRejectedExecutionException poolFull = new EsRejectedExecutionException("pool full", false);
        Executor rejecting = command -> { throw poolFull; };
        ExternalQueryAdmission gate = new ExternalQueryAdmission(threadPool, rejecting, 1, 4, TimeValue.timeValueSeconds(30));
        Outcome holder = acquire(gate);
        Outcome waiter = acquire(gate);
        holder.slot.get().close();
        assertThat(waiter.failure.get(), equalTo(poolFull));
        assertThat(gate.running(), equalTo(0));
    }

    public void testUnlimitedIgnoresSettingChanges() {
        ExternalQueryAdmission gate = ExternalQueryAdmission.unlimited();
        gate.setMaxConcurrentQueries(1);
        gate.setMaxQueuedQueries(1);
        gate.setQueueTimeout(TimeValue.timeValueSeconds(1));
        for (int i = 0; i < 5; i++) {
            assertTrue(acquire(gate).admitted());
        }
        assertThat(gate.running(), equalTo(0));
    }

    public void testMetricsReportTheGatesCounts() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        ExternalQueryAdmission gate = gate(1, 1, TimeValue.timeValueSeconds(30));
        gate.registerMetrics(registry);
        acquire(gate);
        acquire(gate);
        acquire(gate);
        registry.getRecorder().collect();
        assertThat(lastValue(registry, InstrumentType.LONG_ASYNC_GAUGE, ExternalQueryAdmission.RUNNING_CURRENT), equalTo(1L));
        assertThat(lastValue(registry, InstrumentType.LONG_ASYNC_GAUGE, ExternalQueryAdmission.QUEUED_CURRENT), equalTo(1L));
        assertThat(lastValue(registry, InstrumentType.LONG_ASYNC_COUNTER, ExternalQueryAdmission.ADMITTED_TOTAL), equalTo(1L));
        List<Measurement> refused = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_COUNTER, ExternalQueryAdmission.REFUSED_TOTAL);
        assertThat(
            refused.stream()
                .filter(m -> "queue_full".equals(m.attributes().get(ExternalQueryAdmission.REASON_ATTRIBUTE)))
                .mapToLong(Measurement::getLong)
                .max()
                .orElseThrow(),
            equalTo(1L)
        );
    }

    private static long lastValue(RecordingMeterRegistry registry, InstrumentType type, String name) {
        List<Measurement> measurements = registry.getRecorder().getMeasurements(type, name);
        assertFalse(name + " was not recorded", measurements.isEmpty());
        return measurements.get(measurements.size() - 1).getLong();
    }

    public void testDefaultConcurrencyFollowsHeap() {
        long requestLimit60Percent;
        long gib = ByteSizeValue.ofGb(1).getBytes();
        requestLimit60Percent = (long) (gib * 0.6);
        assertThat(ExternalSourceSettings.defaultMaxConcurrentDatasetQueries(gib, requestLimit60Percent), equalTo(4));
        assertThat(ExternalSourceSettings.defaultMaxConcurrentDatasetQueries(2 * gib, 2 * requestLimit60Percent), equalTo(8));
        assertThat(ExternalSourceSettings.defaultMaxConcurrentDatasetQueries(4 * gib, 4 * requestLimit60Percent), equalTo(16));
        assertThat(ExternalSourceSettings.defaultMaxConcurrentDatasetQueries(gib / 4, requestLimit60Percent / 4), equalTo(2));
        assertThat(ExternalSourceSettings.defaultMaxConcurrentDatasetQueries(64 * gib, 64 * requestLimit60Percent), equalTo(64));
        // A tight request breaker binds before the heap quarter.
        assertThat(ExternalSourceSettings.defaultMaxConcurrentDatasetQueries(4 * gib, gib / 2), equalTo(4));
    }
}
