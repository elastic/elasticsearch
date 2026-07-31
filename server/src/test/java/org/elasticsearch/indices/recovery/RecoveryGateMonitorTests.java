/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.indices.recovery.RecoveryGate.Decision;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class RecoveryGateMonitorTests extends ESTestCase {

    public void testEmptyMayRun() {
        final var monitor = newMonitor(new DeterministicTaskQueue(), List.of());
        assertTrue(monitor.evaluate().mayRun());
    }

    public void testAllRunMayRun() {
        final List<RecoveryGate> gateList = new ArrayList<>();
        for (int i = between(1, 5); i > 0; i--) {
            gateList.add(() -> Decision.RUN);
        }
        assertTrue(newMonitor(new DeterministicTaskQueue(), gateList).evaluate().mayRun());
    }

    public void testAnyBlockReturnsBlockingGateDecision() {
        final String gateName = randomIdentifier();
        final String reason = randomAlphaOfLengthBetween(5, 30);
        final List<RecoveryGate> gateList = new ArrayList<>();
        // The single blocking gate wins regardless of how many may-run gates surround it.
        for (int i = between(0, 3); i > 0; i--) {
            gateList.add(() -> Decision.RUN);
        }
        gateList.add(() -> Decision.block(gateName, reason));
        for (int i = between(0, 3); i > 0; i--) {
            gateList.add(() -> Decision.RUN);
        }

        final Decision decision = newMonitor(new DeterministicTaskQueue(), gateList).evaluate();
        assertFalse(decision.mayRun());
        assertThat(decision.gateName(), equalTo(gateName));
        assertThat(decision.reason(), equalTo(reason));
    }

    public void testShortCircuitsOnFirstBlock() {
        final List<RecoveryGate> gateList = new ArrayList<>();
        gateList.add(() -> Decision.block(randomIdentifier(), randomAlphaOfLengthBetween(5, 30)));
        gateList.add(() -> { throw new AssertionError("gates after a node-wide block must not be evaluated"); });
        assertFalse(newMonitor(new DeterministicTaskQueue(), gateList).evaluate().mayRun());
    }

    public void testGatesResolvedOnceOnFirstUse() {
        final AtomicInteger resolutions = new AtomicInteger();
        final var monitor = new RecoveryGateMonitor(() -> {
            resolutions.incrementAndGet();
            return List.of(() -> Decision.RUN);
        }, new DeterministicTaskQueue().getThreadPool());

        assertThat("supplier must not be resolved at construction", resolutions.get(), equalTo(0));
        for (int i = between(1, 3); i > 0; i--) {
            assertTrue(monitor.evaluate().mayRun());
        }
        assertThat(resolutions.get(), equalTo(1));
    }

    public void testListenerFiredWhenAwaitedOutcomeReached() {
        final var taskQueue = new DeterministicTaskQueue();
        final var decision = new AtomicReference<>(Decision.block(randomIdentifier(), randomAlphaOfLengthBetween(5, 30)));
        final var monitor = newMonitor(taskQueue, decision);
        assertFalse(monitor.evaluate().mayRun());
        assertFalse("no recheck without a waiting listener", taskQueue.hasDeferredTasks());

        final AtomicInteger fired = new AtomicInteger();
        monitor.addListener(RecoveryGate.Outcome.RUN, "same-name", fired::incrementAndGet);
        monitor.addListener(RecoveryGate.Outcome.RUN, "same-name", fired::incrementAndGet); // same name: no-op
        final AtomicInteger otherFired = new AtomicInteger();
        monitor.addListener(RecoveryGate.Outcome.RUN, "other-name", otherFired::incrementAndGet);
        assertTrue("registering a waiting listener starts the recheck", taskQueue.hasDeferredTasks());

        // Rechecks while still blocked do not fire the listeners.
        for (int i = between(0, 3); i > 0; i--) {
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            assertTrue("recheck must keep rescheduling while listeners wait", taskQueue.hasDeferredTasks());
        }
        assertThat(fired.get(), equalTo(0));
        assertThat(otherFired.get(), equalTo(0));

        // The gate unblocks: the next recheck fires each registered name exactly once and stops, since no listener remains.
        decision.set(Decision.RUN);
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(fired.get(), equalTo(1));
        assertThat(otherFired.get(), equalTo(1));
        assertFalse("recheck must stop once no listener remains", taskQueue.hasDeferredTasks());
    }

    public void testListenerFiredInlineWhenAlreadyAtAwaitedOutcome() {
        final var taskQueue = new DeterministicTaskQueue();
        final var monitor = newMonitor(taskQueue, new AtomicReference<>(Decision.RUN));

        // Registering for an outcome the decision already has fires inline, so a caller racing with a transition cannot miss it.
        final AtomicInteger fired = new AtomicInteger();
        monitor.addListener(RecoveryGate.Outcome.RUN, randomIdentifier(), fired::incrementAndGet);
        assertThat(fired.get(), equalTo(1));
        assertFalse("no recheck needed for an inline-fired listener", taskQueue.hasDeferredTasks());
    }

    private static RecoveryGateMonitor newMonitor(DeterministicTaskQueue taskQueue, AtomicReference<Decision> decision) {
        return newMonitor(taskQueue, List.of(decision::get));
    }

    private static RecoveryGateMonitor newMonitor(DeterministicTaskQueue taskQueue, List<RecoveryGate> gateList) {
        return new RecoveryGateMonitor(() -> gateList, taskQueue.getThreadPool());
    }
}
