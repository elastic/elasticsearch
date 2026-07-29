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
import org.elasticsearch.indices.recovery.RecoveryGateMonitor.DecisionChangeListener;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
        }, () -> DecisionChangeListener.NOOP, new DeterministicTaskQueue().getThreadPool());

        assertThat("supplier must not be resolved at construction", resolutions.get(), equalTo(0));
        for (int i = between(1, 3); i > 0; i--) {
            assertTrue(monitor.check().mayRun());
        }
        assertThat(resolutions.get(), equalTo(1));
    }

    public void testCheckReportsTransitionsAndRechecksWithListener() {
        final var taskQueue = new DeterministicTaskQueue();
        final var previousDecision = new AtomicReference<Decision>();
        final var currentDecision = new AtomicReference<Decision>();
        final var reportedMillis = new AtomicLong(-1);
        final var changeCount = new AtomicInteger();
        final var onChangeResolutions = new AtomicInteger();
        final var decision = new AtomicReference<>(Decision.RUN);
        final var monitor = new RecoveryGateMonitor(() -> List.of(decision::get), () -> {
            onChangeResolutions.incrementAndGet();
            return (previous, current, durationMillis) -> {
                previousDecision.set(previous);
                currentDecision.set(current);
                reportedMillis.set(durationMillis);
                changeCount.incrementAndGet();
            };
        }, taskQueue.getThreadPool());

        // May-run: no transition, no report, no recheck scheduled, and the change listener is not resolved yet.
        assertTrue(monitor.check().mayRun());
        assertThat(changeCount.get(), equalTo(0));
        assertThat("change listener must not be resolved before the first transition", onChangeResolutions.get(), equalTo(0));
        assertFalse(taskQueue.hasAnyTasks());

        // Flips to block: reported once. Repeated checks while blocked do not re-report, and with no listener waiting there is no
        // periodic recheck.
        final String gateName = randomIdentifier();
        final String reason = randomAlphaOfLengthBetween(5, 30);
        decision.set(Decision.block(gateName, reason));
        final long blockedSince = taskQueue.getCurrentTimeMillis();
        for (int i = between(1, 100); i > 0; i--) {
            assertFalse(monitor.check().mayRun());
        }
        assertThat(changeCount.get(), equalTo(1));
        assertTrue(previousDecision.get().mayRun());
        assertFalse(currentDecision.get().mayRun());
        assertThat(currentDecision.get().gateName(), equalTo(gateName));
        assertThat(currentDecision.get().reason(), equalTo(reason));
        assertFalse("no recheck without a waiting listener", taskQueue.hasAnyTasks());

        // A listener awaiting RUN starts the periodic recheck.
        monitor.addListener(RecoveryGate.Outcome.RUN, "test-listener", () -> {});
        assertTrue(taskQueue.hasDeferredTasks());

        // Rechecks while still blocked change nothing.
        for (int i = between(0, 100); i > 0; i--) {
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
        }
        assertThat(changeCount.get(), equalTo(1));

        // The gate unblocks: the next recheck notices it without any external call, reports the blocked duration, and stops itself
        // because no listener remains.
        decision.set(Decision.RUN);
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(changeCount.get(), equalTo(2));
        assertThat(previousDecision.get().gateName(), equalTo(gateName));
        assertTrue(currentDecision.get().mayRun());
        assertThat(reportedMillis.get(), equalTo(taskQueue.getCurrentTimeMillis() - blockedSince));
        assertThat("change listener resolved exactly once", onChangeResolutions.get(), equalTo(1));
        assertFalse("recheck must stop once no listener remains", taskQueue.hasDeferredTasks());
    }

    public void testListenerFiredWhenAwaitedOutcomeReached() {
        final var taskQueue = new DeterministicTaskQueue();
        final var decision = new AtomicReference<>(Decision.block(randomIdentifier(), randomAlphaOfLengthBetween(5, 30)));
        final var monitor = newMonitor(taskQueue, decision);
        assertFalse(monitor.check().mayRun());
        assertFalse("no recheck without a waiting listener", taskQueue.hasDeferredTasks());

        final AtomicInteger fired = new AtomicInteger();
        monitor.addListener(RecoveryGate.Outcome.RUN, "same-name", fired::incrementAndGet);
        monitor.addListener(RecoveryGate.Outcome.RUN, "same-name", fired::incrementAndGet); // same name: no-op
        final AtomicInteger otherFired = new AtomicInteger();
        monitor.addListener(RecoveryGate.Outcome.RUN, "other-name", otherFired::incrementAndGet);
        assertTrue("registering a waiting listener starts the recheck", taskQueue.hasDeferredTasks());

        // Rechecks while still blocked do not fire the listeners.
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
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
        final var monitor = newMonitor(new DeterministicTaskQueue(), new AtomicReference<>(Decision.RUN));
        assertTrue(monitor.check().mayRun());

        // Registering for an outcome the decision already has fires inline, so a caller racing with a transition cannot miss it.
        final AtomicInteger fired = new AtomicInteger();
        monitor.addListener(RecoveryGate.Outcome.RUN, randomIdentifier(), fired::incrementAndGet);
        assertThat(fired.get(), equalTo(1));
    }

    private static RecoveryGateMonitor newMonitor(DeterministicTaskQueue taskQueue, AtomicReference<Decision> decision) {
        return newMonitor(taskQueue, List.of(decision::get));
    }

    private static RecoveryGateMonitor newMonitor(DeterministicTaskQueue taskQueue, List<RecoveryGate> gateList) {
        return new RecoveryGateMonitor(() -> gateList, () -> DecisionChangeListener.NOOP, taskQueue.getThreadPool());
    }
}
