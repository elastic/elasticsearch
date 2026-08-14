/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.indices.recovery.RecoveryGate.Decision;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.RecoveryGateMonitor.ENABLE_RECOVERY_GATES_SETTING;
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

    public void testThrowingGateIsIgnored() {
        final RecoveryGate throwingGate = () -> { throw new RuntimeException("simulated gate failure"); };

        // A throwing gate fails open: if every other gate allows recoveries (or it is the only gate), the decision is RUN.
        final List<RecoveryGate> allRun = new ArrayList<>();
        allRun.add(throwingGate);
        for (int i = between(0, 3); i > 0; i--) {
            allRun.add(() -> Decision.RUN);
        }
        assertTrue(newMonitor(new DeterministicTaskQueue(), allRun).evaluate().mayRun());

        // A blocking gate after the throwing one still blocks.
        final String gateName = randomIdentifier();
        final Decision decision = newMonitor(
            new DeterministicTaskQueue(),
            List.of(throwingGate, () -> Decision.block(gateName, randomAlphaOfLengthBetween(5, 30)))
        ).evaluate();
        assertFalse(decision.mayRun());
        assertThat(decision.gateName(), equalTo(gateName));
    }

    public void testGatesResolvedOnceOnFirstUse() {
        final AtomicInteger resolutions = new AtomicInteger();
        final var monitor = new RecoveryGateMonitor(() -> {
            resolutions.incrementAndGet();
            return List.of(() -> Decision.RUN);
        }, new DeterministicTaskQueue().getThreadPool(), clusterSettingsWithGatesEnabled());

        assertThat("supplier must not be resolved at construction", resolutions.get(), equalTo(0));
        for (int i = between(1, 3); i > 0; i--) {
            assertTrue(monitor.evaluate().mayRun());
        }
        assertThat(resolutions.get(), equalTo(1));
    }

    public void testCallbackFiredWhenAwaitedOutcomeReached() {
        final var taskQueue = new DeterministicTaskQueue();
        final var decision = new AtomicReference<>(Decision.block(randomIdentifier(), randomAlphaOfLengthBetween(5, 30)));
        final var monitor = newMonitor(taskQueue, decision);
        assertFalse(monitor.evaluate().mayRun());
        assertFalse("no recheck without a waiting callback", taskQueue.hasDeferredTasks());

        final AtomicInteger fired = new AtomicInteger();
        monitor.addCallback(RecoveryGate.Outcome.RUN, fired::incrementAndGet);
        final AtomicInteger otherFired = new AtomicInteger();
        monitor.addCallback(RecoveryGate.Outcome.RUN, otherFired::incrementAndGet);
        // Registration forks the evaluation to the (simulated) generic pool; running it schedules the periodic recheck.
        taskQueue.runAllRunnableTasks();
        assertTrue("waiting callbacks start the recheck", taskQueue.hasDeferredTasks());

        // Rechecks while still blocked fire nothing and keep rescheduling.
        for (int i = between(0, 3); i > 0; i--) {
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            assertTrue("recheck must keep rescheduling while callbacks wait", taskQueue.hasDeferredTasks());
        }
        assertThat(fired.get(), equalTo(0));
        assertThat(otherFired.get(), equalTo(0));

        // The gate unblocks: the next recheck fires each registered callback exactly once and stops, since no callback remains.
        decision.set(Decision.RUN);
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(fired.get(), equalTo(1));
        assertThat(otherFired.get(), equalTo(1));
        assertFalse("recheck must stop once no callback remains", taskQueue.hasDeferredTasks());
    }

    public void testCallbackFiredPromptlyWhenAlreadyAtAwaitedOutcome() {
        final var taskQueue = new DeterministicTaskQueue();
        final var monitor = newMonitor(taskQueue, new AtomicReference<>(Decision.RUN));

        // Registering for an outcome the decision already has fires via the forked evaluation, without waiting for a recheck, so a
        // caller racing with a decision change cannot miss it.
        final AtomicInteger fired = new AtomicInteger();
        monitor.addCallback(RecoveryGate.Outcome.RUN, fired::incrementAndGet);
        assertThat("callback must not fire inline on the registering thread", fired.get(), equalTo(0));
        taskQueue.runAllRunnableTasks();
        assertThat(fired.get(), equalTo(1));
        assertFalse("no recheck needed once the callback fired", taskQueue.hasDeferredTasks());
    }

    public void testCallbackFailureDoesNotPreventOtherCallbacks() {
        final var taskQueue = new DeterministicTaskQueue();
        final var decision = new AtomicReference<>(Decision.block(randomIdentifier(), randomAlphaOfLengthBetween(5, 30)));
        final var monitor = newMonitor(taskQueue, decision);

        monitor.addCallback(RecoveryGate.Outcome.RUN, () -> { throw new RuntimeException("simulated callback failure"); });
        final AtomicInteger fired = new AtomicInteger();
        monitor.addCallback(RecoveryGate.Outcome.RUN, fired::incrementAndGet);
        taskQueue.runAllRunnableTasks();

        decision.set(Decision.RUN);
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat("failure of an earlier callback must not prevent later ones", fired.get(), equalTo(1));
        assertFalse(taskQueue.hasDeferredTasks());
    }

    public void testGatesAreNotConsultedByDefault() {
        // Gates are off by default: recoveries are always allowed and the gates are not consulted, not even resolved.
        final var monitor = new RecoveryGateMonitor(
            () -> { throw new AssertionError("gates must not be resolved while the gates are disabled"); },
            new DeterministicTaskQueue().getThreadPool(),
            ClusterSettings.createBuiltInClusterSettings()
        );
        for (int i = between(1, 3); i > 0; i--) {
            assertTrue(monitor.evaluate().mayRun());
        }
    }

    public void testGatesEnabledSettingUpdatesDynamically() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterSettings = new ClusterSettings(Settings.EMPTY, Set.of(ENABLE_RECOVERY_GATES_SETTING));
        final var monitor = new RecoveryGateMonitor(
            () -> List.of(() -> Decision.block(randomIdentifier(), randomAlphaOfLengthBetween(5, 30))),
            taskQueue.getThreadPool(),
            clusterSettings
        );
        assertTrue("gates are disabled by default", monitor.evaluate().mayRun());

        clusterSettings.applySettings(Settings.builder().put(ENABLE_RECOVERY_GATES_SETTING.getKey(), true).build());
        assertFalse(monitor.evaluate().mayRun());

        // Resetting the setting disables the gates again.
        clusterSettings.applySettings(Settings.EMPTY);
        assertTrue(monitor.evaluate().mayRun());
    }

    public void testDisablingGatesReleasesWaitingCallbacks() {
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterSettings = clusterSettingsWithGatesEnabled();
        final var monitor = new RecoveryGateMonitor(
            () -> List.of(() -> Decision.block(randomIdentifier(), randomAlphaOfLengthBetween(5, 30))),
            taskQueue.getThreadPool(),
            clusterSettings
        );

        final AtomicInteger fired = new AtomicInteger();
        monitor.addCallback(RecoveryGate.Outcome.RUN, fired::incrementAndGet);
        taskQueue.runAllRunnableTasks();
        assertThat(fired.get(), equalTo(0));
        assertTrue("waiting callback starts the periodic recheck", taskQueue.hasDeferredTasks());

        // Disabling the gates is noticed by the next periodic recheck, which fires the waiting callback and stops rescheduling.
        clusterSettings.applySettings(Settings.builder().put(ENABLE_RECOVERY_GATES_SETTING.getKey(), false).build());
        taskQueue.advanceTime();
        taskQueue.runAllRunnableTasks();
        assertThat(fired.get(), equalTo(1));
        assertFalse(taskQueue.hasDeferredTasks());
    }

    private static RecoveryGateMonitor newMonitor(DeterministicTaskQueue taskQueue, AtomicReference<Decision> decision) {
        return newMonitor(taskQueue, List.of(decision::get));
    }

    private static RecoveryGateMonitor newMonitor(DeterministicTaskQueue taskQueue, List<RecoveryGate> gateList) {
        return new RecoveryGateMonitor(() -> gateList, taskQueue.getThreadPool(), clusterSettingsWithGatesEnabled());
    }

    private static ClusterSettings clusterSettingsWithGatesEnabled() {
        return new ClusterSettings(
            Settings.builder().put(ENABLE_RECOVERY_GATES_SETTING.getKey(), true).build(),
            Set.of(ENABLE_RECOVERY_GATES_SETTING)
        );
    }
}
