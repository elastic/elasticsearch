/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.indices.recovery.RecoveryGate.Decision;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class RecoveryGatesTests extends ESTestCase {

    public void testEmptyMayRun() {
        final var gates = newGates();
        assertTrue(gates.isEmpty());
        assertTrue(gates.evaluate().mayRun());
    }

    public void testAllRunMayRun() {
        final var gates = newGates();
        for (int i = between(1, 5); i > 0; i--) {
            gates.addGate(new TestGate(Decision.RUN));
        }
        assertTrue(gates.evaluate().mayRun());
    }

    public void testAnyBlockReturnsBlockingGateDecision() {
        final var gates = newGates();
        final String gateName = randomIdentifier();
        final String reason = randomAlphaOfLengthBetween(5, 30);
        // The single blocking gate wins regardless of how many may-run gates surround it.
        for (int i = between(0, 3); i > 0; i--) {
            gates.addGate(new TestGate(Decision.RUN));
        }
        gates.addGate(new TestGate(Decision.block(gateName, reason)));
        for (int i = between(0, 3); i > 0; i--) {
            gates.addGate(new TestGate(Decision.RUN));
        }

        final Decision decision = gates.evaluate();
        assertFalse(decision.mayRun());
        assertThat(decision.gateName(), equalTo(gateName));
        assertThat(decision.reason(), equalTo(reason));
    }

    public void testShortCircuitsOnFirstBlock() {
        final var gates = newGates();
        gates.addGate(new TestGate(Decision.block(randomIdentifier(), randomAlphaOfLengthBetween(5, 30))));
        gates.addGate(() -> { throw new AssertionError("gates after a node-wide block must not be evaluated"); });
        assertFalse(gates.evaluate().mayRun());
    }

    public void testForwardsChangeHandlerToGates() {
        // The gate-change handler is forwarded to each gate, so a gate reporting a change re-checks (adding a gate does not).
        final AtomicInteger reChecks = new AtomicInteger();
        final var gates = new RecoveryGates(reChecks::incrementAndGet, (gateName, reason) -> {}, (gateName, millis) -> {}, () -> 0L);

        final List<TestGate> added = new ArrayList<>();
        for (int i = between(1, 4); i > 0; i--) {
            final var gate = new TestGate(Decision.RUN);
            gates.addGate(gate);
            added.add(gate);
        }
        assertThat("adding gates does not re-check", reChecks.get(), equalTo(0));

        final int changes = between(1, 10);
        for (int i = 0; i < changes; i++) {
            randomFrom(added).fireGateChange();
        }
        assertThat(reChecks.get(), equalTo(changes));
    }

    public void testCheckReportsTransitionsWithBlockedDuration() {
        final var blockedGate = new AtomicReference<String>();
        final var blockedReason = new AtomicReference<String>();
        final var unblockedGate = new AtomicReference<String>();
        final var reportedMillis = new AtomicLong(-1);
        final var blockedCount = new AtomicInteger();
        final var unblockedCount = new AtomicInteger();
        final long startMillis = randomLongBetween(0, 1_000_000);
        final var clock = new AtomicLong(startMillis);
        final var gates = new RecoveryGates(() -> {}, (gateName, reason) -> {
            blockedGate.set(gateName);
            blockedReason.set(reason);
            blockedCount.incrementAndGet();
        }, (gateName, millis) -> {
            unblockedGate.set(gateName);
            reportedMillis.set(millis);
            unblockedCount.incrementAndGet();
        }, clock::get);

        final var gate = new TestGate(Decision.RUN);
        gates.addGate(gate);

        // Starts may-run: no transition, no callback.
        assertTrue(gates.check().mayRun());
        assertThat(blockedCount.get(), equalTo(0));

        // Flips to block: reported once, naming the responsible gate and reason. Repeated checks while blocked do not re-report.
        final String gateName = randomIdentifier();
        final String reason = randomAlphaOfLengthBetween(5, 30);
        gate.set(Decision.block(gateName, reason));
        final long blockMillis = startMillis + randomLongBetween(0, 10_000);
        clock.set(blockMillis);
        assertFalse(gates.check().mayRun());
        for (int i = between(0, 3); i > 0; i--) {
            assertFalse(gates.check().mayRun());
        }
        assertThat(blockedCount.get(), equalTo(1));
        assertThat(blockedGate.get(), equalTo(gateName));
        assertThat(blockedReason.get(), equalTo(reason));
        assertThat(unblockedCount.get(), equalTo(0));

        // Flips back to run: reported once with the elapsed blocked time and the gate that had been blocking.
        final long blockedDuration = randomLongBetween(1, 120_000);
        gate.set(Decision.RUN);
        clock.set(blockMillis + blockedDuration);
        assertTrue(gates.check().mayRun());
        assertThat(unblockedCount.get(), equalTo(1));
        assertThat(unblockedGate.get(), equalTo(gateName));
        assertThat(reportedMillis.get(), equalTo(blockedDuration));
    }

    private static RecoveryGates newGates() {
        return new RecoveryGates(() -> {}, (gateName, reason) -> {}, (gateName, millis) -> {}, () -> 0L);
    }

    private static class TestGate implements RecoveryGate {
        private volatile Decision decision;
        private volatile Runnable gateChangeHandler = () -> {};

        TestGate(Decision decision) {
            this.decision = decision;
        }

        void set(Decision decision) {
            this.decision = decision;
        }

        @Override
        public Decision evaluate() {
            return decision;
        }

        @Override
        public void setGateChangeHandler(Runnable gateChangeHandler) {
            this.gateChangeHandler = gateChangeHandler;
        }

        void fireGateChange() {
            gateChangeHandler.run();
        }
    }
}
