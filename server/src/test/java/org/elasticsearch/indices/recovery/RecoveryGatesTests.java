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
import org.elasticsearch.indices.recovery.RecoveryGates.DecisionChangeListener;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class RecoveryGatesTests extends ESTestCase {

    public void testEmptyMayRun() {
        final var gates = new RecoveryGates(List.of(), () -> {}, noopChange(), () -> 0L);
        assertTrue(gates.isEmpty());
        assertTrue(gates.evaluate().mayRun());
    }

    public void testAllRunMayRun() {
        final List<RecoveryGate> gates = new ArrayList<>();
        for (int i = between(1, 5); i > 0; i--) {
            gates.add(new TestGate(Decision.RUN));
        }
        assertTrue(new RecoveryGates(gates, () -> {}, noopChange(), () -> 0L).evaluate().mayRun());
    }

    public void testAnyBlockReturnsBlockingGateDecision() {
        final String gateName = randomIdentifier();
        final String reason = randomAlphaOfLengthBetween(5, 30);
        final List<RecoveryGate> gates = new ArrayList<>();
        // The single blocking gate wins regardless of how many may-run gates surround it.
        for (int i = between(0, 3); i > 0; i--) {
            gates.add(new TestGate(Decision.RUN));
        }
        gates.add(new TestGate(Decision.block(gateName, reason)));
        for (int i = between(0, 3); i > 0; i--) {
            gates.add(new TestGate(Decision.RUN));
        }

        final Decision decision = new RecoveryGates(gates, () -> {}, noopChange(), () -> 0L).evaluate();
        assertFalse(decision.mayRun());
        assertThat(decision.gateName(), equalTo(gateName));
        assertThat(decision.reason(), equalTo(reason));
    }

    public void testShortCircuitsOnFirstBlock() {
        final List<RecoveryGate> gates = new ArrayList<>();
        gates.add(new TestGate(Decision.block(randomIdentifier(), randomAlphaOfLengthBetween(5, 30))));
        gates.add(new RecoveryGate() {
            @Override
            public Decision evaluate() {
                throw new AssertionError("gates after a node-wide block must not be evaluated");
            }

            @Override
            public void setGateChangeHandler(Runnable gateChangeHandler) {}
        });
        assertFalse(new RecoveryGates(gates, () -> {}, noopChange(), () -> 0L).evaluate().mayRun());
    }

    public void testInvokesReCheckHandlerOnGateSignal() {
        // The re-check handler is set on each gate, so a gate reporting a change re-checks; construction itself does not.
        final AtomicInteger reChecks = new AtomicInteger();
        final List<TestGate> gates = new ArrayList<>();
        for (int i = between(1, 4); i > 0; i--) {
            gates.add(new TestGate(Decision.RUN));
        }
        new RecoveryGates(List.copyOf(gates), reChecks::incrementAndGet, noopChange(), () -> 0L);
        assertThat("construction does not re-check", reChecks.get(), equalTo(0));

        final int changes = between(1, 10);
        for (int i = 0; i < changes; i++) {
            randomFrom(gates).fireGateChange();
        }
        assertThat(reChecks.get(), equalTo(changes));
    }

    public void testCheckReportsTransitions() {
        final var previousDecision = new AtomicReference<Decision>();
        final var currentDecision = new AtomicReference<Decision>();
        final var reportedDuration = new AtomicLong(-1);
        final var changeCount = new AtomicInteger();
        final long startMillis = randomLongBetween(0, 1_000_000);
        final var clock = new AtomicLong(startMillis);
        final var gate = new TestGate(Decision.RUN);
        final var gates = new RecoveryGates(List.of(gate), () -> {}, (previous, current, durationMillis) -> {
            previousDecision.set(previous);
            currentDecision.set(current);
            reportedDuration.set(durationMillis);
            changeCount.incrementAndGet();
        }, clock::get);

        // Starts may-run: no transition, no callback.
        assertTrue(gates.check().mayRun());
        assertThat(changeCount.get(), equalTo(0));

        // Flips to block: reported once, with the blocking gate and reason. Repeated checks while blocked do not re-report.
        final String gateName = randomIdentifier();
        final String reason = randomAlphaOfLengthBetween(5, 30);
        gate.set(Decision.block(gateName, reason));
        clock.set(startMillis + randomLongBetween(0, 10_000));
        final long blockMillis = clock.get();
        assertFalse(gates.check().mayRun());
        for (int i = between(0, 3); i > 0; i--) {
            assertFalse(gates.check().mayRun());
        }
        assertThat(changeCount.get(), equalTo(1));
        assertTrue(previousDecision.get().mayRun());
        assertThat(currentDecision.get().gateName(), equalTo(gateName));
        assertThat(currentDecision.get().reason(), equalTo(reason));

        // Flips back to run: reported once with the elapsed blocked time and the gate that had been blocking.
        final long blockedDuration = randomLongBetween(1, 120_000);
        gate.set(Decision.RUN);
        clock.set(blockMillis + blockedDuration);
        assertTrue(gates.check().mayRun());
        assertThat(changeCount.get(), equalTo(2));
        assertThat(previousDecision.get().gateName(), equalTo(gateName));
        assertTrue(currentDecision.get().mayRun());
        assertThat(reportedDuration.get(), equalTo(blockedDuration));
    }

    private static DecisionChangeListener noopChange() {
        return (previous, current, durationMillis) -> {};
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
