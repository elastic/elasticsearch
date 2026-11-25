/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicLong;

public class ExternalPlanningReservationTests extends ESTestCase {

    /** Counts what reached the breaker, so a leak is visible as a total that never returns to zero. */
    private static final class CountingBreaker extends NoopCircuitBreaker {
        private final AtomicLong outstanding = new AtomicLong();
        private volatile Runnable onCharge;

        CountingBreaker() {
            super("test");
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            outstanding.addAndGet(bytes);
            Runnable hook = onCharge;
            if (hook != null) {
                onCharge = null;
                hook.run();
            }
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            outstanding.addAndGet(bytes);
        }
    }

    public void testEveryChargeIsReturnedOnClose() {
        CountingBreaker breaker = new CountingBreaker();
        ExternalPlanningReservation reservation = new ExternalPlanningReservation(breaker);
        reservation.chargeQuery(4096);
        ExternalPlanningReservation.Run run = reservation.openRun();
        run.charge(8192);
        assertEquals(12288, breaker.outstanding.get());

        reservation.close();

        assertEquals("the breaker is back where it started", 0, breaker.outstanding.get());
    }

    /**
     * Closing refunds the held total and nothing closes twice, so a charge that arrives afterwards is never
     * released - it leaks for the node's lifetime. Reachable once a charge runs on a thread the query can outrun,
     * which split discovery's hop to esql_external_io is. Refusing is what keeps that a failed query rather than a
     * node that slowly stops accepting work.
     */
    public void testChargingAfterCloseIsRefusedRatherThanLeaked() {
        CountingBreaker breaker = new CountingBreaker();
        ExternalPlanningReservation reservation = new ExternalPlanningReservation(breaker);
        ExternalPlanningReservation.Run run = reservation.openRun();
        run.charge(1024);
        reservation.close();
        assertEquals(0, breaker.outstanding.get());

        expectThrows(IllegalStateException.class, () -> reservation.chargeQuery(1024));
        expectThrows(IllegalStateException.class, () -> run.charge(1024));
        expectThrows(IllegalStateException.class, reservation::openRun);

        assertEquals("and nothing was admitted behind the refund", 0, breaker.outstanding.get());
    }

    /** A run closed on its own still refuses, without disturbing the query-scoped total. */
    public void testAClosedRunRefusesWhileTheQueryReservationStaysUsable() {
        CountingBreaker breaker = new CountingBreaker();
        try (ExternalPlanningReservation reservation = new ExternalPlanningReservation(breaker)) {
            ExternalPlanningReservation.Run run = reservation.openRun();
            run.charge(2048);
            run.close();

            expectThrows(IllegalStateException.class, () -> run.charge(1));
            reservation.chargeQuery(512);
            assertEquals("only the query-scoped charge is still held", 512, breaker.outstanding.get());
        }
        assertEquals(0, breaker.outstanding.get());
    }

    /** A breaker that trips leaves the held total alone, so close refunds only what was admitted. */
    public void testATripLeavesTheHeldTotalUnchanged() {
        CircuitBreaker tripping = new NoopCircuitBreaker("test") {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                throw new CircuitBreakingException("too big", Durability.TRANSIENT);
            }
        };
        ExternalPlanningReservation reservation = new ExternalPlanningReservation(tripping);
        expectThrows(CircuitBreakingException.class, () -> reservation.chargeQuery(1024));
        assertEquals(0, reservation.queryHeld());
        reservation.close();
    }

    /**
     * The anti-leak guard under an interleaving, which is the only way it can fail. A charge reads the flag,
     * charges the breaker and adds to the held total; if the whole of close() runs between the read and the add,
     * the refund takes a total that does not include those bytes and nothing ever releases them - close() is
     * one-shot. They sit on the request breaker for the node's lifetime.
     * <p>
     * Forced here by a breaker that closes the reservation from inside the charge itself, which is the interleaving
     * without the flakiness of racing two threads.
     */
    public void testAChargeRacingCloseIsNeverLeft() {
        CountingBreaker breaker = new CountingBreaker();
        ExternalPlanningReservation reservation = new ExternalPlanningReservation(breaker);
        breaker.onCharge = reservation::close;

        expectThrows(IllegalStateException.class, () -> reservation.chargeQuery(4096));

        assertEquals("the bytes went back rather than sitting on the breaker", 0, breaker.outstanding.get());
        assertEquals(0, reservation.queryHeld());
    }

}
