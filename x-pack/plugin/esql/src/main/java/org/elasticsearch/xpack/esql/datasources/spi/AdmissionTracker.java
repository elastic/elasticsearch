/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Wait/grant hook for node-level external-source admission gates. A waiter that cannot pass a
 * gate (bytes, query budget, storage permits, streaming segmentators) records the wait here and
 * completes it on grant or on timeout/cancel. The stall watchdog polls these events on
 * {@code GENERIC}; it does not run on {@code [scheduler]}.
 * <p>
 * Call {@link #waitStarted} when a caller actually parks. Uncontended acquires skip this hook.
 * {@link #NOOP} is the default when no watchdog is installed (tests, short constructors).
 */
public interface AdmissionTracker {

    String GATE_BYTES = "bytes";
    String GATE_BUDGET = "budget";
    String GATE_SEGMENTATORS = "segmentators";

    static String permits(String scheme) {
        return "permits/" + scheme;
    }

    static String budget(String scheme) {
        return "budget/" + scheme;
    }

    Wait NOOP_WAIT = new Wait() {
        @Override
        public void granted() {}

        @Override
        public void finished() {}
    };

    AdmissionTracker NOOP = new AdmissionTracker() {
        @Override
        public Wait waitStarted(String gate, String waiter) {
            return NOOP_WAIT;
        }
    };

    /**
     * One parked waiter. Exactly one of {@link #granted()} or {@link #finished()} must run;
     * both are idempotent.
     */
    interface Wait {
        /** The waiter received the resource. */
        void granted();

        /** The waiter left without a grant (timeout, interrupt, cancel, close). */
        void finished();
    }

    /**
     * Records that {@code waiter} is parked on {@code gate}.
     */
    default Wait waitStarted(String gate, String waiter) {
        return NOOP_WAIT;
    }

    /** Optional holder probe used to render the WARN graph. */
    default void register(AdmissionGate gate) {}
}
