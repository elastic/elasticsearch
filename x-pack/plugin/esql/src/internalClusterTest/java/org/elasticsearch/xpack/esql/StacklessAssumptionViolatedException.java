/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.junit.AssumptionViolatedException;

/**
 * An {@link AssumptionViolatedException} that suppresses stacktrace generation and storage. When
 * the randomized runner reports a skipped test it includes the full stack trace of the exception —
 * a complete trace for a known skip is noise and can be many kilobytes per test. The skip
 * therefore keeps its reason while dropping the redundant trace entirely.
 */
class StacklessAssumptionViolatedException extends AssumptionViolatedException {
    StacklessAssumptionViolatedException(String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    @Override
    public void setStackTrace(StackTraceElement[] stackTrace) {
        // Intentionally a no-op: mirrors writableStackTrace=false so the randomized runner cannot
        // re-attach a (single, identical) seed frame to the otherwise empty trace.
    }
}
