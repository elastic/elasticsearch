/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;

import java.util.Comparator;
import java.util.function.BooleanSupplier;

/** Separates retained memory from viability without changing the legacy matcher's public API. */
interface SequencePairing extends Releasable {
    /** Borrows identity/handle arguments and consumes the values owner, including on failure. */
    @Nullable
    SequenceMatch match(int stage, BytesRef key, SequenceOrdinal ordinal, BytesRef handle, @Nullable SequenceCandidateValues values);

    int retainedCandidates();

    /** Used markers consume memory but cannot make a later match possible. */
    boolean possibleMatch();

    /** The latest unconsumed logical start; advanced prefixes preserve the legacy bound. */
    @Nullable
    SequenceOrdinal latestUnusedStart();

    /** Default pairing has no new-mode fields; snapshots contain no retained row or value ownership. */
    @Nullable
    default SequenceSourceStatus.LatestValidCounters latestValidCounters() {
        return null;
    }

    /** One adapter per source; completed legacy matches are returned without wrappers. */
    static SequencePairing legacy(
        int stages,
        TimeValue maxSpan,
        CircuitBreaker breaker,
        BooleanSupplier cancelled,
        Comparator<BytesRef> keyOrder,
        @Nullable long[] maxGapNanos
    ) {
        SequenceMatcher matcher = new SequenceMatcher(stages, maxSpan, breaker, cancelled, keyOrder, maxGapNanos);
        long bytes = RamUsageEstimator.shallowSizeOfInstance(Legacy.class);
        try {
            breaker.addEstimateBytesAndMaybeBreak(bytes, "esql_sequence");
        } catch (RuntimeException | Error e) {
            matcher.close();
            throw e;
        }
        try {
            return new Legacy(matcher, breaker, bytes);
        } catch (RuntimeException | Error e) {
            matcher.close();
            breaker.addWithoutBreaking(-bytes, "esql_sequence");
            throw e;
        }
    }

    /** Keeps the old prefix policy, including its advanced-prefix upper bound, unchanged. */
    final class Legacy implements SequencePairing {
        private final SequenceMatcher matcher;
        private final CircuitBreaker breaker;
        private final long bytes;
        private boolean closed;

        private Legacy(SequenceMatcher matcher, CircuitBreaker breaker, long bytes) {
            this.matcher = matcher;
            this.breaker = breaker;
            this.bytes = bytes;
        }

        @Override
        public SequenceMatch match(int stage, BytesRef key, SequenceOrdinal ordinal, BytesRef handle, SequenceCandidateValues values) {
            if (values != null) {
                try {
                    values.close();
                } finally {
                    close();
                }
                throw new IllegalArgumentException("legacy sequence pairing does not accept mandatory values");
            }
            return matcher.match(stage, key, ordinal, handle);
        }

        @Override
        public int retainedCandidates() {
            return matcher.pendingCandidates();
        }

        @Override
        public boolean possibleMatch() {
            return matcher.pendingCandidates() != 0;
        }

        @Override
        public SequenceOrdinal latestUnusedStart() {
            return matcher.latestStart();
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                try {
                    matcher.close();
                } finally {
                    breaker.addWithoutBreaking(-bytes, "esql_sequence");
                }
            }
        }
    }
}
