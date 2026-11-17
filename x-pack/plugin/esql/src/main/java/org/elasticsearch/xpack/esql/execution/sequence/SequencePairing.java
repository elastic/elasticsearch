/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.util.function.Predicate;

/** Separates retained candidate memory from the viability of another match. */
interface SequencePairing extends Releasable {
    /** Borrows identity/handle arguments and consumes the values owner, including on failure. */
    @Nullable
    SequenceMatch match(int stage, BytesRef key, SequenceOrdinal ordinal, BytesRef handle, @Nullable SequenceCandidateValues values);

    int retainedCandidates();

    /** Number of distinct keys currently owning matcher state. */
    int retainedKeys();

    /** Used markers consume memory but cannot make a later match possible. */
    boolean possibleMatch();

    /** The most recent unused first-stage row bounds useful later-stage reads. */
    @Nullable
    SequenceOrdinal latestUnusedStart();

    /**
     * Visits borrowed keys of every unused first-stage row while the source serializes mutation. A false return,
     * including an unsupported matcher implementation, requires an unfiltered search rather than a partial key set.
     * The visitor may stop early by returning false and must copy any key that survives this call.
     */
    default boolean visitUnusedStartKeys(Predicate<BytesRef> visitor) {
        return false;
    }

    /** Detached aggregate work; every source reports these counters, including zero values. */
    SequenceSourceStatus.LatestValidCounters latestValidCounters();
}
