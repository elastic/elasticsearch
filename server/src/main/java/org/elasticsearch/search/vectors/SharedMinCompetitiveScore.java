/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;

import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.search.vectors.AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE;

/**
 * Minimum competitive doc score shared across the leaves of one IVF kNN search, so a leaf can skip candidates that
 * another leaf has already outscored.
 * <p>
 * Only the score is shared. A published bound's doc id tie-breaker is local to the publishing segment, so it cannot
 * order ties against docs in other leaves. A doc in another leaf that ties the shared score may still win the doc id
 * tie-break when leaves are merged, so the shared bound is one that ties beat.
 */
public final class SharedMinCompetitiveScore {
    private final AtomicLong value;
    private final boolean canPublish;

    SharedMinCompetitiveScore() {
        this(new AtomicLong(LEAST_COMPETITIVE), true);
    }

    private SharedMinCompetitiveScore(AtomicLong value, boolean canPublish) {
        this.value = value;
        this.canPublish = canPublish;
    }

    long get() {
        return value.get();
    }

    /**
     * Raises the shared bound to the score of the encoded {@code minCompetitiveDocScore} if it is more competitive;
     * a no-op on a {@link #readOnly()} view.
     * <p>
     * Collectors skip candidates that do not score strictly above the bound, so the stored bound is one ulp below the
     * published score, with the least competitive doc id in place of the segment-local one, to keep candidates that tie it.
     */
    void publish(long minCompetitiveDocScore) {
        if (canPublish == false) {
            return;
        }
        long bound = NeighborQueue.encodeRaw(Integer.MAX_VALUE, Math.nextDown(NeighborQueue.decodeScoreRaw(minCompetitiveDocScore)));
        if (bound > value.get()) {
            value.accumulateAndGet(bound, Math::max);
        }
    }

    /** View over the same score that reads it but ignores {@link #publish}. */
    SharedMinCompetitiveScore readOnly() {
        return canPublish ? new SharedMinCompetitiveScore(value, false) : this;
    }

    boolean canPublish() {
        return canPublish;
    }
}
