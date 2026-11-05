/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import java.util.concurrent.atomic.LongAccumulator;

import static org.elasticsearch.search.vectors.AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE;

/**
 * Minimum competitive doc score shared across the leaves of one IVF kNN search, so a leaf can skip candidates that
 * another leaf has already outscored.
 */
public final class SharedMinCompetitiveScore {
    private final LongAccumulator accumulator;
    private final boolean canPublish;

    SharedMinCompetitiveScore() {
        this(new LongAccumulator(Long::max, LEAST_COMPETITIVE), true);
    }

    private SharedMinCompetitiveScore(LongAccumulator accumulator, boolean canPublish) {
        this.accumulator = accumulator;
        this.canPublish = canPublish;
    }

    long get() {
        return accumulator.get();
    }

    /** Raises the shared score to {@code score} if it is more competitive; a no-op on a {@link #readOnly()} view. */
    void publish(long score) {
        if (canPublish) {
            accumulator.accumulate(score);
        }
    }

    /** View over the same score that reads it but ignores {@link #publish}. */
    SharedMinCompetitiveScore readOnly() {
        return canPublish ? new SharedMinCompetitiveScore(accumulator, false) : this;
    }

    boolean canPublish() {
        return canPublish;
    }
}
