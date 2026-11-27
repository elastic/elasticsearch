/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.search.Scorable;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The highest minimum competitive score that any of the collectors of a top N by descending {@code _score} has reached,
 * shared by the drivers that run it. A collector only offers a score once it holds a full top N of rows that score at
 * least as high, and those rows are part of the output. So every other collector can skip the documents that score
 * lower, whichever shard they are in, by handing the score to {@link Scorable#setMinCompetitiveScore}.
 * <p>
 *     The score only grows, which {@link Scorable#setMinCompetitiveScore} requires.
 * </p>
 */
final class SharedMinCompetitiveScore {
    /**
     * The bits of the score. Scores are never negative, and the bits of non-negative floats sort like the floats.
     */
    private final AtomicInteger bits = new AtomicInteger(Float.floatToIntBits(0f));

    /**
     * The score below which documents can't make the top N, or {@code 0} if no collector has a full top N yet.
     */
    float get() {
        return Float.intBitsToFloat(bits.get());
    }

    /**
     * Raises the shared score to {@code score} if it is lower.
     */
    void offer(float score) {
        if (score > 0 && Float.isFinite(score)) {
            bits.accumulateAndGet(Float.floatToIntBits(score), Math::max);
        }
    }
}
