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
 * The score a document must reach to make a top N by descending {@code _score}, shared by the drivers that run it.
 * <p>
 *     A score is only offered on behalf of a full top N. The rows of that top N are part of the output, unless their
 *     driver fails, and then the query fails or its result is flagged as partial. So no collector needs a document that
 *     scores below the bound, in any shard: it scores below all of those rows or, when the score is the only sort key,
 *     ties with the last one. Collectors skip such documents by handing the bound to
 *     {@link Scorable#setMinCompetitiveScore}.
 * </p>
 * <p>
 *     {@link SharedSortBound} is the bound of a top N on a field. It is shared by the batches of shards of a request and
 *     skips whole shards. This bound belongs to one operator factory and makes scorers skip documents.
 * </p>
 * <p>
 *     The bound only grows, which {@link Scorable#setMinCompetitiveScore} requires.
 * </p>
 */
final class SharedScoreBound {
    /**
     * The bits of the score. Scores are never negative, and the bits of non-negative floats, infinity included, sort
     * like the floats.
     */
    private final AtomicInteger bits = new AtomicInteger(Float.floatToIntBits(0f));

    /**
     * The bound, or {@code 0} if no collector has a full top N yet.
     */
    float get() {
        return Float.intBitsToFloat(bits.get());
    }

    /**
     * Raises the bound to {@code score} if it is lower.
     */
    void offer(float score) {
        int offered = Float.floatToIntBits(score);
        // Not NaN, and not 0, which is the bound already. Most offers don't raise the bound, and those only read it.
        if (score > 0 && offered > bits.get()) {
            bits.accumulateAndGet(offered, Math::max);
        }
    }
}
