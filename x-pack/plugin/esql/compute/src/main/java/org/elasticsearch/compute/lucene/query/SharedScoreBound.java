/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.search.Scorable;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.TopNOperator;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The score a document must reach to make a top N by descending {@code _score}, shared by the drivers that run it.
 * <p>
 *     A score is only offered on behalf of a full top N. The rows of that top N are part of the output, unless their
 *     driver fails, and then the query fails or its result is flagged as partial. So no collector needs a document that
 *     scores below the bound, in any shard. Such a document scores below every row of that top N. The one exception is
 *     a sort by the score alone, where it can also tie with the last row, and tied rows are interchangeable.
 * </p>
 * <p>
 *     Collectors skip those documents by handing the bound to {@link Scorable#setMinCompetitiveScore}.
 * </p>
 * <p>
 *     Two other classes share a bound of a top N, neither for scorers. {@link SharedSortBound} holds the bound on a
 *     field for the batches of shards of a request and skips whole shards. {@link SharedMinCompetitive} holds the encoded
 *     sort keys of a {@link TopNOperator}, which filters documents with a query. This bound is one float that belongs to
 *     one operator factory.
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
