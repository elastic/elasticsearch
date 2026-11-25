/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.lucene.search.cost.DocIdSetBuilderCostEstimator;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.util.function.LongConsumer;

/**
 * A {@link ConstantScoreWeight} whose scorers charge the {@link DocIdSetBuilder} they fill to the request circuit breaker as it grows,
 * so a clause is charged for the set it builds rather than for a bound on every set it could build. The charges go through
 * {@link ContextIndexSearcher#getLeafCharger} of the searcher the weight is created with, which reaches the weight however its query
 * is wrapped; on a searcher without a request circuit breaker nothing is charged.
 */
abstract class ChargingConstantScoreWeight extends ConstantScoreWeight {

    private final ContextIndexSearcher.LeafCharger charger;

    ChargingConstantScoreWeight(Query query, float score, IndexSearcher searcher) {
        super(query, score);
        this.charger = ContextIndexSearcher.getLeafCharger(searcher);
    }

    /** Charges the bytes it accepts to {@code ctx}, for as long as the leaf is scored. */
    LongConsumer leafCharger(LeafReaderContext ctx) {
        return bytes -> charger.charge(ctx, bytes, BitmapTermsQueryBuilder.NAME);
    }

    /**
     * For one {@link DocIdSetBuilder} that a scorer fills on {@code ctx}: accepts each count passed to {@link DocIdSetBuilder#grow},
     * and must be called before that grow, so that the breaker can trip before the builder allocates. What it charges is held for as
     * long as the leaf is scored.
     */
    LongConsumer builderCharger(LeafReaderContext ctx) {
        return new BuilderCharger(leafCharger(ctx), ctx.reader().maxDoc());
    }

    /**
     * Charges what one {@link DocIdSetBuilder} holds, as {@link DocIdSetBuilderCostEstimator} bounds it, while the builder grows. The
     * charge goes up in doubling steps, capped at the dense size, so the breaker is consulted a logarithmic number of times however
     * many grows the set takes, since every charge also checks the parent breaker. While the set is sparse that charges up to twice
     * the bound.
     */
    static final class BuilderCharger implements LongConsumer {

        private final LongConsumer charge;
        private final int maxDoc;
        private final long denseBytes;
        private long grown;
        private long charged;

        /**
         * @param charge reserves more bytes on the breaker
         * @param maxDoc the {@code maxDoc} the builder was created with
         */
        BuilderCharger(LongConsumer charge, int maxDoc) {
            this.charge = charge;
            this.maxDoc = maxDoc;
            this.denseBytes = new DocIdSetBuilderCostEstimator(maxDoc, maxDoc).estimate();
        }

        @Override
        public void accept(long numDocs) {
            if (charged == denseBytes) {
                return;
            }
            grown += numDocs;
            long needed = new DocIdSetBuilderCostEstimator(grown, maxDoc).estimate();
            if (needed > charged) {
                long target = Math.clamp(2 * charged, needed, denseBytes);
                charge.accept(target - charged);
                charged = target;
            }
        }
    }
}
