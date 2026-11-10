/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.AbstractKnnCollector;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;

/**
 * Abstract class for collectors that maintain a maximum score for KNN search.
 * It extends the {@link AbstractKnnCollector} and provides methods to manage
 * the minimum competitive document score, useful for tracking competitive scores
 * over multiple leaves.
 */
abstract class AbstractMaxScoreKnnCollector extends AbstractKnnCollector {
    public static final long LEAST_COMPETITIVE = NeighborQueue.encodeRaw(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);
    // Collector-local copy. Each collector is confined to its leaf search task, so this does not need to be atomic.
    private long collectorMinCompetitiveDocScore = LEAST_COMPETITIVE;

    protected AbstractMaxScoreKnnCollector(int k, long visitLimit, KnnSearchStrategy searchStrategy) {
        super(k, visitLimit, searchStrategy);
    }

    /** Merges the shared bound into this collector's local copy and returns the resulting encoded score. */
    final long mergeMinCompetitiveDocScore(long sharedMinCompetitiveDocScore) {
        collectorMinCompetitiveDocScore = Math.max(
            collectorMinCompetitiveDocScore,
            Math.max(localQueueMinCompetitiveDocScore(), sharedMinCompetitiveDocScore)
        );
        return collectorMinCompetitiveDocScore;
    }

    /**
     * Returns either the competitive score of a full local queue or a shard-wide score already supported by enough
     * candidates across leaves. The latter may be finite before this collector has {@code k} results because it no
     * longer needs to supply {@code k} results independently.
     */
    @Override
    public final float minCompetitiveSimilarity() {
        return NeighborQueue.decodeScoreRaw(Math.max(collectorMinCompetitiveDocScore, localQueueMinCompetitiveDocScore()));
    }

    /** Returns a lower bound on every retained document score. The collector must not be empty. */
    abstract long collectedScoreLowerBound();

    private long localQueueMinCompetitiveDocScore() {
        return numCollected() >= k() ? collectedScoreLowerBound() : LEAST_COMPETITIVE;
    }
}
