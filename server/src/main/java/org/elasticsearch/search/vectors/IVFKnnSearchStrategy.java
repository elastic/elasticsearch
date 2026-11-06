/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.util.SetOnce;

import java.util.Objects;

public class IVFKnnSearchStrategy extends KnnSearchStrategy {
    private final float visitRatio;
    private final int numCands;
    private final int k;
    private final SetOnce<AbstractMaxScoreKnnCollector> collector = new SetOnce<>();
    private final SharedCompetitiveScore.Tracker sharedScoreTracker;

    public IVFKnnSearchStrategy(float visitRatio, int numCands, int k, SharedCompetitiveScore sharedScore) {
        this.visitRatio = visitRatio;
        this.numCands = numCands;
        this.k = k;
        this.sharedScoreTracker = sharedScore == null ? null : sharedScore.newTracker();
    }

    void setCollector(AbstractMaxScoreKnnCollector collector) {
        this.collector.set(collector);
        if (sharedScoreTracker != null) {
            sharedScoreTracker.sync(collector);
        }
    }

    public float getVisitRatio() {
        return visitRatio;
    }

    public int getNumCands() {
        return numCands;
    }

    public int getK() {
        return k;
    }

    /** Whether enough candidates across leaves support the shared competitive score. */
    public boolean hasSharedCompetitiveScore() {
        return sharedScoreTracker != null && sharedScoreTracker.hasEstablishedScore();
    }

    /** Whether either the local queue is full or enough candidates across leaves support a shared score. */
    public boolean hasSufficientCandidates() {
        AbstractMaxScoreKnnCollector collector = this.collector.get();
        assert collector != null : "Collector must be set before checking candidate sufficiency";
        return collector.numCollected() >= collector.k() || hasSharedCompetitiveScore();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IVFKnnSearchStrategy that = (IVFKnnSearchStrategy) o;
        return visitRatio == that.visitRatio && numCands == that.numCands && k == that.k;
    }

    @Override
    public int hashCode() {
        return Objects.hash(visitRatio, numCands, k);
    }

    /**
     * This method is called when the next block of vectors is processed.
     * Contributes newly retained candidates and consumes an established shared competitive score.
     */
    @Override
    public void nextVectorsBlock() {
        if (sharedScoreTracker == null) {
            return;
        }
        assert this.collector.get() != null : "Collector must be set before nextVectorsBlock is called";
        sharedScoreTracker.sync(this.collector.get());
    }
}
