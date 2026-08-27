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
import java.util.concurrent.atomic.LongAccumulator;

public class IVFKnnSearchStrategy extends KnnSearchStrategy {
    private final float visitRatio;
    private final int numCands;
    private final int k;
    private final SetOnce<AbstractMaxScoreKnnCollector> collector = new SetOnce<>();
    private final LongAccumulator accumulator;
    private final KnnSearchProfileData profileData;
    private volatile float resolvedVisitRatio = Float.NaN;

    public IVFKnnSearchStrategy(float visitRatio, int numCands, int k, LongAccumulator accumulator) {
        this(visitRatio, numCands, k, accumulator, null);
    }

    public IVFKnnSearchStrategy(float visitRatio, int numCands, int k, LongAccumulator accumulator, KnnSearchProfileData profileData) {
        this.visitRatio = visitRatio;
        this.numCands = numCands;
        this.k = k;
        this.accumulator = accumulator;
        this.profileData = profileData;
    }

    /**
     * Returns the profiling accumulator threaded down to the codec, or {@code null} when detailed
     * profiling is not enabled for this query.
     */
    public KnnSearchProfileData getProfileData() {
        return profileData;
    }

    void setCollector(AbstractMaxScoreKnnCollector collector) {
        this.collector.set(collector);
        if (accumulator != null) {
            collector.updateMinCompetitiveDocScore(accumulator.get());
        }
    }

    public float getVisitRatio() {
        return visitRatio;
    }

    /**
     * Records the visit ratio the codec actually used. When {@link #getVisitRatio()} is {@code 0.0f} the
     * codec derives the ratio per segment, so it is only known after the search has run. One strategy is
     * built per leaf search, which is what lets the query attribute the ratio back to the right segment
     * without any thread-affinity assumptions.
     */
    public void setResolvedVisitRatio(float resolvedVisitRatio) {
        this.resolvedVisitRatio = resolvedVisitRatio;
    }

    /** The ratio the codec used, or {@link Float#NaN} if it did not report one. */
    public float getResolvedVisitRatio() {
        return resolvedVisitRatio;
    }

    public int getNumCands() {
        return numCands;
    }

    public int getK() {
        return k;
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
     * It accumulates the minimum competitive document score from the collector
     * and updates the accumulator with the most competitive score.
     * If the current score in the accumulator is greater than the minimum competitive
     * document score in the collector, it updates the collector's minimum competitive document score.
     */
    @Override
    public void nextVectorsBlock() {
        if (accumulator == null) {
            return;
        }
        assert this.collector.get() != null : "Collector must be set before nextVectorsBlock is called";
        AbstractMaxScoreKnnCollector knnCollector = this.collector.get();
        long collectorScore = knnCollector.getMinCompetitiveDocScore();
        accumulator.accumulate(collectorScore);
        long currentScore = accumulator.get();
        if (currentScore > collectorScore) {
            knnCollector.updateMinCompetitiveDocScore(currentScore);
        }
    }
}
