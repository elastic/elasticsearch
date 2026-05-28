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
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.SetOnce;

import java.util.Objects;
import java.util.concurrent.atomic.LongAccumulator;

public class IVFKnnSearchStrategy extends KnnSearchStrategy {
    private final float visitRatio;
    private final int numCands;
    private final int k;
    private final SetOnce<AbstractMaxScoreKnnCollector> collector = new SetOnce<>();
    private final LongAccumulator accumulator;
    private final FixedBitSet skipCentroids;
    /**
     * When false, the per-centroid bookkeeping ({@link #initVisitedCentroids}, {@link #beforeCentroidVisit},
     * {@link #afterCentroidVisit}) is short-circuited to no-ops — used by IVF queries that won't be
     * followed by a retry round (the original user query, the post-filter fallback path, and the
     * final retry round).
     */
    private final boolean trackForRetry;
    private FixedBitSet visitedCentroids;
    private FixedBitSet competitiveCentroids;
    private long preVisitMinScore;
    private int preVisitNumCollected;

    public IVFKnnSearchStrategy(float visitRatio, int numCands, int k, LongAccumulator accumulator, FixedBitSet skipCentroids) {
        this(visitRatio, numCands, k, accumulator, skipCentroids, false);
    }

    public IVFKnnSearchStrategy(
        float visitRatio,
        int numCands,
        int k,
        LongAccumulator accumulator,
        FixedBitSet skipCentroids,
        boolean trackForRetry
    ) {
        this.visitRatio = visitRatio;
        this.numCands = numCands;
        this.k = k;
        this.accumulator = accumulator;
        this.skipCentroids = skipCentroids;
        this.trackForRetry = trackForRetry;
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

    public int getNumCands() {
        return numCands;
    }

    public int getK() {
        return k;
    }

    /**
     * Initializes the visited centroids tracker (and the parallel competitive-centroids tracker)
     * with the given number of centroids. Called by IVFVectorsReader when the number of centroids
     * is known. No-op when {@link #trackForRetry} is false.
     */
    public void initVisitedCentroids(int numCentroids) {
        if (trackForRetry && numCentroids > 0) {
            this.visitedCentroids = new FixedBitSet(numCentroids);
            this.competitiveCentroids = new FixedBitSet(numCentroids);
        }
    }

    /**
     * Returns true if the centroid with the given ordinal should be skipped (was visited in a
     * previous round AND contributed no doc to that round's collector heap — see hybrid skip in
     * {@link IVFKnnFloatVectorQuery#buildSkipCentroids}).
     */
    public boolean shouldSkipCentroid(int centroidOrd) {
        return skipCentroids != null && centroidOrd >= 0 && centroidOrd < skipCentroids.length() && skipCentroids.get(centroidOrd);
    }

    /**
     * Marks the centroid with the given ordinal as visited in this round.
     */
    public void markCentroidVisited(int centroidOrd) {
        if (visitedCentroids != null && centroidOrd >= 0 && centroidOrd < visitedCentroids.length()) {
            visitedCentroids.set(centroidOrd);
        }
    }

    /**
     * Snapshot the collector's heap state (size + min competitive score) before visiting a centroid's
     * posting list. Paired with {@link #afterCentroidVisit(int)} which detects whether any doc from
     * this centroid was inserted into the heap (size grew) or caused an eviction (min score changed).
     * No-op when {@link #trackForRetry} is false.
     */
    public void beforeCentroidVisit() {
        if (trackForRetry == false) {
            return;
        }
        AbstractMaxScoreKnnCollector c = collector.get();
        if (c == null) {
            return;
        }
        preVisitNumCollected = c.numCollected();
        preVisitMinScore = c.getMinCompetitiveDocScore();
    }

    /**
     * Mark the centroid as visited, and additionally as competitive if its visit changed the
     * collector's heap state (size grew, or min competitive score moved due to eviction).
     * No-op when {@link #trackForRetry} is false.
     */
    public void afterCentroidVisit(int centroidOrd) {
        if (trackForRetry == false) {
            return;
        }
        markCentroidVisited(centroidOrd);
        if (competitiveCentroids == null || centroidOrd < 0 || centroidOrd >= competitiveCentroids.length()) {
            return;
        }
        AbstractMaxScoreKnnCollector c = collector.get();
        if (c == null) {
            return;
        }
        if (c.numCollected() > preVisitNumCollected || c.getMinCompetitiveDocScore() != preVisitMinScore) {
            competitiveCentroids.set(centroidOrd);
        }
    }

    /**
     * Returns the set of centroids visited in this search round, or null if not tracking.
     */
    FixedBitSet visitedCentroids() {
        return visitedCentroids;
    }

    /**
     * Returns the set of centroids that contributed at least one doc to this round's collector
     * heap (possibly later evicted), or null if not tracking.
     */
    FixedBitSet competitiveCentroids() {
        return competitiveCentroids;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IVFKnnSearchStrategy that = (IVFKnnSearchStrategy) o;
        return visitRatio == that.visitRatio
            && numCands == that.numCands
            && k == that.k
            && Objects.equals(skipCentroids, that.skipCentroids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(visitRatio, numCands, k, skipCentroids);
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
