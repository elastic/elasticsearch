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
import org.elasticsearch.core.Nullable;

import java.util.Objects;

public class IVFKnnSearchStrategy extends KnnSearchStrategy {
    private final float visitRatio;
    private final int numCands;
    private final int k;
    private final SetOnce<AbstractMaxScoreKnnCollector> collector = new SetOnce<>();
    private final SharedMinCompetitiveScore sharedScore;

    /**
     * @param sharedScore minimum competitive score shared across leaves, or {@code null} when there is nothing to share
     */
    public IVFKnnSearchStrategy(float visitRatio, int numCands, int k, @Nullable SharedMinCompetitiveScore sharedScore) {
        this.visitRatio = visitRatio;
        this.numCands = numCands;
        this.k = k;
        this.sharedScore = sharedScore;
    }

    void setCollector(AbstractMaxScoreKnnCollector collector) {
        this.collector.set(collector);
        if (sharedScore != null) {
            collector.updateMinCompetitiveDocScore(sharedScore.get());
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
     * It publishes the minimum competitive document score from the collector to the shared score.
     * If the current shared score is greater than the minimum competitive
     * document score in the collector, it updates the collector's minimum competitive document score.
     */
    @Override
    public void nextVectorsBlock() {
        if (sharedScore == null) {
            return;
        }
        assert this.collector.get() != null : "Collector must be set before nextVectorsBlock is called";
        AbstractMaxScoreKnnCollector knnCollector = this.collector.get();
        long collectorScore = knnCollector.getMinCompetitiveDocScore();
        sharedScore.publish(collectorScore);
        long currentScore = sharedScore.get();
        if (currentScore > collectorScore) {
            knnCollector.updateMinCompetitiveDocScore(currentScore);
        }
    }
}
