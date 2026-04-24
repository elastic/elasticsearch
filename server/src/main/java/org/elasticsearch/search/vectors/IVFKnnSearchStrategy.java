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
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;

import java.util.Objects;
import java.util.concurrent.atomic.LongAccumulator;

public class IVFKnnSearchStrategy extends KnnSearchStrategy {
    private final float visitRatio;
    private final int numCands;
    private final int k;
    private final SetOnce<AbstractMaxScoreKnnCollector> collector = new SetOnce<>();
    private final LongAccumulator accumulator;
    /**
     * When non-null, IVF search uses this quantization from the mapping for query/posting scoring instead of
     * the value in {@code mivf}. When null, the on-disk {@code mivf} encoding is used.
     */
    @Nullable
    private final ESNextDiskBBQVectorsFormat.QuantEncoding searchQuantEncodingOverride;

    public IVFKnnSearchStrategy(float visitRatio, int numCands, int k, LongAccumulator accumulator) {
        this(visitRatio, numCands, k, accumulator, null);
    }

    public IVFKnnSearchStrategy(
        float visitRatio,
        int numCands,
        int k,
        LongAccumulator accumulator,
        @Nullable ESNextDiskBBQVectorsFormat.QuantEncoding searchQuantEncodingOverride
    ) {
        this.visitRatio = visitRatio;
        this.numCands = numCands;
        this.k = k;
        this.accumulator = accumulator;
        this.searchQuantEncodingOverride = searchQuantEncodingOverride;
    }

    @Nullable
    public ESNextDiskBBQVectorsFormat.QuantEncoding getSearchQuantEncodingOverride() {
        return searchQuantEncodingOverride;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IVFKnnSearchStrategy that = (IVFKnnSearchStrategy) o;
        return visitRatio == that.visitRatio
            && numCands == that.numCands
            && k == that.k
            && Objects.equals(searchQuantEncodingOverride, that.searchQuantEncodingOverride);
    }

    @Override
    public int hashCode() {
        return Objects.hash(visitRatio, numCands, k, searchQuantEncodingOverride);
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
