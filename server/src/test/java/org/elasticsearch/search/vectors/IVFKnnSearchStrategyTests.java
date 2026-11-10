/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;
import org.elasticsearch.test.ESTestCase;

public class IVFKnnSearchStrategyTests extends ESTestCase {

    public void testMaxScorePropagation() {
        SharedCompetitiveScore sharedScore = new SharedCompetitiveScore(2);
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(0.5f, 100, 10, sharedScore);
        MaxScoreTopKnnCollector collector = new MaxScoreTopKnnCollector(2, 1000, strategy);
        strategy.setCollector(collector);

        collector.collect(1, 0.9f);

        // queue is not saturated, it should not be updated
        strategy.nextVectorsBlock();
        assertFalse(strategy.hasSharedCompetitiveScore());
        assertFalse(strategy.hasSufficientCandidates());
        assertEquals(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE, sharedScore.get());
        assertEquals(
            AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE,
            collector.mergeMinCompetitiveDocScore(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE)
        );

        // shared score should now be updated
        collector.collect(2, 0.9f);
        long competitiveScore = NeighborQueue.encodeRaw(2, 0.9f);
        strategy.nextVectorsBlock();
        assertTrue(strategy.hasSharedCompetitiveScore());
        assertTrue(strategy.hasSufficientCandidates());
        assertEquals(competitiveScore, sharedScore.get());
        assertEquals(competitiveScore, collector.mergeMinCompetitiveDocScore(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE));

        // another collector publishes a more competitive score
        competitiveScore = NeighborQueue.encodeRaw(3, 1.5f);
        IVFKnnSearchStrategy otherStrategy = new IVFKnnSearchStrategy(0.5f, 100, 10, sharedScore);
        MaxScoreTopKnnCollector otherCollector = new MaxScoreTopKnnCollector(2, 1000, otherStrategy);
        otherStrategy.setCollector(otherCollector);
        otherCollector.collect(4, 1.6f);
        otherCollector.collect(3, 1.5f);
        otherStrategy.nextVectorsBlock();
        assertEquals(competitiveScore, sharedScore.get());
        strategy.nextVectorsBlock();
        assertEquals(competitiveScore, collector.mergeMinCompetitiveDocScore(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE));
    }

    public void testRequiredObservationsMustBePositive() {
        expectThrows(IllegalArgumentException.class, () -> new SharedCompetitiveScore(0));
    }

    public void testCombinesObservationsAcrossCollectors() {
        SharedCompetitiveScore sharedScore = new SharedCompetitiveScore(6);
        IVFKnnSearchStrategy firstStrategy = new IVFKnnSearchStrategy(0.5f, 100, 10, sharedScore);
        MaxScoreTopKnnCollector firstCollector = new MaxScoreTopKnnCollector(3, 1000, firstStrategy);
        firstStrategy.setCollector(firstCollector);
        firstCollector.collect(1, 100f);
        firstCollector.collect(2, 99f);
        firstCollector.collect(3, 98f);
        firstStrategy.nextVectorsBlock();
        assertEquals(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE, sharedScore.get());

        IVFKnnSearchStrategy secondStrategy = new IVFKnnSearchStrategy(0.5f, 100, 10, sharedScore);
        MaxScoreTopKnnCollector secondCollector = new MaxScoreTopKnnCollector(3, 1000, secondStrategy);
        secondStrategy.setCollector(secondCollector);
        secondCollector.collect(4, 3f);
        secondCollector.collect(5, 2f);
        secondCollector.collect(6, 1f);
        secondStrategy.nextVectorsBlock();
        assertEquals(NeighborQueue.encodeRaw(6, 1f), sharedScore.get());

        IVFKnnSearchStrategy consumingStrategy = new IVFKnnSearchStrategy(0.5f, 100, 10, sharedScore);
        MaxScoreTopKnnCollector consumingCollector = new MaxScoreTopKnnCollector(10, 1000, consumingStrategy);
        consumingStrategy.setCollector(consumingCollector);
        assertEquals(1f, consumingCollector.minCompetitiveSimilarity(), 0f);
    }
}
