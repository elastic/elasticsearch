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
        SharedMinCompetitiveScore sharedScore = new SharedMinCompetitiveScore();
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(0.5f, 100, 10, sharedScore);
        MaxScoreTopKnnCollector collector = new MaxScoreTopKnnCollector(2, 1000, strategy);
        strategy.setCollector(collector);

        collector.collect(1, 0.9f);

        // queue is not saturated, it should not be updated
        strategy.nextVectorsBlock();
        assertEquals(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE, sharedScore.get());
        assertEquals(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE, collector.getMinCompetitiveDocScore());

        // shared score should now be updated, while the collector keeps its own more competitive bound
        collector.collect(2, 0.9f);
        long competitiveScore = NeighborQueue.encodeRaw(2, 0.9f);
        strategy.nextVectorsBlock();
        assertEquals(sharedBound(0.9f), sharedScore.get());
        assertEquals(competitiveScore, collector.getMinCompetitiveDocScore());

        // updated shared score directly with more competitive score
        sharedScore.publish(NeighborQueue.encodeRaw(3, 1.5f));
        assertEquals(sharedBound(1.5f), sharedScore.get());
        strategy.nextVectorsBlock();
        assertEquals(sharedBound(1.5f), collector.getMinCompetitiveDocScore());
        assertEquals(sharedBound(1.5f), sharedScore.get());
        // a doc tying the shared score is still competitive
        assertTrue(collector.minCompetitiveSimilarity() < 1.5f);
    }

    public void testReadOnlySharedScoreIsNotPublishedTo() {
        SharedMinCompetitiveScore sharedScore = new SharedMinCompetitiveScore();
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(0.5f, 100, 10, sharedScore.readOnly());
        MaxScoreTopKnnCollector collector = new MaxScoreTopKnnCollector(2, 1000, strategy);
        strategy.setCollector(collector);

        // saturated queue: its cutoff is not published
        collector.collect(1, 0.9f);
        collector.collect(2, 0.9f);
        long collectorScore = NeighborQueue.encodeRaw(2, 0.9f);
        strategy.nextVectorsBlock();
        assertEquals(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE, sharedScore.get());
        assertEquals(collectorScore, collector.getMinCompetitiveDocScore());

        // a more competitive shared cutoff is still read
        sharedScore.publish(NeighborQueue.encodeRaw(3, 1.5f));
        strategy.nextVectorsBlock();
        assertEquals(sharedBound(1.5f), collector.getMinCompetitiveDocScore());
        assertEquals(sharedBound(1.5f), sharedScore.get());
    }

    /**
     * Leaf A publishes a cutoff whose doc id is local to A, so leaf B cannot tell whether its own doc with the same score
     * would lose the tie-break at merge. B must keep docs that tie the cutoff, and may only skip docs scoring below it.
     */
    public void testTiedScoreInAnotherLeafStaysCompetitive() {
        SharedMinCompetitiveScore sharedScore = new SharedMinCompetitiveScore();
        IVFKnnSearchStrategy strategyA = new IVFKnnSearchStrategy(0.5f, 100, 10, sharedScore);
        MaxScoreTopKnnCollector leafA = new MaxScoreTopKnnCollector(2, 1000, strategyA);
        strategyA.setCollector(leafA);
        IVFKnnSearchStrategy strategyB = new IVFKnnSearchStrategy(0.5f, 100, 10, sharedScore);
        MaxScoreTopKnnCollector leafB = new MaxScoreTopKnnCollector(2, 1000, strategyB);
        strategyB.setCollector(leafB);

        leafA.collect(5, 1f);
        leafA.collect(6, 1f);
        strategyA.nextVectorsBlock();

        leafB.collect(1, 0.5f);
        leafB.collect(2, 0.6f);
        strategyB.nextVectorsBlock();

        // readers collect a doc only when minCompetitiveSimilarity() < score
        assertTrue(leafB.minCompetitiveSimilarity() < 1f);
        assertFalse(leafB.minCompetitiveSimilarity() < 0.99f);
        assertTrue(leafB.collect(0, 1f));
    }

    /** The encoded bound {@link SharedMinCompetitiveScore#get()} returns once {@code score} is the shared score. */
    static long sharedBound(float score) {
        return NeighborQueue.encodeLeastCompetitive(Math.nextDown(score));
    }
}
