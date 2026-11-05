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

        // shared score should now be updated
        collector.collect(2, 0.9f);
        long competitiveScore = NeighborQueue.encodeRaw(2, 0.9f);
        strategy.nextVectorsBlock();
        assertEquals(competitiveScore, sharedScore.get());
        assertEquals(competitiveScore, collector.getMinCompetitiveDocScore());

        // updated shared score directly with more competitive score
        competitiveScore = NeighborQueue.encodeRaw(3, 1.5f);
        sharedScore.publish(competitiveScore);
        assertEquals(competitiveScore, sharedScore.get());
        strategy.nextVectorsBlock();
        assertEquals(competitiveScore, collector.getMinCompetitiveDocScore());
        assertEquals(competitiveScore, sharedScore.get());
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
        long otherLeafScore = NeighborQueue.encodeRaw(3, 1.5f);
        sharedScore.publish(otherLeafScore);
        strategy.nextVectorsBlock();
        assertEquals(otherLeafScore, collector.getMinCompetitiveDocScore());
        assertEquals(otherLeafScore, sharedScore.get());
    }
}
