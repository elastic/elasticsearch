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

import java.util.concurrent.atomic.LongAccumulator;

public class IVFKnnSearchStrategyTests extends ESTestCase {

    public void testMaxScorePropagation() {
        LongAccumulator accumulator = new LongAccumulator(Long::max, AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE);
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(0.5f, accumulator);
        MaxScoreTopKnnCollector collector = new MaxScoreTopKnnCollector(2, 1000, strategy);
        strategy.setCollector(collector);

        collector.collect(1, 0.9f);
        long competitiveScore = NeighborQueue.encodeRaw(1, 0.9f);

        // accumulator should now be updated
        strategy.nextVectorsBlock();
        assertEquals(competitiveScore, accumulator.get());
        assertEquals(competitiveScore, collector.getMinCompetitiveDocScore());

        // updated accumulator directly with more competitive score
        competitiveScore = NeighborQueue.encodeRaw(2, 1.5f);
        accumulator.accumulate(competitiveScore);
        assertEquals(competitiveScore, accumulator.get());
        strategy.nextVectorsBlock();
        assertEquals(competitiveScore, collector.getMinCompetitiveDocScore());
        assertEquals(competitiveScore, accumulator.get());
    }
}
