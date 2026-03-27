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

public class MaxScoreTopKnnCollectorTests extends ESTestCase {

    public void testMaxScorePropagation() {
        LongAccumulator accumulator = new LongAccumulator(Long::max, AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE);
        MaxScoreTopKnnCollector collector = new MaxScoreTopKnnCollector(2, 1000, new IVFKnnSearchStrategy(0.5f, accumulator));
        long competitiveScore = NeighborQueue.encodeRaw(1, 0.9f);

        collector.updateMinCompetitiveDocScore(competitiveScore);
        assertEquals(competitiveScore, collector.getMinCompetitiveDocScore());
        // haven't collected k results
        assertTrue(Float.NEGATIVE_INFINITY == collector.minCompetitiveSimilarity());
        collector.collect(2, 1.5f);
        assertTrue(Float.NEGATIVE_INFINITY == collector.minCompetitiveSimilarity());

        // we always provide the min competitive that this collector collected
        assertEquals(NeighborQueue.encodeRaw(2, 1.5f), collector.getMinCompetitiveDocScore());
        collector.collect(3, 1.9f);

        // min competitive for this collector is more than global
        assertEquals(NeighborQueue.encodeRaw(2, 1.5f), collector.getMinCompetitiveDocScore());

        // we have collected k results, min competitive is the min value collected
        assertEquals(1.5f, collector.minCompetitiveSimilarity(), 0.0f);
        // we update the global min competitive doc score with a new value
        competitiveScore = NeighborQueue.encodeRaw(4, 4f);
        collector.updateMinCompetitiveDocScore(competitiveScore);
        assertEquals(competitiveScore, collector.getMinCompetitiveDocScore());
        assertEquals(4f, collector.minCompetitiveSimilarity(), 0.0f);
    }

}
