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

public class MaxScoreTopKnnCollectorTests extends ESTestCase {

    public void testMaxScorePropagation() {
        MaxScoreTopKnnCollector collector = new MaxScoreTopKnnCollector(2, 1000, new IVFKnnSearchStrategy(0.5f, 100, 10, null));
        long competitiveScore = NeighborQueue.encodeRaw(1, 0.9f);

        assertEquals(competitiveScore, collector.mergeMinCompetitiveDocScore(competitiveScore));
        // A shared score is competitive even before the local queue fills.
        assertEquals(0.9f, collector.minCompetitiveSimilarity(), 0f);
        collector.collect(2, 1.5f);
        assertEquals(0.9f, collector.minCompetitiveSimilarity(), 0f);

        // we ignore queue min until we've collected k results
        assertEquals(competitiveScore, collector.mergeMinCompetitiveDocScore(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE));
        collector.collect(3, 1.9f);

        // min competitive for this collector is more than global once we have k
        assertEquals(
            NeighborQueue.encodeRaw(2, 1.5f),
            collector.mergeMinCompetitiveDocScore(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE)
        );

        // we have collected k results, min competitive is the min value collected
        assertEquals(1.5f, collector.minCompetitiveSimilarity(), 0.0f);
        // we update the global min competitive doc score with a new value
        competitiveScore = NeighborQueue.encodeRaw(4, 4f);
        assertEquals(competitiveScore, collector.mergeMinCompetitiveDocScore(competitiveScore));
        assertEquals(4f, collector.minCompetitiveSimilarity(), 0.0f);
    }

    public void testUpdateMinCompetitiveDocScoreWithPartialQueue() {
        MaxScoreTopKnnCollector collector = new MaxScoreTopKnnCollector(2, 1000, new IVFKnnSearchStrategy(0.5f, 100, 10, null));
        collector.collect(2, 2.0f);

        long globalCompetitiveScore = NeighborQueue.encodeRaw(10, 0.2f);
        assertEquals(globalCompetitiveScore, collector.mergeMinCompetitiveDocScore(globalCompetitiveScore));
        assertEquals(0.2f, collector.minCompetitiveSimilarity(), 0f);
    }

}
