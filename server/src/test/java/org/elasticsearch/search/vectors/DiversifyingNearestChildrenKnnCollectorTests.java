/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;
import org.elasticsearch.test.ESTestCase;

public class DiversifyingNearestChildrenKnnCollectorTests extends ESTestCase {

    public void testMinCompetitiveScoreIgnoresPartialQueue() {
        FixedBitSet parents = new FixedBitSet(6);
        parents.set(2);
        parents.set(5);
        DiversifyingNearestChildrenKnnCollector collector = new DiversifyingNearestChildrenKnnCollector(
            2,
            1000,
            new IVFKnnSearchStrategy(0.5f, 100, 2, null),
            parents
        );

        collector.collect(0, 0.9f);
        assertEquals(
            AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE,
            collector.mergeMinCompetitiveDocScore(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE)
        );

        long sharedCompetitiveScore = NeighborQueue.encodeRaw(4, 0.2f);
        assertEquals(sharedCompetitiveScore, collector.mergeMinCompetitiveDocScore(sharedCompetitiveScore));

        collector.collect(3, 0.8f);
        assertEquals(
            NeighborQueue.encodeRaw(3, 0.8f),
            collector.mergeMinCompetitiveDocScore(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE)
        );
    }

    public void testDoesNotPublishBeforeCollectingKParents() {
        SharedCompetitiveScore sharedScore = new SharedCompetitiveScore(2);
        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(0.5f, 100, 2, sharedScore);
        FixedBitSet parents = new FixedBitSet(6);
        parents.set(2);
        parents.set(5);
        DiversifyingNearestChildrenKnnCollector collector = new DiversifyingNearestChildrenKnnCollector(2, 1000, strategy, parents);
        strategy.setCollector(collector);

        collector.collect(0, 0.9f);
        strategy.nextVectorsBlock();
        assertEquals(AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE, sharedScore.get());

        collector.collect(3, 0.8f);
        strategy.nextVectorsBlock();
        assertEquals(NeighborQueue.encodeRaw(3, 0.8f), sharedScore.get());
    }
}
