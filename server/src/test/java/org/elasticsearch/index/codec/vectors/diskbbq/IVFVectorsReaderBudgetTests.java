/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.test.ESTestCase;

/** The visit budget helpers are shared by the search loop and query-side work estimates, so they must stay in step. */
public class IVFVectorsReaderBudgetTests extends ESTestCase {

    public void testSegmentVisitRatioCombinesBaseBoostAndCap() {
        int numCands = randomIntBetween(10, 1000);
        int k = randomIntBetween(1, numCands);
        int numVectors = randomIntBetween(1, 20_000_000);
        float expected = Math.min(
            IVFVectorsReader.computeDynamicVisitRatio(numCands, k) * IVFVectorsReader.computeSmallSegmentBoost(k, numVectors),
            IVFVectorsReader.computeSegmentSizeCap(numVectors)
        );
        assertEquals(expected, IVFVectorsReader.segmentVisitRatio(numCands, k, numVectors), 0f);
    }

    public void testDefaultBudgetOfAMillionVectorSegment() {
        // k=10 and the default num_candidates of 15 scan about 1% of the vectors, i.e. ~2% of posting entries with SOAR
        float ratio = IVFVectorsReader.segmentVisitRatio(15, 10, 1_000_000);
        assertEquals(0.00976f, ratio, 1e-5f);
        assertEquals((long) (2.0 * ratio * 1_000_000), IVFVectorsReader.visitBudget(ratio, 1_000_000));
    }
}
