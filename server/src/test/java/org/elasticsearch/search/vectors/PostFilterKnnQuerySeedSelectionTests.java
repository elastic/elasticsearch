/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.test.ESTestCase;

/**
 * Unit tests for {@link PostFilterKnnQuery#nearestSeedsPerLeaf}, which chooses the retry seed entry
 * points: the highest-scoring (nearest) round-0 matches per leaf, capped per leaf, returned sorted by
 * doc id so {@link SeededRetryCollectorManager} can range-partition them.
 */
public class PostFilterKnnQuerySeedSelectionTests extends ESTestCase {

    /** When a leaf has more matches than the cap, only the highest-scoring ones survive. */
    public void testKeepsNearestWhenOverCap() {
        ScoreDoc[][] perLeaf = new ScoreDoc[][] {
            {
                new ScoreDoc(10, 0.1f),
                new ScoreDoc(11, 0.9f),
                new ScoreDoc(12, 0.5f),
                new ScoreDoc(13, 0.7f),
                new ScoreDoc(14, 0.3f),
                new ScoreDoc(15, 0.8f) } };
        // top-4 by score: 0.9(11), 0.8(15), 0.7(13), 0.5(12) -> ids sorted ascending, in leaf 0
        assertArrayEquals(new int[][] { { 11, 12, 13, 15 } }, PostFilterKnnQuery.nearestSeedsPerLeaf(perLeaf, 4));
    }

    /** At or under the cap, every match is kept and each leaf's ids are sorted by doc id. */
    public void testKeepsAllWhenUnderCap() {
        ScoreDoc[][] perLeaf = new ScoreDoc[][] { { new ScoreDoc(30, 0.2f), new ScoreDoc(20, 0.9f), new ScoreDoc(25, 0.5f) } };
        assertArrayEquals(new int[][] { { 20, 25, 30 } }, PostFilterKnnQuery.nearestSeedsPerLeaf(perLeaf, 4));
    }

    /** The cap applies per leaf independently; the result is indexed by leaf ordinal. */
    public void testCapAppliesPerLeaf() {
        ScoreDoc[][] perLeaf = new ScoreDoc[][] {
            // leaf 0: 5 matches, cap 2 -> keep the two highest: 0.9(1), 0.8(4)
            { new ScoreDoc(0, 0.1f), new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.2f), new ScoreDoc(3, 0.3f), new ScoreDoc(4, 0.8f) },
            null, // a leaf with no matches
            // leaf 2: 3 matches, cap 2 -> keep 0.7(101), 0.6(100)
            { new ScoreDoc(100, 0.6f), new ScoreDoc(101, 0.7f), new ScoreDoc(102, 0.05f) } };
        assertArrayEquals(new int[][] { { 1, 4 }, null, { 100, 101 } }, PostFilterKnnQuery.nearestSeedsPerLeaf(perLeaf, 2));
    }

    /** Empty input yields an all-null per-leaf array of the same length. */
    public void testEmpty() {
        assertArrayEquals(new int[3][], PostFilterKnnQuery.nearestSeedsPerLeaf(new ScoreDoc[3][], 4));
    }
}
