/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.search.vectors.KnnSearchBuilder.NUM_CANDS_LIMIT;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/** Round-1 sizing arithmetic used by the IVF post-filter path. */
public class PostFilterSizingTests extends ESTestCase {

    /**
     * {@code Math.clamp} throws when {@code min > max}, and the {@code 1.2x} floor crosses
     * {@code NUM_CANDS_LIMIT} at {@code k = 8334}. IVF reaches this sooner than a bare {@code k} would: it
     * sizes from {@code postFilterExpectedBaseQueryDocMatches()}, so at the default {@code bbq_disk} oversample of 3 a user
     * {@code k} of ~2779 already produces a pool above the threshold. Sizing must saturate, not throw.
     */
    public void testScaledKSaturatesInsteadOfThrowing() {
        for (int k : new int[] { 2778, 2779, 8332, 8333, 8334, 9000, NUM_CANDS_LIMIT }) {
            for (float selectivity : new float[] { 0.05f, 0.5f, 0.999f, 1.0f }) {
                int scaledK = PostFilterableKnnQuery.computeScaledK(k, selectivity);
                assertThat("k=" + k + " selectivity=" + selectivity, scaledK, greaterThanOrEqualTo(1));
                assertThat("k=" + k + " selectivity=" + selectivity, scaledK, lessThanOrEqualTo(NUM_CANDS_LIMIT));
            }
        }
        // and the ordinary case still follows the binomial model: 2.5*sqrt(10*0.5/0.5) = 7.905 -> ceil(17.905/0.5)
        assertEquals(36, PostFilterableKnnQuery.computeScaledK(10, 0.5f));
    }

    /**
     * IVF reads exploration effort off the {@code numCands/k} ratio, so moving {@code k} without moving
     * {@code numCands} silently re-tunes how much of a segment gets scanned.
     */
    public void testNumCandsPreservingRatioHoldsTheRatio() {
        assertEquals(300, PostFilterableKnnQuery.numCandsPreservingRatio(100, 10, 30));
        assertEquals(30, PostFilterableKnnQuery.numCandsPreservingRatio(100, 10, 3));
        assertEquals(6, PostFilterableKnnQuery.numCandsPreservingRatio(20, 10, 3));
    }

    public void testNumCandsPreservingRatioClampsToKAndLimit() {
        assertEquals("never below the new k", 30, PostFilterableKnnQuery.numCandsPreservingRatio(5, 10, 30));
        assertEquals(NUM_CANDS_LIMIT, PostFilterableKnnQuery.numCandsPreservingRatio(NUM_CANDS_LIMIT, 10, NUM_CANDS_LIMIT));
        assertEquals("a k of 0 cannot define a ratio", 7, PostFilterableKnnQuery.numCandsPreservingRatio(3, 0, 7));
    }
}
