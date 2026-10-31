/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.elasticsearch.test.ESTestCase;

/** Tests that post-filter rounds do not use score floors from patience-terminated HNSW searches. */
public class PostFilterEarlyTerminationTests extends ESTestCase {

    private static final String FIELD = "vector";
    private static final int K = 3;
    private static final int NUM_CANDS = 10;
    private static final float SELECTIVITY = 0.8f;
    private static final KnnSearchStrategy STRATEGY = KnnSearchStrategy.Hnsw.DEFAULT;
    private static final BitSetProducer PARENTS = context -> null;

    public void testFloatDelegateDisablesEarlyTermination() {
        ESKnnFloatVectorQuery original = new ESKnnFloatVectorQuery(FIELD, new float[] { 1f }, K, NUM_CANDS, null, STRATEGY, true);
        ESKnnFloatVectorQuery delegate = (ESKnnFloatVectorQuery) original.createPostFilterDelegate(SELECTIVITY);

        assertEarlyTerminationEnabled(original.getKnnCollectorManager(K, null));
        assertEarlyTerminationDisabled(delegate.getKnnCollectorManager(delegate.k(), null));
    }

    public void testByteDelegateDisablesEarlyTermination() {
        ESKnnByteVectorQuery original = new ESKnnByteVectorQuery(FIELD, new byte[] { 1 }, K, NUM_CANDS, null, STRATEGY, true);
        ESKnnByteVectorQuery delegate = (ESKnnByteVectorQuery) original.createPostFilterDelegate(SELECTIVITY);

        assertEarlyTerminationEnabled(original.getKnnCollectorManager(K, null));
        assertEarlyTerminationDisabled(delegate.getKnnCollectorManager(delegate.k(), null));
    }

    public void testDiversifyingFloatDelegateDisablesEarlyTermination() {
        ESDiversifyingChildrenFloatKnnVectorQuery original = new ESDiversifyingChildrenFloatKnnVectorQuery(
            FIELD,
            new float[] { 1f },
            null,
            K,
            NUM_CANDS,
            PARENTS,
            STRATEGY,
            true
        );
        ESDiversifyingChildrenFloatKnnVectorQuery delegate = (ESDiversifyingChildrenFloatKnnVectorQuery) original.createPostFilterDelegate(
            SELECTIVITY
        );

        assertEarlyTerminationEnabled(original.getKnnCollectorManager(K, null));
        assertEarlyTerminationDisabled(delegate.getKnnCollectorManager(delegate.k(), null));
    }

    public void testDiversifyingByteDelegateDisablesEarlyTermination() {
        ESDiversifyingChildrenByteKnnVectorQuery original = new ESDiversifyingChildrenByteKnnVectorQuery(
            FIELD,
            new byte[] { 1 },
            null,
            K,
            NUM_CANDS,
            PARENTS,
            STRATEGY,
            true
        );
        ESDiversifyingChildrenByteKnnVectorQuery delegate = (ESDiversifyingChildrenByteKnnVectorQuery) original.createPostFilterDelegate(
            SELECTIVITY
        );

        assertEarlyTerminationEnabled(original.getKnnCollectorManager(K, null));
        assertEarlyTerminationDisabled(delegate.getKnnCollectorManager(delegate.k(), null));
    }

    private static void assertEarlyTerminationEnabled(KnnCollectorManager manager) {
        assertTrue(manager instanceof PatienceCollectorManager);
    }

    private static void assertEarlyTerminationDisabled(KnnCollectorManager manager) {
        assertFalse(manager instanceof PatienceCollectorManager);
    }
}
