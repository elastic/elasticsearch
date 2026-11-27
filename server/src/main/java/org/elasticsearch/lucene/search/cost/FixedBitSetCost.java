/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.util.FixedBitSet;

/**
 * The heap of a {@code FixedBitSet(maxDoc)}, the dense per-leaf doc set that scorers, the query cache and
 * {@code DocIdSetBuilder} fall back to. Shared so that every estimator sizes the same set the same way; what holds on to the
 * set (an iterator, a {@code DocIdSet}, a builder) is for the caller to add.
 */
public final class FixedBitSetCost {

    /** The {@code FixedBitSet} object and the header of its {@code long[]}. Conservative without compressed oops. */
    public static final long BASE_BYTES = 56L;

    /** The {@code BitSetIterator} a scorer reads the set through. Conservative without compressed oops. */
    public static final long ITERATOR_BYTES = 40L;

    private FixedBitSetCost() {}

    /** The bit set over a leaf of {@code maxDoc} documents. */
    public static long bytes(int maxDoc) {
        return BASE_BYTES + (long) FixedBitSet.bits2words(maxDoc) * Long.BYTES;
    }

    /** The bit set a scorer keeps for a leaf of {@code maxDoc} documents, together with the iterator it reads it through. */
    public static long scorerBytes(int maxDoc) {
        return bytes(maxDoc) + ITERATOR_BYTES;
    }
}
