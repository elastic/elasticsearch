/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.util.Optional;

/**
 * Outlines the Int7 OSQ query-time scorer. The concrete implementation will
 * connect to the native OSQ routines and apply the similarity-specific
 * corrections.
 */
public final class Int7uOSQVectorScorer {

    public static Optional<RandomVectorScorer> create(
        VectorSimilarityFunction sim,
        QuantizedByteVectorValues values,
        byte[] quantizedQuery,
        float lowerInterval,
        float upperInterval,
        float additionalCorrection,
        int quantizedComponentSum
    ) {
        // TODO add JDK21 fallback logic and native scorer dispatch
        return Optional.empty();
    }

    private Int7uOSQVectorScorer() {}
}
