/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

import java.util.Optional;

public final class Int7SQVectorScorer {

    // Unconditionally returns an empty optional on <= JDK 21, since the scorer is only supported on JDK 22+
    public static Optional<RandomVectorScorer> create(VectorSimilarityFunction sim, QuantizedByteVectorValues values, float[] queryVector) {
        return Optional.empty();
    }

    private Int7SQVectorScorer() {}
}
