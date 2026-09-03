/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.simdvec.internal.vectorization;

import org.elasticsearch.simdvec.AshScorer;

import java.io.IOException;

public final class ESNextAshBBQVectorsScorer implements AshScorer<byte[]> {

    private final BBQDotProduct dotProduct;

    public ESNextAshBBQVectorsScorer(BBQDotProduct dotProduct) {
        this.dotProduct = dotProduct;
    }

    @Override
    public float score(byte[] query) throws IOException {
        return dotProduct.dotProduct(query);
    }

    @Override
    public void scoreBulk(byte[] query, int blockSize, float[] scores) throws IOException {
        dotProduct.dotProductBulk(query, blockSize, scores);
    }
}
