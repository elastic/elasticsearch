/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;

import java.io.IOException;

final class DefaultESVectorizationProvider extends ESVectorizationProvider {
    private final ESVectorUtilSupport vectorUtilSupport;

    DefaultESVectorizationProvider() {
        vectorUtilSupport = new DefaultESVectorUtilSupport();
    }

    @Override
    public ESVectorUtilSupport getVectorUtilSupport() {
        return vectorUtilSupport;
    }

    @Override
    public ES91OSQVectorsScorer newES91OSQVectorsScorer(IndexInput input, int dimension) throws IOException {
        return new ES91OSQVectorsScorer(input, dimension);
    }
}
