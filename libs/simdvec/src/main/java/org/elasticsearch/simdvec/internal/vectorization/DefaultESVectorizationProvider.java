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
import org.elasticsearch.simdvec.ES91Int4VectorsScorer;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ES92Int7VectorsScorer;

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
    public ES91OSQVectorsScorer newES91OSQVectorsScorer(IndexInput input, int dimension) {
        return new ES91OSQVectorsScorer(input, dimension);
    }

    @Override
    public ES91Int4VectorsScorer newES91Int4VectorsScorer(IndexInput input, int dimension) {
        return new ES91Int4VectorsScorer(input, dimension);
    }

    @Override
    public ES92Int7VectorsScorer newES92Int7VectorsScorer(IndexInput input, int dimension) {
        return new ES92Int7VectorsScorer(input, dimension);
    }
}
