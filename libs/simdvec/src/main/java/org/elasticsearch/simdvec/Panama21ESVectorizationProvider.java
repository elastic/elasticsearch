/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport;
import org.elasticsearch.simdvec.internal.vectorization.PanamaESVectorUtilSupport;

final class Panama21ESVectorizationProvider extends ESVectorizationProvider {
    @Override
    ESVectorUtilSupport getVectorUtilSupport() {
        return new PanamaESVectorUtilSupport();
    }

    @Override
    public VectorScorerFactory getVectorScorerFactory() {
        return new Panama21VectorScorerFactory();
    }
}
