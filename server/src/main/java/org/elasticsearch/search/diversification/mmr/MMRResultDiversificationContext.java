/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification.mmr;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.diversification.ResultDiversificationContext;
import org.elasticsearch.search.vectors.VectorData;

import java.util.Map;

public class MMRResultDiversificationContext extends ResultDiversificationContext {

    private final float lambda;

    public MMRResultDiversificationContext(
        String field,
        float lambda,
        int numCandidates,
        @Nullable VectorData queryVector,
        @Nullable Map<Integer, VectorData> fieldVectors
    ) {
        super(field, numCandidates, queryVector, fieldVectors);
        this.lambda = lambda;
    }

    public float getLambda() {
        return lambda;
    }
}
