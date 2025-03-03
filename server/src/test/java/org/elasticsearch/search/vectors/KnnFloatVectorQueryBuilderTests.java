/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

public class KnnFloatVectorQueryBuilderTests extends AbstractKnnVectorQueryBuilderTestCase {
    @Override
    DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.FLOAT;
    }

    @Override
    KnnVectorQueryBuilder createKnnVectorQueryBuilder(
        String fieldName,
        int k,
        int numCands,
        RescoreVectorBuilder rescoreVectorBuilder,
        Float similarity
    ) {
        float[] vector = new float[vectorDimensions];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = randomFloat();
        }
        return new KnnVectorQueryBuilder(fieldName, vector, k, numCands, rescoreVectorBuilder, similarity);
    }

    @Override
    protected String randomIndexType() {
        return randomFrom(ALL_INDEX_TYPES);
    }
}
