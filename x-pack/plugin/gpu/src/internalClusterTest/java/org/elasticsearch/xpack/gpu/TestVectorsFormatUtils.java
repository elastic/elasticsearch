/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import static org.elasticsearch.test.ESTestCase.randomFrom;

class TestVectorsFormatUtils {
    static DenseVectorFieldMapper.VectorSimilarity randomGPUSupportedSimilarity(DenseVectorFieldMapper.VectorIndexType vectorIndexType) {
        if (vectorIndexType == DenseVectorFieldMapper.VectorIndexType.INT8_HNSW) {
            return randomFrom(
                DenseVectorFieldMapper.VectorSimilarity.L2_NORM,
                DenseVectorFieldMapper.VectorSimilarity.COSINE,
                DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT
            );
        }
        return randomFrom(DenseVectorFieldMapper.VectorSimilarity.values());
    }
}
