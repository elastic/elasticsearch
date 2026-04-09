/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Locale;

public enum SimilarityMeasure {
    COSINE(DenseVectorFieldMapper.VectorSimilarity.COSINE),
    DOT_PRODUCT(DenseVectorFieldMapper.VectorSimilarity.DOT_PRODUCT),
    L2_NORM(DenseVectorFieldMapper.VectorSimilarity.L2_NORM),;

    private final DenseVectorFieldMapper.VectorSimilarity vectorSimilarity;

    SimilarityMeasure(DenseVectorFieldMapper.VectorSimilarity vectorSimilarity) {
        this.vectorSimilarity = vectorSimilarity;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public DenseVectorFieldMapper.VectorSimilarity vectorSimilarity() {
        return vectorSimilarity;
    }

    public static SimilarityMeasure fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }
}
