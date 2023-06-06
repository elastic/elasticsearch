/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * This class extends {@link org.apache.lucene.document.KnnByteVectorField}
 * with a single goal to override the Lucene's limit of max vector dimensions,
 * and set it to {@link org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper#MAX_DIMS_COUNT}
 */
public class XKnnByteVectorField extends KnnByteVectorField {

    private static FieldType createType(byte[] v, VectorSimilarityFunction similarityFunction) {
        if (v == null) {
            throw new IllegalArgumentException("vector value must not be null");
        }
        int dimension = v.length;
        if (dimension == 0) {
            throw new IllegalArgumentException("cannot index an empty vector");
        }
        if (dimension > DenseVectorFieldMapper.MAX_DIMS_COUNT) {
            throw new IllegalArgumentException("cannot index vectors with dimension greater than " + DenseVectorFieldMapper.MAX_DIMS_COUNT);
        }
        if (similarityFunction == null) {
            throw new IllegalArgumentException("similarity function must not be null");
        }
        FieldType type = new FieldType() {
            @Override
            public int vectorDimension() {
                return dimension;
            }

            @Override
            public VectorEncoding vectorEncoding() {
                return VectorEncoding.BYTE;
            }

            @Override
            public VectorSimilarityFunction vectorSimilarityFunction() {
                return similarityFunction;
            }
        };
        type.freeze();

        return type;
    }

    public XKnnByteVectorField(String name, byte[] vector, VectorSimilarityFunction similarityFunction) {
        super(name, vector, createType(vector, similarityFunction));
    }

    public XKnnByteVectorField(String name, byte[] vector) {
        this(name, vector, VectorSimilarityFunction.EUCLIDEAN);
    }
}
