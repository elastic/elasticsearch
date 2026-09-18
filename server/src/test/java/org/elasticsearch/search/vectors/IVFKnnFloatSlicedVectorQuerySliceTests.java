/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;

public class IVFKnnFloatSlicedVectorQuerySliceTests extends AbstractIVFKnnSlicedVectorQueryTestCase {

    private float[] randomVector(int dim) {
        float[] vector = new float[dim];
        for (int i = 0; i < dim; i++) {
            vector[i] = randomFloat();
        }
        VectorUtil.l2normalize(vector);
        return vector;
    }

    @Override
    protected Field createVectorField(String name, int dimensions) {
        return new KnnFloatVectorField(name, randomVector(dimensions), VectorSimilarityFunction.EUCLIDEAN);
    }

    @Override
    protected Query createSlicedQuery(
        String field,
        int dimensions,
        int k,
        int numCands,
        Query filter,
        float visitRatio,
        BytesRef... sliceIds
    ) {
        return new IVFKnnFloatSlicedVectorQuery(
            field,
            randomVector(dimensions),
            k,
            numCands,
            filter,
            visitRatio,
            testResolver(),
            SLICE_FIELD,
            sliceIds
        );
    }

    @Override
    protected Query createToStringQuery(String field, int k, int numCands, Query filter, float visitRatio, BytesRef... sliceIds) {
        return new IVFKnnFloatSlicedVectorQuery(
            field,
            new float[] { 0.0f, 1.0f },
            k,
            numCands,
            filter,
            visitRatio,
            testResolver(),
            SLICE_FIELD,
            sliceIds
        );
    }

    @Override
    protected VectorSimilarityFunction similarityFunction() {
        return VectorSimilarityFunction.EUCLIDEAN;
    }

    @Override
    protected String queryToStringPrefix() {
        return "IVFKnnFloatSlicedVectorQuery";
    }

    @Override
    protected Object firstQueryElement() {
        return "0.0";
    }
}
