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
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

public class IVFKnnByteSlicedVectorQueryTests extends AbstractIVFKnnSlicedVectorQueryTestCase {

    private byte[] randomByteVector(int dim) {
        byte[] vector = new byte[dim];
        random().nextBytes(vector);
        return vector;
    }

    @Override
    protected Field createVectorField(String name, int dimensions) {
        return new KnnByteVectorField(name, randomByteVector(dimensions), VectorSimilarityFunction.DOT_PRODUCT);
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
        return new IVFKnnByteSlicedVectorQuery(
            field,
            randomByteVector(dimensions),
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
        return new IVFKnnByteSlicedVectorQuery(
            field,
            new byte[] { 0, 1 },
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
        return VectorSimilarityFunction.DOT_PRODUCT;
    }

    @Override
    protected String queryToStringPrefix() {
        return "IVFKnnByteSlicedVectorQuery";
    }

    @Override
    protected Object firstQueryElement() {
        return "0";
    }
}
