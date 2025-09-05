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

public class DenseVectorQueryBytesTests extends AbstractDenseVectorQueryTestCase {
    @Override
    DenseVectorQuery getDenseVectorQuery(String field, float[] query) {
        byte[] bytes = new byte[query.length];
        for (int i = 0; i < query.length; i++) {
            bytes[i] = (byte) query[i];
        }
        return new DenseVectorQuery.Bytes(bytes, field);
    }

    @Override
    float[] randomVector(int dim) {
        byte[] bytes = new byte[dim];
        random().nextBytes(bytes);
        float[] floats = new float[dim];
        for (int i = 0; i < dim; i++) {
            floats[i] = bytes[i];
        }
        return floats;
    }

    @Override
    Field getKnnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
        byte[] bytes = new byte[vector.length];
        for (int i = 0; i < vector.length; i++) {
            bytes[i] = (byte) vector[i];
        }
        return new KnnByteVectorField(name, bytes, similarityFunction);
    }
}
