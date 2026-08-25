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
import org.elasticsearch.index.codec.vectors.VectorTestUtils;

public class DenseVectorQueryFloatsTests extends AbstractDenseVectorQueryTestCase {
    @Override
    DenseVectorQuery getDenseVectorQuery(String field, float[] query) {
        return DenseVectorQuery.Floats.codecScored(query, field);
    }

    @Override
    Query getDenseVectorQuery(String field, float[] query, Query filter) {
        return DenseVectorQuery.Floats.codecScored(query, field).filteredBy(filter);
    }

    @Override
    float[] randomVector(int dim) {
        return VectorTestUtils.randomFloatVector(dim);
    }

    @Override
    Field getKnnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
        return new KnnFloatVectorField(name, vector, similarityFunction);
    }
}
