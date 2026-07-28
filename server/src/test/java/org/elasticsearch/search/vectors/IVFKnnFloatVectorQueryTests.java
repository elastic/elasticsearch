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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;

import java.io.IOException;

public class IVFKnnFloatVectorQueryTests extends AbstractIVFKnnVectorQueryTestCase<float[]> {

    @Override
    float[] vector(int... components) {
        float[] v = new float[components.length];
        for (int i = 0; i < components.length; i++) {
            v[i] = components[i];
        }
        return v;
    }

    @Override
    float[][] createVectorArray(int size) {
        return new float[size][];
    }

    @Override
    IVFKnnFloatVectorQuery getKnnVectorQuery(String field, float[] query, int k, Query queryFilter, float visitRatio) {
        return new IVFKnnFloatVectorQuery(field, query, k, k, queryFilter, visitRatio, testResolver());
    }

    @Override
    float[] randomVector(int dim) {
        return VectorTestUtils.randomNormalizedFloatVector(dim);
    }

    @Override
    Field getKnnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
        return new KnnFloatVectorField(name, vector, similarityFunction);
    }

    @Override
    Field getKnnVectorField(String name, float[] vector) {
        return new KnnFloatVectorField(name, vector);
    }

    public void testToString() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", vector(0, 1), vector(1, 2), vector(0, 0));
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new float[] { 0.0f, 1.0f }, 10);
            assertEquals("IVFKnnFloatVectorQuery:field[0.0,...][10]", query.toString("ignored"));

            assertDocScoreQueryToString(query.rewrite(newSearcher(reader)));

            // test with filter
            Query filter = new TermQuery(new Term("id", "text"));
            query = getKnnVectorQuery("field", new float[] { 0.0f, 1.0f }, 10, filter);
            assertEquals("IVFKnnFloatVectorQuery:field[0.0,...][10][id:text]", query.toString("ignored"));
        }
    }
}
