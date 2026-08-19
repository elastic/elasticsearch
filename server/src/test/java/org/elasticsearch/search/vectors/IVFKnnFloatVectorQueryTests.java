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
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;

import java.io.IOException;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;

public class IVFKnnFloatVectorQueryTests extends AbstractIVFKnnVectorQueryTestCase {

    @Override
    IVFKnnFloatVectorQuery getKnnVectorQuery(String field, float[] query, int k, Query queryFilter, float visitRatio) {
        return new IVFKnnFloatVectorQuery(field, query, k, k, queryFilter, visitRatio, testResolver());
    }

    @Override
    float[] randomVector(int dim) {
        float[] vector = new float[dim];
        for (int i = 0; i < dim; i++) {
            vector[i] = randomFloat();
        }
        VectorUtil.l2normalize(vector);
        return vector;
    }

    @Override
    Field getKnnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
        return new KnnFloatVectorField(name, vector, similarityFunction);
    }

    @Override
    Field getKnnVectorField(String name, float[] vector) {
        return new KnnFloatVectorField(name, vector);
    }

    public void testEqualsDifferentResolver() {
        float[] queryVector = new float[] { 0, 1 };
        TestIvfQueryConfigResolver resolver1 = new TestIvfQueryConfigResolver(
            CentroidIndexFormat.FLAT,
            QuantEncoding.ONE_BIT_4BIT_QUERY,
            false,
            1.0f
        );
        TestIvfQueryConfigResolver resolver2 = new TestIvfQueryConfigResolver(
            CentroidIndexFormat.FLAT,
            QuantEncoding.ONE_BIT_4BIT_QUERY,
            false,
            2.0f
        );
        IVFKnnFloatVectorQuery q1 = new IVFKnnFloatVectorQuery("field", queryVector, 10, 10, null, 0.05f, resolver1);
        IVFKnnFloatVectorQuery q2 = new IVFKnnFloatVectorQuery("field", queryVector, 10, 10, null, 0.05f, resolver2);
        IVFKnnFloatVectorQuery q3 = new IVFKnnFloatVectorQuery("field", queryVector, 10, 10, null, 0.05f, resolver1);

        // Queries with different resolvers must not be equal (prevents query cache collisions)
        assertNotEquals(q1, q2);
        assertNotEquals(q1.hashCode(), q2.hashCode());

        // Queries with the same resolver config must still be equal
        assertEquals(q1, q3);
        assertEquals(q1.hashCode(), q3.hashCode());
    }

    public void testToString() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
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
