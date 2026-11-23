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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

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

    /**
     * A leaf that searches with another vector than the query's, such as one preconditioned for its segment, gets
     * {@link QueryTransformCache#NONE} rather than the query's cache, which holds transformations of the query's vector.
     */
    public void testLeafWithAnotherVectorGetsNoTransformCache() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", vector(0, 1), vector(1, 2), vector(0, 0));
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            for (boolean copiesQuery : new boolean[] { false, true }) {
                TransformCacheCapturingQuery query = new TransformCacheCapturingQuery(new float[] { 0.0f, 1.0f }, copiesQuery);
                newSearcher(reader).search(query, 10);
                assertThat(query.captured, not(empty()));
                if (copiesQuery) {
                    assertThat(query.captured, everyItem(sameInstance(QueryTransformCache.NONE)));
                } else {
                    assertThat(query.captured, everyItem(not(sameInstance(QueryTransformCache.NONE))));
                }
            }
        }
    }

    /** Records the transform cache of each leaf's search strategy, searching with a copy of the query vector if asked. */
    private class TransformCacheCapturingQuery extends IVFKnnFloatVectorQuery {
        private final boolean copiesQuery;
        private final List<QueryTransformCache> captured = Collections.synchronizedList(new ArrayList<>());

        TransformCacheCapturingQuery(float[] query, boolean copiesQuery) {
            super("field", query, 10, 10, null, 0.05f, testResolver());
            this.copiesQuery = copiesQuery;
        }

        @Override
        protected float[] segmentQuery(LeafReaderContext context, boolean usePrecondition) throws IOException {
            float[] segmentQuery = super.segmentQuery(context, usePrecondition);
            return copiesQuery ? segmentQuery.clone() : segmentQuery;
        }

        @Override
        protected IVFCollectorManager getKnnCollectorManager(
            int k,
            SharedMinCompetitiveScore sharedScore,
            QueryTransformCache queryTransformCache
        ) {
            return new IVFCollectorManager(k, sharedScore, queryTransformCache) {
                @Override
                IVFKnnSearchStrategy newSearchStrategy(float visitRatio, int numCands, int queryK, boolean searchesQueryVector) {
                    IVFKnnSearchStrategy strategy = super.newSearchStrategy(visitRatio, numCands, queryK, searchesQueryVector);
                    captured.add(strategy.getQueryTransformCache());
                    return strategy;
                }
            };
        }
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
