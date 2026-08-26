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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.CentroidIndexFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.TestIvfQueryConfigResolver;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.profile.query.QueryProfiler;

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

    public void testProfileDataCollected() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new float[] { 0, 0 }, 3);
            query.enableProfiling();
            searcher.rewrite(query);

            assertNotNull("profileData should be set after rewrite", query.profileData);
            assertEquals("ivf", query.profileData.toMap().get("algorithm"));
            assertEquals("field", query.profileData.toMap().get("field"));
            assertTrue("total_time_ns should be > 0", (long) query.profileData.toMap().get("total_time_ns") > 0);
            assertTrue("segments_searched should be > 0", (int) query.profileData.toMap().get("segments_searched") > 0);
            assertNotNull("ivf section should be present", query.profileData.toMap().get("ivf"));

            @SuppressWarnings("unchecked")
            java.util.List<java.util.Map<String, Object>> segments = (java.util.List<java.util.Map<String, Object>>) query.profileData
                .toMap()
                .get("segments");
            assertNotNull("per-segment breakdown should be present", segments);
            assertFalse("at least one segment should be recorded", segments.isEmpty());
            assertNotNull("segment name should be present", segments.get(0).get("name"));
            assertTrue("doc_count should be > 0", (int) segments.get(0).get("doc_count") > 0);

            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> ivf = (java.util.Map<String, Object>) query.profileData.toMap().get("ivf");
            assertTrue("centroids_evaluated should be > 0", (int) ivf.get("centroids_evaluated") > 0);
            assertTrue("postings_scored should be > 0", (long) ivf.get("postings_scored") > 0);

            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> timings = (java.util.Map<String, Object>) ivf.get("timings");
            assertNotNull("timings should be present", timings);
            assertTrue("posting_visit_ns should be > 0", (long) timings.get("posting_visit_ns") > 0);
            assertTrue("scoring_ns should be > 0", (long) timings.get("scoring_ns") > 0);

            String scorer = (String) query.profileData.toMap().get("scorer");
            assertNotNull("scorer implementation should be captured", scorer);
            assertTrue(
                "scorer should be native/panama/scalar, got: " + scorer,
                scorer.equals("native") || scorer.equals("panama") || scorer.equals("scalar")
            );
        }
    }

    public void testProfileDataWithFilter() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            Query filter = new TermQuery(new Term("id", "id1"));
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new float[] { 0, 0 }, 3, filter);
            query.enableProfiling();
            searcher.rewrite(query);

            assertNotNull("profileData should be set after rewrite", query.profileData);
            java.util.Map<String, Object> map = query.profileData.toMap();
            assertTrue("filter_time_ns should be > 0", (long) map.get("filter_time_ns") > 0);
        }
    }

    public void testProfilePublishedWhenFilterMatchesNothing() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            ContextIndexSearcher searcher = new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                TrivialQueryCachingPolicy.ALWAYS,
                true
            );
            QueryProfiler profiler = new QueryProfiler();
            searcher.setProfiler(profiler);
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new float[] { 0, 0 }, 3, new MatchNoDocsQuery());
            searcher.rewrite(query);

            java.util.Map<String, Object> breakdown = profiler.getKnnProfileBreakdown();
            assertNotNull("knn_profile must be published even when the filter matches nothing", breakdown);
            assertEquals("ivf", breakdown.get("algorithm"));
            assertEquals("field", breakdown.get("field"));
        }
    }

    public void testProfileDataTransferredToProfiler() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new float[] { 0, 0 }, 3);
            query.enableProfiling();
            searcher.rewrite(query);

            QueryProfiler profiler = new QueryProfiler();
            query.profile(profiler);

            assertNotNull("knnProfileBreakdown should be set on profiler", profiler.getKnnProfileBreakdown());
            assertEquals("ivf", profiler.getKnnProfileBreakdown().get("algorithm"));
            assertTrue("vectorOpsCount should be > 0", profiler.getVectorOpsCount() > 0);
        }
    }
}
