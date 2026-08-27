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
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

public class IVFKnnByteVectorQueryTests extends AbstractIVFKnnVectorQueryTestCase<byte[]> {

    @Before
    public void setUpByteFormat() {
        format = new ESNextDiskBBQVectorsFormat(128, 4, null);
    }

    @Override
    byte[] vector(int... components) {
        byte[] v = new byte[components.length];
        for (int i = 0; i < components.length; i++) {
            v[i] = (byte) components[i];
        }
        return v;
    }

    @Override
    byte[][] createVectorArray(int size) {
        return new byte[size][];
    }

    @Override
    IVFKnnByteVectorQuery getKnnVectorQuery(String field, byte[] query, int k, Query queryFilter, float visitRatio) {
        return new IVFKnnByteVectorQuery(field, query, k, k, queryFilter, visitRatio, testResolver());
    }

    @Override
    byte[] randomVector(int dim) {
        byte[] v = new byte[dim];
        random().nextBytes(v);
        return v;
    }

    @Override
    Field getKnnVectorField(String name, byte[] vector, VectorSimilarityFunction similarityFunction) {
        return new KnnByteVectorField(name, vector, similarityFunction);
    }

    @Override
    Field getKnnVectorField(String name, byte[] vector) {
        return new KnnByteVectorField(name, vector);
    }

    @Override
    boolean supportsCosine() {
        return false;
    }

    public void testToString() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", vector(0, 1), vector(1, 2), vector(0, 0));
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new byte[] { 0, 1 }, 10);
            assertEquals("IVFKnnByteVectorQuery:field[0,...][10]", query.toString("ignored"));

            assertDocScoreQueryToString(query.rewrite(newSearcher(reader)));

            // test with filter
            Query filter = new TermQuery(new Term("id", "text"));
            query = getKnnVectorQuery("field", new byte[] { 0, 1 }, 10, filter);
            assertEquals("IVFKnnByteVectorQuery:field[0,...][10][id:text]", query.toString("ignored"));
        }
    }

    /**
     * Byte IVF must collect the same detailed codec-level breakdown as float IVF. Guards against the
     * regression where the byte path built the strategy without profile data, producing an all-zeros
     * {@code ivf} block.
     */
    public void testProfileDataCollected() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", vector(0, 1), vector(1, 2), vector(0, 0));
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new byte[] { 0, 0 }, 3);
            query.enableProfiling();
            searcher.rewrite(query);

            assertNotNull("profileData should be set after rewrite", query.profileData);
            Map<String, Object> map = query.profileData.toMap();
            assertEquals("ivf", map.get("algorithm"));
            assertEquals("field", map.get("field"));
            assertTrue("total_time_ns should be > 0", (long) map.get("total_time_ns") > 0);
            assertTrue("segments_searched should be > 0", (int) map.get("segments_searched") > 0);
            assertNotNull("per-segment breakdown should be present", map.get("segments"));

            @SuppressWarnings("unchecked")
            Map<String, Object> ivf = (Map<String, Object>) map.get("ivf");
            assertNotNull("ivf section should be present", ivf);
            assertTrue("centroids_evaluated should be > 0", (int) ivf.get("centroids_evaluated") > 0);
            assertTrue("postings_scored should be > 0", (long) ivf.get("postings_scored") > 0);
            assertTrue("visit_ratio_used should be > 0", (float) ivf.get("visit_ratio_used") > 0f);

            @SuppressWarnings("unchecked")
            Map<String, Object> timings = (Map<String, Object>) ivf.get("timings");
            assertNotNull("timings should be present", timings);
            assertTrue("posting_visit_ns should be > 0", (long) timings.get("posting_visit_ns") > 0);
            assertTrue("scoring_ns should be > 0", (long) timings.get("scoring_ns") > 0);
        }
    }

    public void testProfilePublishedWhenFilterMatchesNothing() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", vector(0, 1), vector(1, 2), vector(0, 0));
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
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new byte[] { 0, 0 }, 3, new MatchNoDocsQuery());
            searcher.rewrite(query);

            Map<String, Object> breakdown = profiler.getKnnProfileBreakdown();
            assertNotNull("knn_profile must be published even when the filter matches nothing", breakdown);
            assertEquals("ivf", breakdown.get("algorithm"));
            assertEquals("field", breakdown.get("field"));
        }
    }

    public void testProfileDataTransferredToProfiler() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", vector(0, 1), vector(1, 2), vector(0, 0));
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            AbstractIVFKnnVectorQuery query = getKnnVectorQuery("field", new byte[] { 0, 0 }, 3);
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
