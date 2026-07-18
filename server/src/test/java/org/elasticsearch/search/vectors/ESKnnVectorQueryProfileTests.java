/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ESKnnVectorQueryProfileTests extends ESTestCase {

    public void testFloatVectorQueryProfile() throws IOException {
        try (Directory dir = newDirectory()) {
            indexFloatDocs(dir, 20, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                ESKnnFloatVectorQuery query = new ESKnnFloatVectorQuery("vector", new float[] { 1f, 2f, 3f, 4f }, 5, 10, null, null);
                query.enableProfiling();
                searcher.rewrite(query);

                QueryProfiler profiler = new QueryProfiler();
                query.profile(profiler);

                Map<String, Object> breakdown = profiler.getKnnProfileBreakdown();
                assertThat(breakdown, notNullValue());
                assertThat(breakdown.get("algorithm"), equalTo("hnsw"));
                assertThat((long) breakdown.get("total_time_ns"), greaterThan(0L));

                @SuppressWarnings("unchecked")
                Map<String, Object> hnsw = (Map<String, Object>) breakdown.get("hnsw");
                assertThat(hnsw, notNullValue());
                assertThat(hnsw.get("k"), equalTo(5));
                assertThat(hnsw.get("num_candidates"), equalTo(10));
                assertThat(hnsw.get("has_filter"), equalTo(false));
                assertThat((int) hnsw.get("leaf_searches"), greaterThan(0));

                @SuppressWarnings("unchecked")
                Map<String, Object> timings = (Map<String, Object>) hnsw.get("timings");
                assertThat(timings, notNullValue());
                assertThat((long) timings.get("avg_leaf_search_ns"), greaterThan(0L));

                assertThat(breakdown, not(hasKey("ivf")));
            }
        }
    }

    public void testByteVectorQueryProfile() throws IOException {
        try (Directory dir = newDirectory()) {
            indexByteDocs(dir, 20, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                ESKnnByteVectorQuery query = new ESKnnByteVectorQuery("vector", new byte[] { 1, 2, 3, 4 }, 5, 10, null, null);
                query.enableProfiling();
                searcher.rewrite(query);

                QueryProfiler profiler = new QueryProfiler();
                query.profile(profiler);

                Map<String, Object> breakdown = profiler.getKnnProfileBreakdown();
                assertThat(breakdown, notNullValue());
                assertThat(breakdown.get("algorithm"), equalTo("hnsw"));

                @SuppressWarnings("unchecked")
                Map<String, Object> hnsw = (Map<String, Object>) breakdown.get("hnsw");
                assertThat(hnsw, notNullValue());
                assertThat(hnsw.get("k"), equalTo(5));
                assertThat((int) hnsw.get("leaf_searches"), greaterThan(0));
            }
        }
    }

    public void testFloatVectorQueryProfileWithFilter() throws IOException {
        try (Directory dir = newDirectory()) {
            indexFloatDocs(dir, 20, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Query filter = new TermQuery(new Term("id", "doc_5"));
                ESKnnFloatVectorQuery query = new ESKnnFloatVectorQuery("vector", new float[] { 1f, 2f, 3f, 4f }, 5, 10, filter, null);
                query.enableProfiling();
                searcher.rewrite(query);

                QueryProfiler profiler = new QueryProfiler();
                query.profile(profiler);

                Map<String, Object> breakdown = profiler.getKnnProfileBreakdown();
                assertThat(breakdown, notNullValue());

                @SuppressWarnings("unchecked")
                Map<String, Object> hnsw = (Map<String, Object>) breakdown.get("hnsw");
                assertThat(hnsw.get("has_filter"), equalTo(true));
            }
        }
    }

    public void testFloatVectorQueryMergeTime() throws IOException {
        try (Directory dir = newDirectory()) {
            indexFloatDocs(dir, 20, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                ESKnnFloatVectorQuery query = new ESKnnFloatVectorQuery("vector", new float[] { 1f, 2f, 3f, 4f }, 5, 10, null, null);
                query.enableProfiling();
                searcher.rewrite(query);

                Map<String, Object> map = profiler(query).getKnnProfileBreakdown();
                assertThat(map, hasKey("merge_time_ns"));

                @SuppressWarnings("unchecked")
                Map<String, Object> hnsw = (Map<String, Object>) map.get("hnsw");
                @SuppressWarnings("unchecked")
                Map<String, Object> timings = (Map<String, Object>) hnsw.get("timings");
                assertThat(timings, not(hasKey("merge_ns")));
            }
        }
    }

    public void testVectorOpsCountTracked() throws IOException {
        try (Directory dir = newDirectory()) {
            indexFloatDocs(dir, 20, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                ESKnnFloatVectorQuery query = new ESKnnFloatVectorQuery("vector", new float[] { 1f, 2f, 3f, 4f }, 5, 10, null, null);
                searcher.rewrite(query);

                QueryProfiler profiler = new QueryProfiler();
                query.profile(profiler);
                assertThat(profiler.getVectorOpsCount(), greaterThan(0L));
            }
        }
    }

    /**
     * The query-phase mechanism: with a {@link QueryProfiler} attached to a {@link ContextIndexSearcher},
     * the query self-enables and self-publishes its breakdown during {@code rewrite()} alone — no explicit
     * {@code enableProfiling()}/{@code profile()} calls (those are only needed on a plain IndexSearcher).
     */
    public void testSelfPublishesWithProfilerOnContextSearcher() throws IOException {
        try (Directory dir = newDirectory()) {
            indexFloatDocs(dir, 20, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = contextSearcher(reader);
                QueryProfiler profiler = new QueryProfiler();
                searcher.setProfiler(profiler);

                ESKnnFloatVectorQuery query = new ESKnnFloatVectorQuery("vector", new float[] { 1f, 2f, 3f, 4f }, 5, 10, null, null);
                searcher.rewrite(query);

                Map<String, Object> breakdown = profiler.getKnnProfileBreakdown();
                assertThat(breakdown, notNullValue());
                assertThat(breakdown.get("algorithm"), equalTo("hnsw"));
                assertThat((long) breakdown.get("total_time_ns"), greaterThan(0L));
                assertThat(profiler.getVectorOpsCount(), greaterThan(0L));
            }
        }
    }

    /**
     * A {@link ContextIndexSearcher} without a profiler must not collect a breakdown: {@code rewrite()} runs
     * but nothing is published, so a fresh profiler stays empty.
     */
    public void testNoBreakdownWhenProfilerAbsent() throws IOException {
        try (Directory dir = newDirectory()) {
            indexFloatDocs(dir, 20, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = contextSearcher(reader);
                // no setProfiler(...)
                ESKnnFloatVectorQuery query = new ESKnnFloatVectorQuery("vector", new float[] { 1f, 2f, 3f, 4f }, 5, 10, null, null);
                searcher.rewrite(query);

                QueryProfiler profiler = new QueryProfiler();
                query.profile(profiler);
                // profileData was never allocated, so no detailed breakdown — only the lightweight ops count.
                assertThat(profiler.getKnnProfileBreakdown(), nullValue());
            }
        }
    }

    private static ContextIndexSearcher contextSearcher(IndexReader reader) throws IOException {
        return new ContextIndexSearcher(
            reader,
            IndexSearcher.getDefaultSimilarity(),
            IndexSearcher.getDefaultQueryCache(),
            TrivialQueryCachingPolicy.ALWAYS,
            true
        );
    }

    private QueryProfiler profiler(ESKnnFloatVectorQuery query) {
        QueryProfiler profiler = new QueryProfiler();
        query.profile(profiler);
        return profiler;
    }

    private void indexFloatDocs(Directory dir, int numDocs, int dim) throws IOException {
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                float[] vec = new float[dim];
                for (int d = 0; d < dim; d++) {
                    vec[d] = randomFloat();
                }
                doc.add(new KnnFloatVectorField("vector", vec, VectorSimilarityFunction.EUCLIDEAN));
                doc.add(new StringField("id", "doc_" + i, Field.Store.YES));
                w.addDocument(doc);
            }
        }
    }

    private void indexByteDocs(Directory dir, int numDocs, int dim) throws IOException {
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                byte[] vec = new byte[dim];
                random().nextBytes(vec);
                doc.add(new KnnByteVectorField("vector", vec, VectorSimilarityFunction.EUCLIDEAN));
                doc.add(new StringField("id", "doc_" + i, Field.Store.YES));
                w.addDocument(doc);
            }
        }
    }
}
