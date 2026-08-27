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
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
                assertThat((int) breakdown.get("segments_searched"), greaterThan(0));
                assertThat(breakdown.get("field"), equalTo("vector"));

                @SuppressWarnings("unchecked")
                Map<String, Object> timings = (Map<String, Object>) hnsw.get("timings");
                assertThat(timings, notNullValue());
                assertThat((long) timings.get("avg_leaf_search_ns"), greaterThan(0L));
                assertThat(timings, hasKey("overhead_ns"));

                // Per-segment detail is resolved from the real LeafReaderContexts, which is why it is asserted
                // here rather than in KnnSearchProfileDataTests.
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> segments = (List<Map<String, Object>>) breakdown.get("segments");
                assertThat(segments, notNullValue());
                assertThat(segments.size(), equalTo(breakdown.get("segments_searched")));
                assertThat(segments.get(0).get("name"), notNullValue());
                assertThat((int) segments.get(0).get("doc_count"), greaterThan(0));
                assertThat((int) segments.get(0).get("vector_count"), greaterThan(0));
                assertThat((long) segments.get(0).get("vector_bytes"), greaterThan(0L));
                assertThat((long) segments.get(0).get("size_in_bytes"), greaterThan(0L));
                assertThat((long) segments.get(0).get("search_time_ns"), greaterThan(0L));
                assertThat((long) segments.get(0).get("nodes_visited"), greaterThan(0L));
                // HNSW leaves have no visit ratio; that is IVF-only.
                assertThat(segments.get(0), not(hasKey("visit_ratio_used")));

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
                assertThat(breakdown.get("field"), equalTo("vector"));
                assertThat((long) breakdown.get("total_time_ns"), greaterThan(0L));

                @SuppressWarnings("unchecked")
                Map<String, Object> hnsw = (Map<String, Object>) breakdown.get("hnsw");
                assertThat(hnsw, notNullValue());
                assertThat(hnsw.get("k"), equalTo(5));
                assertThat(hnsw.get("num_candidates"), equalTo(10));
                assertThat((int) breakdown.get("segments_searched"), greaterThan(0));
                assertThat((long) hnsw.get("nodes_visited"), greaterThan(0L));

                @SuppressWarnings("unchecked")
                Map<String, Object> timings = (Map<String, Object>) hnsw.get("timings");
                assertThat(timings, notNullValue());
                assertThat((long) timings.get("avg_leaf_search_ns"), greaterThan(0L));

                assertThat(breakdown, not(hasKey("ivf")));
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
                // The remainder of total_time_ns after per-leaf search and merge. It is floored at zero
                // because the per-leaf times are a sum over leaves searched in parallel and can exceed the
                // wall-clock total.
                assertThat((long) timings.get("overhead_ns"), greaterThanOrEqualTo(0L));
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
     * Several kNN queries sharing one {@link QueryProfiler} (as happens in the query phase for a bool with
     * multiple {@code knn} clauses) must each contribute a breakdown rather than overwriting a single slot.
     * The collapsed view wraps them under {@code knn_queries}.
     */
    public void testMultipleKnnQueriesAccumulate() throws IOException {
        try (Directory dir = newDirectory()) {
            indexFloatDocs(dir, 20, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = contextSearcher(reader);
                QueryProfiler profiler = new QueryProfiler();
                searcher.setProfiler(profiler);

                searcher.rewrite(new ESKnnFloatVectorQuery("vector", new float[] { 1f, 2f, 3f, 4f }, 5, 10, null, null));
                searcher.rewrite(new ESKnnFloatVectorQuery("vector", new float[] { 4f, 3f, 2f, 1f }, 3, 8, null, null));

                assertThat(profiler.getKnnProfileBreakdowns().size(), equalTo(2));

                Map<String, Object> collapsed = profiler.getKnnProfileBreakdown();
                assertThat(collapsed, hasKey("knn_queries"));
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> queries = (List<Map<String, Object>>) collapsed.get("knn_queries");
                assertThat(queries.size(), equalTo(2));
                assertThat(queries.get(0).get("algorithm"), equalTo("hnsw"));
                assertThat(queries.get(1).get("algorithm"), equalTo("hnsw"));
                // k is per-query, confirming the two breakdowns are distinct
                @SuppressWarnings("unchecked")
                Map<String, Object> hnsw0 = (Map<String, Object>) queries.get(0).get("hnsw");
                @SuppressWarnings("unchecked")
                Map<String, Object> hnsw1 = (Map<String, Object>) queries.get(1).get("hnsw");
                assertThat(hnsw0.get("k"), equalTo(5));
                assertThat(hnsw1.get("k"), equalTo(3));
            }
        }
    }

    /**
     * {@code rewrite()} carries no once-only contract, so a query that is rewritten twice against the same
     * profiler must still publish exactly one breakdown and count its vector ops once.
     */
    public void testSelfPublishIsIdempotentAcrossRewrites() throws IOException {
        try (Directory dir = newDirectory()) {
            indexFloatDocs(dir, 20, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = contextSearcher(reader);
                QueryProfiler profiler = new QueryProfiler();
                searcher.setProfiler(profiler);

                ESKnnFloatVectorQuery query = new ESKnnFloatVectorQuery("vector", new float[] { 1f, 2f, 3f, 4f }, 5, 10, null, null);
                searcher.rewrite(query);
                long opsAfterFirstRewrite = profiler.getVectorOpsCount();
                searcher.rewrite(query);

                assertThat(profiler.getKnnProfileBreakdowns().size(), equalTo(1));
                assertThat(profiler.getVectorOpsCount(), equalTo(opsAfterFirstRewrite));
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
