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
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
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
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that {@link PostFilterKnnQuery} publishes its {@code post_filter} breakdown when a profiler is
 * attached to the searcher, and that the configured quantization is propagated onto each round's inner
 * kNN query (the per-round {@code inner} breakdown carries the {@code quantization} label).
 */
public class PostFilterKnnQueryProfileTests extends ESTestCase {

    public void testPostFilterBreakdownPublishedWithQuantization() throws IOException {
        try (Directory dir = newDirectory()) {
            // 8 docs all matching the filter: post-filter engages (threshold 0) and round 0 satisfies k.
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 8; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }, VectorSimilarityFunction.EUCLIDEAN));
                    doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = contextSearcher(reader);
                QueryProfiler profiler = new QueryProfiler();
                searcher.setProfiler(profiler);

                int k = 4;
                Query filter = new TermQuery(new Term("tag", "pass"));
                ESKnnFloatVectorQuery inner = new ESKnnFloatVectorQuery("vector", new float[] { 0f }, k, 10, filter, null);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(inner, filter, k, "vector", null, 0f);
                // Mirror the mapper: quantization is set on the outer query after the post-filter wrap.
                pfq.setQuantization("bbq_hnsw");

                searcher.rewrite(pfq);

                Map<String, Object> breakdown = profiler.getKnnProfileBreakdown();
                assertThat(breakdown, notNullValue());
                assertThat(breakdown.get("algorithm"), equalTo("hnsw"));
                assertThat(breakdown.get("field"), equalTo("vector"));
                assertThat(breakdown, hasKey("post_filter"));

                @SuppressWarnings("unchecked")
                Map<String, Object> postFilter = (Map<String, Object>) breakdown.get("post_filter");
                assertThat(postFilter.get("engaged"), equalTo(true));
                assertThat((float) postFilter.get("selectivity"), equalTo(1.0f));
                assertThat((float) postFilter.get("threshold"), equalTo(0.0f));
                assertThat(postFilter, hasKey("early_exit"));
                assertThat((long) postFilter.get("total_vector_ops"), greaterThan(0L));

                @SuppressWarnings("unchecked")
                List<Map<String, Object>> rounds = (List<Map<String, Object>>) postFilter.get("rounds");
                assertThat(rounds.size(), greaterThan(0));
                Map<String, Object> initial = rounds.get(0);
                assertThat(initial.get("name"), equalTo("initial"));
                assertThat((int) initial.get("docs_found"), greaterThan(0));

                // The per-round inner breakdown must carry the quantization forwarded onto the delegate.
                @SuppressWarnings("unchecked")
                Map<String, Object> inner0 = (Map<String, Object>) initial.get("inner");
                assertThat(inner0, notNullValue());
                assertThat(inner0.get("algorithm"), equalTo("hnsw"));
                assertThat(inner0.get("quantization"), equalTo("bbq_hnsw"));
            }
        }
    }

    /**
     * When the filter is not selective enough, post-filtering is declined and the bare inner query runs.
     * The breakdown must then keep the plain kNN shape (so the output does not depend on whether the
     * post-filter wrapper happened to be inserted) plus a compact section explaining the decision.
     */
    public void testNotEngagedBreakdownExplainsTheDecision() throws IOException {
        try (Directory dir = newDirectory()) {
            // Half the docs match the filter, so selectivity (0.5) falls below the threshold below.
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 8; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }, VectorSimilarityFunction.EUCLIDEAN));
                    doc.add(new KeywordField("tag", i % 2 == 0 ? "pass" : "drop", Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = contextSearcher(reader);
                QueryProfiler profiler = new QueryProfiler();
                searcher.setProfiler(profiler);

                int k = 4;
                Query filter = new TermQuery(new Term("tag", "pass"));
                ESKnnFloatVectorQuery inner = new ESKnnFloatVectorQuery("vector", new float[] { 0f }, k, 10, filter, null);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(inner, filter, k, "vector", null, 0.9f);

                searcher.rewrite(pfq);

                Map<String, Object> breakdown = profiler.getKnnProfileBreakdown();
                assertThat(breakdown, notNullValue());
                // The inner query's own breakdown is surfaced, so the shape matches an unwrapped kNN query.
                assertThat(breakdown.get("algorithm"), equalTo("hnsw"));
                assertThat(breakdown.get("field"), equalTo("vector"));
                assertThat(breakdown, hasKey("hnsw"));

                @SuppressWarnings("unchecked")
                Map<String, Object> postFilter = (Map<String, Object>) breakdown.get("post_filter");
                assertThat(postFilter.get("engaged"), equalTo(false));
                assertThat((float) postFilter.get("selectivity"), equalTo(0.5f));
                assertThat((float) postFilter.get("threshold"), equalTo(0.9f));
                // No rounds ran, so the round-specific keys are omitted rather than reported as zeroes.
                assertFalse(postFilter.containsKey("rounds"));
            }
        }
    }

    public void testMatchNoDocsFilterStillPublishesProfile() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { 0f }, VectorSimilarityFunction.EUCLIDEAN));
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
                writer.commit();
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = contextSearcher(reader);
                QueryProfiler profiler = new QueryProfiler();
                searcher.setProfiler(profiler);

                ESKnnFloatVectorQuery inner = new ESKnnFloatVectorQuery("vector", new float[] { 0f }, 1, 4, new MatchNoDocsQuery(), null);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(inner, new MatchNoDocsQuery(), 1, "vector", null, 0f);
                searcher.rewrite(pfq);

                Map<String, Object> breakdown = profiler.getKnnProfileBreakdown();
                assertThat(breakdown, notNullValue());
                assertThat(breakdown.get("field"), equalTo("vector"));
                @SuppressWarnings("unchecked")
                Map<String, Object> postFilter = (Map<String, Object>) breakdown.get("post_filter");
                assertThat(postFilter.get("match_no_docs"), equalTo(true));
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
}
