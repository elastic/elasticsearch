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
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.CheckJoinIndex;
import org.apache.lucene.search.join.QueryBitSetProducer;
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
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that nested (parent/child) kNN queries now publish a detailed {@code hnsw} breakdown, matching
 * the plain HNSW queries. Previously these emitted only the vector-op count and no {@code knn_profile}
 * breakdown.
 */
public class ESDiversifyingChildrenKnnVectorQueryProfileTests extends ESTestCase {

    public void testFloatNestedQueryPublishesHnswBreakdown() throws IOException {
        try (Directory dir = newDirectory()) {
            indexFloatBlocks(dir, 30, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = contextSearcher(reader);
                QueryProfiler profiler = new QueryProfiler();
                searcher.setProfiler(profiler);
                BitSetProducer parents = parentFilter(reader);

                ESDiversifyingChildrenFloatKnnVectorQuery query = new ESDiversifyingChildrenFloatKnnVectorQuery(
                    "vector",
                    new float[] { 1f, 2f, 3f, 4f },
                    null,
                    5,
                    10,
                    parents,
                    null
                );
                query.setQuantization("bbq_hnsw");
                searcher.rewrite(query);

                assertHnswBreakdown(profiler, "bbq_hnsw");
            }
        }
    }

    public void testByteNestedQueryPublishesHnswBreakdown() throws IOException {
        try (Directory dir = newDirectory()) {
            indexByteBlocks(dir, 30, 4);
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher searcher = contextSearcher(reader);
                QueryProfiler profiler = new QueryProfiler();
                searcher.setProfiler(profiler);
                BitSetProducer parents = parentFilter(reader);

                ESDiversifyingChildrenByteKnnVectorQuery query = new ESDiversifyingChildrenByteKnnVectorQuery(
                    "vector",
                    new byte[] { 1, 2, 3, 4 },
                    null,
                    5,
                    10,
                    parents,
                    null
                );
                searcher.rewrite(query);

                assertHnswBreakdown(profiler, null);
            }
        }
    }

    private static void assertHnswBreakdown(QueryProfiler profiler, String expectedQuantization) {
        Map<String, Object> breakdown = profiler.getKnnProfileBreakdown();
        assertThat(breakdown, notNullValue());
        assertThat(breakdown.get("algorithm"), equalTo("hnsw"));
        assertThat(breakdown.get("field"), equalTo("vector"));
        if (expectedQuantization != null) {
            assertThat(breakdown.get("quantization"), equalTo(expectedQuantization));
        }
        assertThat((long) breakdown.get("total_time_ns"), greaterThanOrEqualTo(0L));

        @SuppressWarnings("unchecked")
        Map<String, Object> hnsw = (Map<String, Object>) breakdown.get("hnsw");
        assertThat(hnsw, notNullValue());
        assertThat(hnsw.get("k"), equalTo(5));
        assertThat(hnsw.get("num_candidates"), equalTo(10));
        assertThat((int) hnsw.get("leaf_searches"), greaterThan(0));

        @SuppressWarnings("unchecked")
        Map<String, Object> timings = (Map<String, Object>) hnsw.get("timings");
        assertThat(timings, notNullValue());
        assertThat((long) timings.get("avg_leaf_search_ns"), greaterThanOrEqualTo(0L));
        assertThat(profiler.getVectorOpsCount(), greaterThanOrEqualTo(0L));
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

    private static BitSetProducer parentFilter(IndexReader reader) throws IOException {
        BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
        CheckJoinIndex.check(reader, parentsFilter);
        return parentsFilter;
    }

    private void indexFloatBlocks(Directory dir, int numBlocks, int dim) throws IOException {
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < numBlocks; i++) {
                Document child = new Document();
                float[] vec = new float[dim];
                for (int d = 0; d < dim; d++) {
                    vec[d] = randomFloat();
                }
                child.add(new KnnFloatVectorField("vector", vec, VectorSimilarityFunction.EUCLIDEAN));
                Document parent = new Document();
                parent.add(new KeywordField("docType", "_parent", Field.Store.NO));
                w.addDocuments(List.of(child, parent));
            }
        }
    }

    private void indexByteBlocks(Directory dir, int numBlocks, int dim) throws IOException {
        try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < numBlocks; i++) {
                Document child = new Document();
                byte[] vec = new byte[dim];
                random().nextBytes(vec);
                child.add(new KnnByteVectorField("vector", vec, VectorSimilarityFunction.EUCLIDEAN));
                Document parent = new Document();
                parent.add(new KeywordField("docType", "_parent", Field.Store.NO));
                w.addDocuments(List.of(child, parent));
            }
        }
    }
}
