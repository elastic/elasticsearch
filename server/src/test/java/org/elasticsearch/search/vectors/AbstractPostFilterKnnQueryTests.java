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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

abstract class AbstractPostFilterKnnQueryTests extends ESTestCase {

    protected abstract AssertingKnnQuery.VectorType vectorType();

    protected abstract void addVectorField(Document doc, String field, float value);

    private AssertingKnnQuery createQuery(String field, float[] queryVector, int k, int numCands, Query filter, float postFilterScale) {
        return new AssertingKnnQuery(vectorType(), field, queryVector, k, numCands, filter, postFilterScale);
    }

    /**
     * 8 docs, all tagged "pass", vectors [0..7]. The delegate collects all 8 (numCands=10 >= 8),
     * all pass the filter, and the top k=4 are returned without retry.
     */
    public void testRound0AloneSatisfiesK() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 8; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", "pass", Field.Store.YES));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, 10, userFilter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 1, 2, 3 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
                assertEquals(1.0f, meta.postFilterDelegateSelectivity(), 0.001f);
            }
        }
    }

    /**
     * 8 docs: docs 0-3 pass (vectors 0..3), docs 4-7 fail (vectors 10..13). With k=4, numCands=3,
     * and scale=0.5, the delegate collects numCands=3 candidates: {0,1,2}. All 3 pass, but k=4
     * needs 1 more. The retry excludes {0,1,2} (filteredOut(empty) union topK({0,1,2})) and is seeded
     * from all matching = {0,1,2}. The retry finds {3}, which passes, closing the gap.
     */
    public void testRetryClosesTheGap() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", 10f + i);
                doc.add(new KeywordField("tag", "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                int numCands = 3;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 0.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 1, 2, 3 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.5f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(1, meta.retryCalls());
                assertArrayEquals(new int[] { 0, 1, 2 }, meta.retryExcludedDocs());
                assertArrayEquals(new int[][] { { 0, 1, 2 } }, meta.retrySeedDocs());
                assertEquals(1, meta.retryRemainingK());
            }
        }
    }

    /**
     * Two segments of 30 docs each. Each segment has 2 passing docs near the query vector and
     * 2 passing docs far away (beyond the optimistic collection window). Lucene's optimistic
     * collection inflates per-leaf k, but with 30 docs per segment the first pass still cannot
     * collect the far-away passers. The first pass finds only 4 passers total (2 per segment),
     * which is less than k=5, so a retry fires with seed docs spanning both segments. The retry
     * cannot reach the far passers either, so the query falls through to the bare inner query,
     * which exact-searches the 4 passers per leaf (cardinality &le; numCands) and merges to k=5.
     */
    public void testRetrySeedDocsSpanMultipleSegments() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            for (int i = 0; i < 30; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                boolean pass = (i <= 1 || i >= 28);
                doc.add(new KeywordField("tag", pass ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();
            for (int i = 0; i < 30; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", 50f + i);
                boolean pass = (i <= 1 || i >= 28);
                doc.add(new KeywordField("tag", pass ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals("expected two segments", 2, reader.leaves().size());
                int k = 5;
                int numCands = 5;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 0.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.retryCalls());
                assertArrayEquals(new int[][] { { 0, 1 }, { 30, 31 } }, meta.retrySeedDocs());
            }
        }
    }

    /**
     * 8 docs: doc 0 passes (vector 0.0), docs 1-6 fail (vectors 1..6), doc 7 passes (vector 7.0).
     * With k=2, numCands=3, and scale=1.5, the delegate collects numCands=3 candidates: {0,1,2}.
     * Only doc 0 passes. Retry excludes filteredOut({1,2}) union topK({0}) = {0,1,2}, seeded from
     * {0}. Retry's top result among {3,4,5} is doc 3 (fail). Post-filter rounds return null,
     * falling through to the bare inner query which pre-filters for docs {0,7}.
     */
    public void testFallThroughToInnerQueryClosesTheGap() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 8; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                boolean passes = (i == 0 || i == 7);
                doc.add(new KeywordField("tag", passes ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 2;
                int numCands = 3;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 7 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.25f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(1, meta.retryCalls());
                assertArrayEquals(new int[] { 0, 1, 2 }, meta.retryExcludedDocs());
                assertArrayEquals(new int[][] { { 0 } }, meta.retrySeedDocs());
                assertEquals(1, meta.retryRemainingK());
            }
        }
    }

    /**
     * 10 docs: docs 0-5 fail (vectors 0..5), docs 6-9 pass (vectors 10..13). With numCands=4 and
     * scale=1.5, the delegate collects numCands=4 candidates = {0,1,2,3} -- all fail. The full
     * candidate pool has zero passers, so PostFilterKnnQuery returns null and falls through to
     * the bare inner query.
     */
    public void testFallsThroughToBareInnerWhenPostFilterYieldsNoMatches() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 6; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", 10f + i);
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                int numCands = 4;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 6, 7, 8, 9 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
            }
        }
    }

    /**
     * 10 docs, selectivity=0.5 (5 even docs pass), threshold=0.9 -> post-filter gated off; outer
     * rewrites straight to the bare inner query.
     */
    public void testSkipsPostFilterWhenSelectivityBelowThreshold() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", i % 2 == 0 ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, 10, userFilter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0.9f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 2, 4, 6 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(0, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
            }
        }
    }

    /**
     * 20 docs, selectivity=0.7 (14 pass, 6 fail), arranged so the kNN region around the query is
     * negatively correlated to the filter: the 3 closest docs fail and only 2 close passers exist.
     * Early-exit fires: k=6 &ge; EARLY_EXIT_MIN_K and 2 &lt; (6*0.7)/2 = 2.1.
     */
    public void testEarlyExitBailsOnNegativelyCorrelatedFilter() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            for (int i = 3; i < 5; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            for (int i = 0; i < 3; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", 50f + i);
                doc.add(new KeywordField("tag", "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            for (int i = 0; i < 12; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", 80f + i);
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 6;
                int numCands = 7;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 0.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 3, 4, 8, 9, 10, 11 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.7f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(0, meta.retryCalls());
            }
        }
    }

    public void testMatchNoDocsFilterShortCircuits() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                Query filter = MatchNoDocsQuery.INSTANCE;
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, 10, filter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, filter, k, "vector", null, 0f);

                Query rewritten = pfq.rewrite(searcher);
                assertSame(MatchNoDocsQuery.INSTANCE, rewritten);

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(0, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
            }
        }
    }

    protected static void assertDocsByScoreDescending(ScoreDoc[] actual, int[] expectedDocs) {
        assertEquals("doc count mismatch", expectedDocs.length, actual.length);
        for (int i = 0; i < expectedDocs.length; i++) {
            assertEquals("doc at rank " + i, expectedDocs[i], actual[i].doc);
            if (i > 0) {
                assertTrue(
                    "scores must be descending: rank " + (i - 1) + "=" + actual[i - 1].score + ", rank " + i + "=" + actual[i].score,
                    actual[i - 1].score >= actual[i].score
                );
            }
        }
    }
}
