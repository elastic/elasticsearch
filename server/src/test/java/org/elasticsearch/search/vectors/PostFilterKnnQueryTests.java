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

public class PostFilterKnnQueryTests extends ESTestCase {

    /**
     * 8 docs, all tagged "pass", vectors [0..7]. The delegate collects all 8 (numCands=10 ≥ 8),
     * all pass the filter, and the top k=4 are returned without retry or fallback.
     */
    public void testRound0AloneSatisfiesK() throws IOException {
        // 8 pass docs (vectors 0.0..7.0)
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 8; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }));
                doc.add(new KeywordField("tag", "pass", Field.Store.YES));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, 10, userFilter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 1, 2, 3 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
                assertEquals(0, meta.fallbackCalls());
                assertEquals(1.0f, meta.postFilterDelegateSelectivity(), 0.001f);
            }
        }
    }

    /**
     * 8 docs: docs 0-3 pass (vectors 0..3), docs 4-7 fail (vectors 10..13). With k=4, numCands=3,
     * and scale=0.5, the delegate collects numCands=3 candidates: {0,1,2}. All 3 pass, but k=4
     * needs 1 more. The retry excludes {0,1,2} (filteredOut(∅) ∪ topK({0,1,2})) and is seeded
     * from all matching = {0,1,2}. The retry finds {3}, which passes, closing the gap.
     */
    public void testRetryClosesTheGap() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }));
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { 10f + i }));
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
                // scale=0.5 -> scaledK=2, Lucene k=numCands=3: pool = {0,1,2}, all pass, short by 1.
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, numCands, userFilter, 0.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 1, 2, 3 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.5f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(1, meta.retryCalls());
                // Excluded = filteredOut(∅) ∪ topK({0,1,2})
                assertArrayEquals(new int[] { 0, 1, 2 }, meta.retryExcludedDocs());
                // Seeds = all matching docs from round 0
                assertArrayEquals(new int[] { 0, 1, 2 }, meta.retrySeedDocs());
                assertEquals(1, meta.retryRemainingK());
                assertEquals(0, meta.fallbackCalls());
            }
        }
    }

    /**
     * Two segments: Seg 0 has 3 pass + 1 fail; Seg 1 has 3 pass + 1 fail. With numCands=2,
     * each leaf's HNSW collects 2 candidates. The full candidate pool spans both segments,
     * so the retry's seed docs must include matching docs from both segments — verifying that
     * {@code getPostFilterCandidates} correctly captures all per-leaf candidates.
     */
    public void testRetrySeedDocsSpanMultipleSegments() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            // Segment 0: docs 0-2 pass (vectors 0..2), doc 3 fails (vector 3).
            for (int i = 0; i < 3; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }));
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { 3f }));
                doc.add(new KeywordField("tag", "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();
            // Segment 1: docs 4-6 pass (vectors 100..102), doc 7 fails (vector 103).
            for (int i = 0; i < 3; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { 100f + i }));
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { 103f }));
                doc.add(new KeywordField("tag", "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals("expected two segments", 2, reader.leaves().size());
                int k = 5;
                int numCands = 2;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                // Lucene k=numCands=2 per leaf.
                // Seg 0 pool: {0,1} (both pass). Seg 1 pool: {4,5} (both pass).
                // Full pool = {0,1,4,5}. Matching = {0,1,4,5}. k=5, short by 1.
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, numCands, userFilter, 0.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.retryCalls());
                // Excluded = filteredOut(∅) ∪ topK({0,1,4,5})
                assertArrayEquals(new int[] { 0, 1, 4, 5 }, meta.retryExcludedDocs());
                // Seeds span both segments: all matching from round 0
                assertArrayEquals(new int[] { 0, 1, 4, 5 }, meta.retrySeedDocs());
            }
        }
    }

    /**
     * 8 docs: doc 0 passes (vector 0.0), docs 1-6 fail (vectors 1..6), doc 7 passes (vector 7.0).
     * With k=2, numCands=3, and scale=1.5, the delegate collects numCands=3 candidates: {0,1,2}.
     * Only doc 0 passes. Retry excludes filteredOut({1,2}) ∪ topK({0}) = {0,1,2}, seeded from
     * {0}. Retry's top result among {3,4,5} is doc 3 (fail) — no improvement. Fallback then
     * pre-filters for doc 7.
     */
    public void testFallbackClosesTheGap() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 8; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }));
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
                // scale=1.5, Lucene k=numCands=3: pool={0,1,2}, only {0} passes.
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, numCands, userFilter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 7 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.25f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(1, meta.retryCalls());
                // Excluded = filteredOut({1,2}) ∪ topK({0})
                assertArrayEquals(new int[] { 0, 1, 2 }, meta.retryExcludedDocs());
                // Seeds = all matching from round 0
                assertArrayEquals(new int[] { 0 }, meta.retrySeedDocs());
                assertEquals(1, meta.retryRemainingK());
                assertEquals(1, meta.fallbackCalls());
                assertArrayEquals(new int[] { 0 }, meta.fallbackExcludedDocs());
                assertEquals(1, meta.fallbackRemainingK());
            }
        }
    }

    /**
     * 10 docs: docs 0-5 fail (vectors 0..5), docs 6-9 pass (vectors 10..13). With numCands=4 and
     * scale=1.5, the delegate collects numCands=4 candidates = {0,1,2,3} — all fail. The full
     * candidate pool has zero passers, so PostFilterKnnQuery returns null and falls through to
     * the bare inner query. The bare inner carries the original user filter, returning the 4
     * closest passing docs.
     */
    public void testFallsThroughToBareInnerWhenPostFilterYieldsNoMatches() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 6; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }));
                doc.add(new KeywordField("tag", "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { 10f + i }));
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
                // scale=1.5, Lucene k=numCands=4: pool = {0,1,2,3}, all fail.
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, numCands, userFilter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                // Bare inner carries the original user filter; returns the 4 closest passing docs.
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 6, 7, 8, 9 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
                assertEquals(0, meta.fallbackCalls());
            }
        }
    }

    /**
     * 10 docs, selectivity=0.5 (5 even docs pass), threshold=0.9 -> post-filter gated off; outer
     * rewrites straight to the bare inner query. Bare inner carries the original user filter and
     * returns the filtered top-k: the 4 closest passing docs {0,2,4,6}.
     */
    public void testSkipsPostFilterWhenSelectivityBelowThreshold() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }));
                doc.add(new KeywordField("tag", i % 2 == 0 ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, 10, userFilter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0.9f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                // Bare inner carries the original user filter; returns the 4 closest passing docs.
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 2, 4, 6 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(0, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
                assertEquals(0, meta.fallbackCalls());
            }
        }
    }

    /**
     * 20 docs, selectivity=0.7 (14 pass, 6 fail), arranged so the kNN region around the query is
     * negatively correlated to the filter: the 3 closest docs fail and only 2 close passers exist.
     * With k=6, numCands=7, and scale=0.5, the delegate collects numCands=7 candidates:
     * {0,1,2,3,4,5,6}. Only docs 3 and 4 pass (2 matches). The early-exit fires: k=6 &ge;
     * EARLY_EXIT_MIN_K and 2 &lt; (6*0.7)/2 = 2.1, so retry and fallback are skipped and the
     * outer rewrite falls through to the bare inner query, returning the 6 closest passing docs.
     */
    public void testEarlyExitBailsOnNegativelyCorrelatedFilter() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // Close cluster: 3 fail at vectors 0..2, 2 pass at vectors 3..4.
            for (int i = 0; i < 3; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }));
                doc.add(new KeywordField("tag", "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            for (int i = 3; i < 5; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }));
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            // Far fail cluster: 3 fail at vectors 100..102.
            for (int i = 0; i < 3; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { 100f + i }));
                doc.add(new KeywordField("tag", "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            // Far pass cluster: 12 pass at vectors 200..211.
            for (int i = 0; i < 12; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { 200f + i }));
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
                // scale=0.5, Lucene k=numCands=7: pool = {0,1,2,3,4,5,6}, only {3,4} pass.
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, numCands, userFilter, 0.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                // Bare inner runs with the user filter; returns the 6 closest passing docs.
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 3, 4, 8, 9, 10, 11 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.7f, meta.postFilterDelegateSelectivity(), 0.001f);
                // Early-exit fires before retry/fallback can run.
                assertEquals(0, meta.retryCalls());
                assertEquals(0, meta.fallbackCalls());
            }
        }
    }

    public void testMatchNoDocsFilterShortCircuits() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }));
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 4;
                Query filter = MatchNoDocsQuery.INSTANCE;
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, 10, filter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, filter, k, "vector", null, 0f);

                Query rewritten = pfq.rewrite(searcher);
                assertSame(MatchNoDocsQuery.INSTANCE, rewritten);

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(0, meta.postFilterDelegateCalls());
                assertEquals(0, meta.retryCalls());
                assertEquals(0, meta.fallbackCalls());
            }
        }
    }

    private static void assertDocsByScoreDescending(ScoreDoc[] actual, int[] expectedDocs) {
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
