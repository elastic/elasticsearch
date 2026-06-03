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
     * 8 docs, all tagged "pass", vectors [0..7]. Round 0 (scaledK=12) returns top-k=4 — enough to
     * satisfy k without retry or fallback.
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
     * 8 docs: docs 0-3 pass (vectors 0..3), docs 4-7 fail (vectors 10..13). With k=4 and scale=0.5,
     * round 0's delegate asks for scaledK=2 and returns docs [0,1] (both pass, but still need 2 results). The retry
     * excludes {0,1} and, since all failing docs have large distances, returns {2,3} (both pass),
     * closing the gap. Fallback never fires.
     */
    public void testRetryClosesTheGap() throws IOException {
        // Pass docs 0-3 at small distances; fail docs 4-7 at large distances.
        // ExcludeDocsQuery({0,1}) leaves {2,3} as the next-closest, both passing.
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
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                // scale=0.5 -> scaledK=ceil(4*0.5)=2: round 0 finds top-2={0,1}, both pass, short by 2.
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, 10, userFilter, 0.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 1, 2, 3 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.5f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(1, meta.retryCalls());
                // Excluded = collected passing docs from round 0.
                assertArrayEquals(new int[] { 0, 1 }, meta.retryExcludedDocs());
                // SeedDocs = sorted getTrackedDocs() from round 0 — single segment, per-leaf
                // collector size = numCands (10) which holds every doc in the leaf, so tracked
                // covers all 8 docs (not just topDocs.scoreDocs).
                assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }, meta.retrySeedDocs());
                assertEquals(2, meta.retryRemainingK());
                assertEquals(0, meta.fallbackCalls());
            }
        }
    }

    /**
     * Two segments of 4 docs each, all passing. Round 0 with scaledK=2 returns the global top-2
     * via {@link AssertingKnnQuery#mergeLeafResults}, but the per-leaf {@link DocTrackingCollector}
     * captures each leaf's own top-numCands first. The retry's {@code seedDocs} therefore must be
     * the union of both leaves' tracked sets — verifying {@link DocTrackingCollectorManager}
     * correctly aggregates across leaves and that {@link PostFilterKnnQuery} threads that union
     * into {@link PostFilterableKnnQuery#createRetryQuery}.
     */
    public void testRetrySeedDocsMergedAcrossLeavesFromGetTrackedDocs() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            // Segment 0: docs 0-3 at vectors 0..3.
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { (float) i }));
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();
            // Segment 1: docs 4-7 at vectors 100..103 (far from query, so the global top-2
            // comes entirely from segment 0).
            for (int i = 0; i < 4; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField("vector", new float[] { 100f + i }));
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals("expected two segments", 2, reader.leaves().size());
                int k = 4;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                // scale=0.5 -> scaledK=2: round 0 picks global top-2 = {0, 1} from segment 0.
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, 10, userFilter, 0.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 1, 2, 3 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.retryCalls());
                // Round 0 surfaced only {0, 1} as the post-filter passing set.
                assertArrayEquals(new int[] { 0, 1 }, meta.retryExcludedDocs());
                // getTrackedDocs() returns the union of per-leaf top-numCands. Both leaves have
                // 4 docs each ≤ numCands(=10), so all 8 are tracked across the two leaves.
                assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }, meta.retrySeedDocs());
            }
        }
    }

    /**
     * 8 docs: doc 0 passes (vector 0.0), docs 1-6 fail (vectors 1..6), doc 7 passes (vector 7.0).
     * With k=2 and scale=1.5 (scaledK=3), round 0 finds {0,1,2} — only doc 0 passes. The retry
     * excludes {0} and returns nothing. The augmented pre-filter
     * fallback then ANDs the original filter with ExcludeDocsQuery({0}), pre-filters the KNN, and
     * returns doc 7 as the only remaining passing doc.
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
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                // scale=1.5 -> scaledK=ceil(2*1.5)=3: round 0 finds {0,1,2}, only {0} passes.
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, 10, userFilter, 1.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 7 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.25f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(1, meta.retryCalls());
                assertArrayEquals(new int[] { 0 }, meta.retryExcludedDocs());
                // SeedDocs = sorted getTrackedDocs() from round 0 — single segment, per-leaf
                // collector holds all 8 docs (numCands=10 > 8), so the full set is tracked.
                assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }, meta.retrySeedDocs());
                assertEquals(1, meta.retryRemainingK());
                assertEquals(1, meta.fallbackCalls());
                assertArrayEquals(new int[] { 0 }, meta.fallbackExcludedDocs());
                assertEquals(1, meta.fallbackRemainingK());
            }
        }
    }

    /**
     * 10 docs: docs 0-5 fail (vectors 0..5), docs 6-9 pass (vectors 10..13). With scale=1.5
     * (scaledK=6), round 0's top-6 candidates are all fail docs -> applyFilter returns an empty
     * array -> PostFilterKnnQuery returns null and falls through to the bare inner query. The bare
     * inner carries the original user filter, so it returns the filtered top-k: the 4 closest
     * passing docs {6,7,8,9}.
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
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                // scale=1.5 -> scaledK=6: all top-6 docs fail the filter -> falls through to bare inner.
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, 10, userFilter, 1.5f);
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
     * negatively correlated to the filter: the 3 closest docs fail and only 2 close passers exist. With k=10 and
     * scale=0.5 (scaledK=5), round 0's top-5 = {0..4} yields only {3,4} after filtering -
     * scoreDocs.length=2. The early-exit fires: k=10 ≥ EARLY_EXIT_MIN_K and 2 &lt;
     * (k * selectivity) / 2 = 3.5, so retry and fallback are skipped and the outer rewrite falls
     * through to the bare inner query (which carries the user filter), returning the 10 closest
     * passing docs.
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
                int k = 10;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                // scale=0.5 -> scaledK=5: top-5 closest = {0,1,2,3,4}, only {3,4} pass.
                AssertingKnnQuery asserting = new AssertingKnnQuery("vector", new float[] { 0f }, k, 20, userFilter, 0.5f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                // Bare inner runs with the user filter; returns the 10 closest passing docs.
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 3, 4, 8, 9, 10, 11, 12, 13, 14, 15 });

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
