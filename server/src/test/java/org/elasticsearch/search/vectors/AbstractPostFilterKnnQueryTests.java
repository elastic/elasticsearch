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
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

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
     * 8 docs: docs 0-5 pass (vectors 0..5), docs 6-7 fail (vectors 10..11), so selectivity is 0.75 - inside
     * the band post-filtering actually runs in, since it only engages at selectivity >= the configured
     * threshold. With k=4, numCands=3, and scale=0.5, the delegate collects numCands=3 candidates: {0,1,2}.
     * All 3 pass, but k=4 needs 1 more. The retry excludes {0,1,2} (filteredOut(empty) union topK({0,1,2}))
     * and is seeded from all matching = {0,1,2}. The retry finds {3}, which passes, closing the gap.
     * <p>
     * The retry asks for 4, not 1: the shortfall is in survivors, so it is inflated for filter attrition the
     * same way round 0 was - computeScaledK(1, 0.75) = ceil((1 + 2.5*sqrt(1*0.25/0.75))/0.75) = 4.
     */
    public void testRetryClosesTheGap() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 6; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            for (int i = 0; i < 2; i++) {
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
                assertEquals(0.75f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(1, meta.retryCalls());
                assertArrayEquals(new int[] { 0, 1, 2 }, meta.retryExcludedDocs());
                assertArrayEquals(new int[][] { { 0, 1, 2 } }, meta.retrySeedDocs());
                assertEquals("1 survivor short, inflated for selectivity 0.75", 4, meta.retryRemainingK());
            }
        }
    }

    /**
     * Two segments of 30 docs each. Each segment has 2 passing docs near the query vector and 2 passing docs
     * far away. Lucene's optimistic collection shows 20 candidates per leaf in round 0 (perLeafTopK with
     * LAMBDA=16), so round 0 finds only the 4 near passers: the pool is short, both leaves are exhausted, and
     * retry 1 re-enters both, seeded from each segment's near passers. Retries surface 5 docs per leaf; retry 1
     * finds only failures, retry 2 reaches segment 0's far passers (docs 28, 29) and fills the pool - but segment
     * 0's floor (doc 29) is still above the pool's threshold (doc 30), so it alone is exhausted and retry 3
     * re-enters only that leaf. It comes back empty (every doc of it is now excluded), which proves the pool:
     * post-filtering returns it without falling back.
     * <p>
     * Seeds follow the search frontier: retry 3 is seeded with the passers retry 2 found in each segment (docs
     * 28, 29 and 58, 59 - the latter below the threshold, so not in the pool), not with the round-0 ones.
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

                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 1, 28, 29, 30 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(3, meta.retryCalls());
                List<boolean[]> rounds = meta.retryLeavesPerRound();
                assertArrayEquals("short pool: both leaves exhausted", new boolean[] { true, true }, rounds.get(0));
                assertArrayEquals("short pool: both leaves exhausted", new boolean[] { true, true }, rounds.get(1));
                assertArrayEquals("full pool, but segment 0's floor is above the threshold", new boolean[] { true, false }, rounds.get(2));
                assertArrayEquals(new int[][] { { 28, 29 }, { 58, 59 } }, meta.retrySeedDocs());
                assertEquals("the pool was proven, so post-filtering scored it", 1, meta.finalizeCalls());
            }
        }
    }

    /**
     * A filter that is hostile <em>near the query</em> but not globally: post-filtering only engages at
     * selectivity >= the configured threshold, so a corpus-wide 0.25 is unreachable. Here 20 docs are 0.75
     * selective overall, but the five failures sit immediately next to the query - the negatively-correlated
     * shape that defeats a single retry.
     * <p>
     * doc 0 passes (vector 0.0), docs 1-5 fail (vectors 1..5), docs 6-19 pass. With k=2, numCands=3, and
     * scale=1.5, the delegate collects numCands=3 candidates: {0,1,2}. Only doc 0 passes, so the pool is
     * short and the leaf is exhausted. Retry 1 excludes filteredOut({1,2}) union topK({0}) = {0,1,2}, seeded
     * from {0}, and its candidates {3,4,5} all fail - the pool is still short and the leaf still exhausted.
     * Retry 2 excludes {0..5} and surfaces {6,7,8}, which all pass: the pool {0,6} is full and the leaf's
     * floor (doc 8) is below the pool's threshold (doc 6), so the pool is proven and post-filtering returns
     * it without falling back.
     * <p>
     * Each retry asks for max(computeScaledK(1, 0.75) = 4, round-0 k = 3) = 4, but numCands stays 3, so each
     * surfaces only 3 docs; crossing the gap genuinely needs the second round.
     */
    public void testSecondRetryCrossesTheFailGap() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 20; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                boolean passes = (i == 0 || i > 5);
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
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 6 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.75f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertEquals(2, meta.retryCalls());
                assertArrayEquals(
                    "the second retry excludes everything both earlier rounds saw",
                    new int[] { 0, 1, 2, 3, 4, 5 },
                    meta.retryExcludedDocs()
                );
                assertArrayEquals(new int[][] { { 0 } }, meta.retrySeedDocs());
                assertEquals("1 survivor short, inflated for selectivity 0.75", 4, meta.retryRemainingK());
                assertEquals("the pool was proven, so post-filtering scored it", 1, meta.finalizeCalls());
            }
        }
    }

    /**
     * The scenario from the review of the original post-filter PR: the pool is full, so a "retry when short"
     * rule is satisfied, yet the answer is wrong because one segment's whole candidate list was filtered out
     * and its next doc outranks the survivor from the other segment.
     * <p>
     * Segment A holds vectors 0..19 (docs 0-19): the 16 nearest fail, the last 4 pass. Segment B holds vectors
     * 100..119 (docs 20-39), all passing. Query [0], k=1, numCands=3: Lucene's optimistic collection shows 15
     * candidates per leaf in round 0, so A shows only failures while B fills the pool with doc 20 (vector 100).
     * A's floor - its 15th candidate, a failure - is still above that, so A is exhausted and must be re-entered,
     * while B has already shown it holds nothing better than doc 20 and must not be. The retry surfaces A's
     * doc 16 (vector 16), which replaces doc 20: the correct top-1 and the one pre-filtering returns.
     */
    public void testRetryReentersOnlyTheExhaustedLeaf() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            for (int i = 0; i < 20; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", i < 16 ? "fail" : "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();
            for (int i = 0; i < 20; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", 100f + i);
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals("expected two segments", 2, reader.leaves().size());
                int k = 1;
                int numCands = 3;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createQuery("vector", new float[] { 0f }, k, numCands, userFilter, 1.0f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertDocsByScoreDescending(td.scoreDocs, new int[] { 16 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals(0.6f, meta.postFilterDelegateSelectivity(), 0.001f);
                assertThat("the full-but-wrong pool must trigger a retry", meta.retryCalls(), greaterThanOrEqualTo(1));
                for (boolean[] retryLeaves : meta.retryLeavesPerRound()) {
                    assertArrayEquals("only segment A is exhausted; B must not be re-entered", new boolean[] { true, false }, retryLeaves);
                }
                assertEquals("the pool was proven, so post-filtering scored it rather than falling back", 1, meta.finalizeCalls());
            }
        }
    }

    /**
     * Post-filtering is an execution optimisation: whatever it returns must be what pre-filtering returns.
     * Random small segments with random pass/fail tags, indexed with the brute-force flat format so the
     * only approximation in play is the one under test - a leaf showing fewer candidates than the pool
     * needs. The expected answer is computed by exhaustive search over the passing docs.
     */
    public void testPostFilterMatchesExhaustiveSearch() throws IOException {
        int numSegments = between(1, 4);
        int totalDocs = 0;
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < numSegments * 12; i++) {
            values.add(i);
        }
        Collections.shuffle(values, random());
        List<Integer> passingValues = new ArrayList<>();
        IndexWriterConfig cfg = new IndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(new ES93FlatVectorFormat()));
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            for (int segment = 0; segment < numSegments; segment++) {
                int docsInSegment = between(1, 12);
                for (int i = 0; i < docsInSegment; i++) {
                    int value = values.get(totalDocs++);
                    boolean pass = randomDouble() < 0.75;
                    if (pass) {
                        passingValues.add(value);
                    }
                    Document doc = new Document();
                    addVectorField(doc, "vector", (float) value);
                    doc.add(new KeywordField("tag", pass ? "pass" : "fail", Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = between(1, 4);
                int numCands = between(k, k + 3);
                // Query below every value, so the nearest docs are simply the smallest values and no two tie.
                float[] queryVector = new float[] { -1f };
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createQuery("vector", queryVector, k, numCands, userFilter, randomFrom(1.0f, 1.5f));
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                Collections.sort(passingValues);
                int[] expectedValues = passingValues.stream().limit(k).mapToInt(Integer::intValue).toArray();
                int[] actualValues = new int[td.scoreDocs.length];
                for (int i = 0; i < td.scoreDocs.length; i++) {
                    actualValues[i] = values.get(td.scoreDocs[i].doc);
                }
                assertArrayEquals(
                    "post-filtered top-k must equal the exhaustive top-k over passing docs (segments="
                        + numSegments
                        + ", k="
                        + k
                        + ", numCands="
                        + numCands
                        + ", retries="
                        + asserting.postFilterMeta().retryCalls()
                        + ")",
                    expectedValues,
                    actualValues
                );
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

    private AssertingKnnQuery createPoolQuery(
        String field,
        float[] queryVector,
        int k,
        int numCands,
        Query filter,
        float postFilterScale,
        float poolScale
    ) {
        return new AssertingKnnQuery(vectorType(), field, queryVector, k, numCands, filter, postFilterScale, poolScale);
    }

    /**
     * The retry round must aim for {@code PostFilterableKnnQuery#candidatePoolSize}, not the user's {@code k}:
     * the pool is what the final scoring pass consumes, so stopping at {@code k} survivors starves it.
     * <p>
     * 8 docs all passing, vectors 0..7. k=2 with poolScale=3 makes the candidate pool 6; numCands=4 caps round 0 at 4
     * candidates. 4 &ge; k, so a k-based retry decision would fire nothing at all - the discriminating
     * assertion here is that a retry fires, asking for the missing 6-4=2 - which, every doc passing, the
     * 1.2x floor in computeScaledK rounds up to 3.
     */
    public void testRetryTargetsCandidatePoolNotFinalK() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 8; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 2;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createPoolQuery("vector", new float[] { 0f }, k, 4, userFilter, 1.0f, 3.0f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                assertDocsByScoreDescending(td.scoreDocs, new int[] { 0, 1 });

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(1, meta.postFilterDelegateCalls());
                assertEquals("a retry must fire even though round 0 already had k survivors", 1, meta.retryCalls());
                assertEquals("retry must ask for the pool shortfall, not the k shortfall", 3, meta.retryRemainingK());
            }
        }
    }

    /** With the pool already filled by round 0 there is nothing to retry for. */
    public void testNoRetryWhenCandidatePoolAlreadyFilled() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 8; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 2;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createPoolQuery("vector", new float[] { 0f }, k, 8, userFilter, 1.0f, 3.0f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                TopDocs td = searcher.search(pfq, k);

                assertEquals(k, td.scoreDocs.length);
                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals(0, meta.retryCalls());
                assertEquals("the pool, not k, is what finalizeTopK receives", 6, meta.finalizePoolSize());
                assertEquals(1, meta.finalizeCalls());
                assertEquals(k, meta.finalizeFinalK());
            }
        }
    }

    /**
     * {@code finalizeTopK} is the engine's exact scoring pass, so it must run exactly once when
     * post-filtering succeeds and never when the query falls back - the fallback path does its own scoring.
     */
    public void testFinalizeTopKSkippedOnFallback() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // Only 1 passing doc, so post-filtering can never reach k=3 and must fall back.
            Document passing = new Document();
            addVectorField(passing, "vector", 0f);
            passing.add(new KeywordField("tag", "pass", Field.Store.NO));
            writer.addDocument(passing);
            for (int i = 1; i < 8; i++) {
                Document doc = new Document();
                addVectorField(doc, "vector", (float) i);
                doc.add(new KeywordField("tag", "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                int k = 3;
                Query userFilter = new TermQuery(new Term("tag", "pass"));
                AssertingKnnQuery asserting = createPoolQuery("vector", new float[] { 0f }, k, 8, userFilter, 1.0f, 1.0f);
                PostFilterKnnQuery pfq = new PostFilterKnnQuery(asserting, userFilter, k, "vector", null, 0f);

                searcher.search(pfq, k);

                AssertingKnnQuery.PostFilterMeta meta = asserting.postFilterMeta();
                assertEquals("post-filtering came up short, so its scoring pass must not run", 0, meta.finalizeCalls());
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
