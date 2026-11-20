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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LRUQueryCache;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KnnQueryUtilsTests extends ESTestCase {

    public void testMergeScoreDocArrays() {
        ScoreDoc a = new ScoreDoc(1, 0.9f);
        ScoreDoc b = new ScoreDoc(2, 0.8f);
        ScoreDoc c = new ScoreDoc(3, 0.7f);

        assertEquals(0, KnnQueryUtils.mergeScoreDocArrays(new ScoreDoc[0], new ScoreDoc[0]).length);

        ScoreDoc[] leftOnly = KnnQueryUtils.mergeScoreDocArrays(new ScoreDoc[] { a, b }, new ScoreDoc[0]);
        assertEquals(2, leftOnly.length);
        assertSame(a, leftOnly[0]);
        assertSame(b, leftOnly[1]);

        ScoreDoc[] rightOnly = KnnQueryUtils.mergeScoreDocArrays(new ScoreDoc[0], new ScoreDoc[] { a, b });
        assertEquals(2, rightOnly.length);
        assertSame(a, rightOnly[0]);
        assertSame(b, rightOnly[1]);

        ScoreDoc[] both = KnnQueryUtils.mergeScoreDocArrays(new ScoreDoc[] { a }, new ScoreDoc[] { b, c });
        assertEquals(3, both.length);
        assertSame(a, both[0]);
        assertSame(b, both[1]);
        assertSame(c, both[2]);
    }

    public void testDedupAndSelectTopKDeduplicatesByDocId() {
        ScoreDoc[] input = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(3, 0.7f), new ScoreDoc(2, 0.8f), new ScoreDoc(4, 0.6f) };
        ScoreDoc[] result = KnnQueryUtils.dedupAndSelectTopK(input, 4);
        assertEquals(4, result.length);
        assertEquals(1, result[0].doc);
        assertEquals(0.9f, result[0].score, 0.001f);
        assertEquals(2, result[1].doc);
        assertEquals(0.8f, result[1].score, 0.001f);
        assertEquals(3, result[2].doc);
        assertEquals(0.7f, result[2].score, 0.001f);
        assertEquals(4, result[3].doc);
        assertEquals(0.6f, result[3].score, 0.001f);
    }

    public void testDedupAndSelectTopKDocIdCollisionKeepsHighestScore() {
        // doc 2 appears twice with scores 0.7 and 0.8; the higher must win
        ScoreDoc[] input = new ScoreDoc[] {
            new ScoreDoc(1, 0.9f),
            new ScoreDoc(2, 0.7f),
            new ScoreDoc(4, 0.7f),
            new ScoreDoc(2, 0.8f),
            new ScoreDoc(3, 0.6f) };
        ScoreDoc[] result = KnnQueryUtils.dedupAndSelectTopK(input, 5);
        assertEquals(4, result.length);
        assertEquals(1, result[0].doc);
        assertEquals(0.9f, result[0].score, 0.001f);
        assertEquals(2, result[1].doc);
        assertEquals(0.8f, result[1].score, 0.001f);
        // docs 3 and 4 both have 0.7 (order between them is unspecified after dedup)
        assertEquals(0.7f, result[2].score, 0.001f);
        assertEquals(0.6f, result[3].score, 0.001f);
        assertEquals(3, result[3].doc);
    }

    public void testDedupAndSelectTopKPartialSelectionKeepsTopK() {
        // Scores intentionally out of order — partial selection must still find the top-k.
        ScoreDoc[] input = new ScoreDoc[] {
            new ScoreDoc(0, 0.1f),
            new ScoreDoc(1, 0.9f),
            new ScoreDoc(2, 0.4f),
            new ScoreDoc(3, 0.8f),
            new ScoreDoc(4, 0.2f),
            new ScoreDoc(5, 0.7f),
            new ScoreDoc(6, 0.3f),
            new ScoreDoc(7, 0.6f),
            new ScoreDoc(8, 0.5f),
            new ScoreDoc(9, 0.95f) };
        ScoreDoc[] result = KnnQueryUtils.dedupAndSelectTopK(input, 3);
        assertEquals(3, result.length);
        // Top 3 scores: 0.95 (doc 9), 0.9 (doc 1), 0.8 (doc 3), in descending order.
        assertEquals(9, result[0].doc);
        assertEquals(0.95f, result[0].score, 0.001f);
        assertEquals(1, result[1].doc);
        assertEquals(0.9f, result[1].score, 0.001f);
        assertEquals(3, result[2].doc);
        assertEquals(0.8f, result[2].score, 0.001f);
    }

    public void testDedupAndSelectTopKEmptyInputOrZeroK() {
        assertEquals(0, KnnQueryUtils.dedupAndSelectTopK(new ScoreDoc[0], 5).length);
        assertEquals(0, KnnQueryUtils.dedupAndSelectTopK(new ScoreDoc[] { new ScoreDoc(0, 1f) }, 0).length);
    }

    public void testCategorizeByFilterPassesMatchingDocs() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 5; i++) {
                Document doc = new Document();
                doc.add(new StringField("tag", i % 2 == 0 ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Weight filterWeight = searcher.createWeight(
                    searcher.rewrite(new TermQuery(new Term("tag", "pass"))),
                    ScoreMode.COMPLETE_NO_SCORES,
                    1f
                );

                ScoreDoc[][] perLeaf = new ScoreDoc[1][];
                perLeaf[0] = new ScoreDoc[] {
                    new ScoreDoc(0, 0.9f),
                    new ScoreDoc(1, 0.8f),
                    new ScoreDoc(2, 0.7f),
                    new ScoreDoc(3, 0.6f),
                    new ScoreDoc(4, 0.5f) };

                PostFilterKnnQuery.FilteredCandidates result = PostFilterKnnQuery.applyFilter(
                    perLeaf,
                    filterWeight,
                    searcher.getIndexReader().leaves()
                );
                assertPerLeafMatchingDocs(result.matchingPerLeaf(), new int[] { 0, 2, 4 }, new float[] { 0.9f, 0.7f, 0.5f });
                assertArrayEquals(new int[] { 1, 3 }, result.filteredOutPerLeaf()[0]);
            }
        }
    }

    private static void assertPerLeafMatchingDocs(ScoreDoc[][] perLeafMatching, int[] expectedDocs, float[] expectedScores) {
        int totalMatching = 0;
        for (ScoreDoc[] leaf : perLeafMatching) {
            if (leaf != null) totalMatching += leaf.length;
        }
        assertEquals("doc count", expectedDocs.length, totalMatching);
        for (int i = 0; i < expectedDocs.length; i++) {
            int doc = expectedDocs[i];
            float score = expectedScores[i];
            boolean found = false;
            for (ScoreDoc[] leaf : perLeafMatching) {
                if (leaf == null) continue;
                for (ScoreDoc sd : leaf) {
                    if (sd.doc == doc) {
                        assertEquals("score for doc " + doc, score, sd.score, 0.001f);
                        found = true;
                        break;
                    }
                }
                if (found) break;
            }
            assertTrue("missing doc " + doc, found);
        }
    }

    public void testCategorizeByFilterReturnsEmptyWhenNoneMatch() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("tag", "a", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Weight filterWeight = searcher.createWeight(
                    searcher.rewrite(new TermQuery(new Term("tag", "nonexistent"))),
                    ScoreMode.COMPLETE_NO_SCORES,
                    1f
                );
                ScoreDoc[][] perLeaf = new ScoreDoc[1][];
                perLeaf[0] = new ScoreDoc[] { new ScoreDoc(0, 0.9f) };
                PostFilterKnnQuery.FilteredCandidates result = PostFilterKnnQuery.applyFilter(
                    perLeaf,
                    filterWeight,
                    searcher.getIndexReader().leaves()
                );
                assertPerLeafMatchingDocs(result.matchingPerLeaf(), new int[0], new float[0]);
                assertArrayEquals(new int[] { 0 }, result.filteredOutPerLeaf()[0]);
            }
        }
    }

    public void testExpandToParentBlocksExpandsWholeBlock() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // 6 docs: children 0,1 under parent 2; children 3,4 under parent 5
            for (int i = 0; i < 6; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                BitSetProducer parentsFilter = context -> {
                    FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
                    bits.set(2);
                    bits.set(5);
                    return bits;
                };

                // Matched via one child of each parent (sorted ascending by doc, as applyFilter produces).
                ScoreDoc[][] matchingPerLeaf = new ScoreDoc[1][];
                matchingPerLeaf[0] = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(3, 0.7f) };

                int[] excluded = KnnQueryUtils.expandToParentBlocks(matchingPerLeaf, reader, parentsFilter);
                // Whole block of parent 2 ({0,1,2}) and parent 5 ({3,4,5}), sorted.
                assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 5 }, excluded);
            }
        }
    }

    public void testExpandToParentBlocksDeduplicatesSameParent() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // 6 docs: children 0,1 under parent 2; children 3,4 under parent 5
            for (int i = 0; i < 6; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                BitSetProducer parentsFilter = context -> {
                    FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
                    bits.set(2);
                    bits.set(5);
                    return bits;
                };

                // Two matched children resolving to the SAME parent 2 - block must be emitted only once.
                ScoreDoc[][] matchingPerLeaf = new ScoreDoc[1][];
                matchingPerLeaf[0] = new ScoreDoc[] { new ScoreDoc(0, 0.8f), new ScoreDoc(1, 0.9f) };

                int[] excluded = KnnQueryUtils.expandToParentBlocks(matchingPerLeaf, reader, parentsFilter);
                assertArrayEquals(new int[] { 0, 1, 2 }, excluded);
            }
        }
    }

    public void testExpandToParentBlocksAcrossLeaves() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            // Leaf 0: docs 0,1,2 with parent at 2. Leaf 1: docs 3,4,5 with parent at global 5 (local 2).
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                // Parent is the last doc of each leaf's block (local ordinal 2 in both leaves).
                BitSetProducer parentsFilter = context -> {
                    FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
                    bits.set(2);
                    return bits;
                };

                ScoreDoc[][] matchingPerLeaf = new ScoreDoc[2][];
                matchingPerLeaf[0] = new ScoreDoc[] { new ScoreDoc(0, 0.9f) }; // leaf 0, parent global 2
                matchingPerLeaf[1] = new ScoreDoc[] { new ScoreDoc(3, 0.8f) }; // leaf 1, parent global 5

                int[] excluded = KnnQueryUtils.expandToParentBlocks(matchingPerLeaf, reader, parentsFilter);
                assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 5 }, excluded);
            }
        }
    }

    public void testExpandToParentBlocksCollapsesMultipleChildrenPerParent() throws IOException {
        // Sliced IVF can surface two children of the SAME parent as separate candidates (one per slice).
        // After applyFilter sorts by doc ID, same-parent children are adjacent, so each block must be
        // emitted exactly once and the output must stay sorted (no duplicated/out-of-order block ranges).
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // 6 docs: children 0,1 under parent 2; children 3,4 under parent 5
            for (int i = 0; i < 6; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                BitSetProducer parentsFilter = context -> {
                    FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
                    bits.set(2);
                    bits.set(5);
                    return bits;
                };

                // Both children of parent 2 (docs 0,1) and both children of parent 5 (docs 3,4) matched.
                ScoreDoc[][] matchingPerLeaf = new ScoreDoc[1][];
                matchingPerLeaf[0] = new ScoreDoc[] {
                    new ScoreDoc(0, 0.9f),
                    new ScoreDoc(1, 0.8f),
                    new ScoreDoc(3, 0.7f),
                    new ScoreDoc(4, 0.6f) };

                int[] excluded = KnnQueryUtils.expandToParentBlocks(matchingPerLeaf, reader, parentsFilter);
                assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 5 }, excluded);
            }
        }
    }

    public void testExpandToParentBlocksEmptyInput() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                BitSetProducer parentsFilter = context -> {
                    FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
                    bits.set(2);
                    return bits;
                };
                assertEquals(0, KnnQueryUtils.expandToParentBlocks(new ScoreDoc[1][], reader, parentsFilter).length);
            }
        }
    }

    public void testCategorizeByFilterAcrossLeaves() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            for (int i = 0; i < 3; i++) {
                Document doc = new Document();
                doc.add(new StringField("tag", i % 2 == 0 ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();
            for (int i = 3; i < 6; i++) {
                Document doc = new Document();
                doc.add(new StringField("tag", i % 2 == 0 ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals(2, searcher.getIndexReader().leaves().size());
                Weight filterWeight = searcher.createWeight(
                    searcher.rewrite(new TermQuery(new Term("tag", "pass"))),
                    ScoreMode.COMPLETE_NO_SCORES,
                    1f
                );

                // Per-leaf candidates indexed by leaf ordinal. Scores intentionally non-monotonic
                // with docId — categorizeByFilter only reports the passing-set, regardless of order.
                ScoreDoc[][] perLeaf = new ScoreDoc[2][];
                perLeaf[0] = new ScoreDoc[] {
                    new ScoreDoc(0, 0.5f),   // seg 0, pass
                    new ScoreDoc(1, 0.95f),  // seg 0, fail
                    new ScoreDoc(2, 0.6f),   // seg 0, pass
                };
                perLeaf[1] = new ScoreDoc[] {
                    new ScoreDoc(4, 0.9f),   // seg 1, pass
                    new ScoreDoc(5, 0.7f),   // seg 1, fail
                };
                PostFilterKnnQuery.FilteredCandidates result = PostFilterKnnQuery.applyFilter(
                    perLeaf,
                    filterWeight,
                    searcher.getIndexReader().leaves()
                );
                assertPerLeafMatchingDocs(result.matchingPerLeaf(), new int[] { 0, 2, 4 }, new float[] { 0.5f, 0.6f, 0.9f });
                assertArrayEquals(new int[] { 1 }, result.filteredOutPerLeaf()[0]);
                assertArrayEquals(new int[] { 5 }, result.filteredOutPerLeaf()[1]);
            }
        }
    }

    public void testComputeSelectivity() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // 4 of 10 docs match "pass" → selectivity 0.4
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                doc.add(new StringField("tag", i < 4 ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Weight w = searcher.createWeight(
                    searcher.rewrite(new TermQuery(new Term("tag", "pass"))),
                    ScoreMode.COMPLETE_NO_SCORES,
                    1f
                );
                assertEquals(0.4f, KnnQueryUtils.computeSelectivity(w, searcher.getIndexReader().leaves(), 10), 0.001f);
            }
        }
    }

    public void testComputeSelectivityClampsToOne() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 5; i++) {
                Document doc = new Document();
                doc.add(new StringField("tag", "a", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Weight w = searcher.createWeight(searcher.rewrite(new TermQuery(new Term("tag", "a"))), ScoreMode.COMPLETE_NO_SCORES, 1f);
                // filterCost (5) > totalVectors (2) — raw ratio would be 2.5, expect clamp to 1
                assertEquals(1f, KnnQueryUtils.computeSelectivity(w, searcher.getIndexReader().leaves(), 2), 0f);
            }
        }
    }

    public void testComputeSelectivityZeroTotalVectors() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("tag", "a", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Weight w = searcher.createWeight(searcher.rewrite(new TermQuery(new Term("tag", "a"))), ScoreMode.COMPLETE_NO_SCORES, 1f);
                assertEquals(0f, KnnQueryUtils.computeSelectivity(w, searcher.getIndexReader().leaves(), 0), 0f);
            }
        }
    }

    public void testSampleOrdinalsAreBoundedUniqueAndRepeatable() {
        long[] populations = { 0, 1, 20, 21, 100, 500, 1000, 45500, Integer.MAX_VALUE, Long.MAX_VALUE };
        int[] expectedSizes = { 0, 1, 20, 21, 84, 251, 334, 495, 500, 500 };
        for (int i = 0; i < populations.length; i++) {
            long[] sample = KnnQueryUtils.sampleOrdinals(populations[i]);
            assertEquals(expectedSizes[i], sample.length);
            assertArrayEquals(sample, KnnQueryUtils.sampleOrdinals(populations[i]));
            long previous = -1;
            for (long ordinal : sample) {
                assertTrue(ordinal > previous);
                assertTrue(ordinal < populations[i]);
                previous = ordinal;
            }
        }
    }

    public void testEstimateSelectivityBySamplingCountsNestedVectorsAcrossCacheStates() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // Nineteen vectors are all sampled. Unequal block sizes distinguish child coverage from parent coverage.
            int[] childrenPerParent = { 1, 3, 15 };
            for (int parent = 0; parent < childrenPerParent.length; parent++) {
                List<Document> block = new ArrayList<>();
                for (int child = 0; child < childrenPerParent[parent]; child++) {
                    block.add(selectivityVectorDoc());
                }
                block.add(new Document()); // A nested child without the target vector must not enter the denominator.
                Document root = new Document();
                root.add(new StringField("kind", "parent", Field.Store.NO));
                root.add(new StringField("tag", parent < 2 ? "pass" : "fail", Field.Store.NO));
                block.add(root);
                writer.addDocuments(block);
            }
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                LRUQueryCache cache = new LRUQueryCache(100, 10_000_000, ctx -> true, 1f);
                searcher.setQueryCache(cache);
                searcher.setQueryCachingPolicy(TrivialQueryCachingPolicy.NEVER);
                BitSetProducer parents = new QueryBitSetProducer(new TermQuery(new Term("kind", "parent")));
                Query filter = new ToChildBlockJoinQuery(new TermQuery(new Term("tag", "pass")), parents);
                Weight cold = KnnQueryUtils.createFilterWeight(searcher, filter, "floats").weight();
                assertEquals(2L, cold.scorerSupplier(reader.leaves().getFirst()).cost());
                assertSampledSelectivity(4f / 19, cold, reader, 0f);
                assertEquals(0L, cache.getCacheCount());

                var floatQuery = new ESDiversifyingChildrenFloatKnnVectorQuery(
                    "floats",
                    new float[] { 1, 2 },
                    filter,
                    1,
                    10,
                    parents,
                    null
                );
                var byteQuery = new ESDiversifyingChildrenByteKnnVectorQuery("bytes", new byte[] { 1, 2 }, filter, 1, 10, parents, null);
                assertEquals(4f / 19, floatQuery.estimateFilterSelectivity(cold, reader.leaves()), 0f);
                assertEquals(4f / 19, byteQuery.estimateFilterSelectivity(cold, reader.leaves()), 0f);

                searcher.setQueryCachingPolicy(TrivialQueryCachingPolicy.ALWAYS);
                assertEquals(4, searcher.count(cold.getQuery()));
                assertTrue(cache.getCacheCount() > 0);
                Weight warm = KnnQueryUtils.createFilterWeight(searcher, filter, "floats").weight();
                assertEquals(4L, warm.scorerSupplier(reader.leaves().getFirst()).cost());
                long cacheHits = cache.getHitCount();
                assertSampledSelectivity(4f / 19, warm, reader, 0f);
                assertTrue(cache.getHitCount() > cacheHits);
            }
        }
    }

    public void testEstimateSelectivityBySamplingHandlesSparseNonNestedVectorsAndTwoPhaseFilter() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 20; i++) {
                Document doc = i % 2 == 0 ? selectivityVectorDoc() : new Document();
                doc.add(new NumericDocValuesField("flag", i % 4 == 0 ? 1 : 0));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                Weight filter = selectivityFilterWeight(searcher, NumericDocValuesField.newSlowExactQuery("flag", 1));
                assertNotNull(filter.scorer(reader.leaves().getFirst()).twoPhaseIterator());
                // Five matching vectors out of ten; neither all 20 docs nor the approximation's matches are the population.
                assertSampledSelectivity(0.5f, filter, reader, 0f);
                assertSampledSelectivity(1f, selectivityFilterWeight(searcher, new MatchAllDocsQuery()), reader, 0f);
                Weight noMatches = selectivityFilterWeight(searcher, new TermQuery(new Term("missing", "value")));
                assertNull(noMatches.scorerSupplier(reader.leaves().getFirst()));
                assertSampledSelectivity(0f, noMatches, reader, 0f);
            }
        }
    }

    public void testEstimateSelectivityBySamplingWithNoVectors() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            try (IndexReader empty = DirectoryReader.open(writer)) {
                Weight filter = selectivityFilterWeight(new IndexSearcher(empty), new MatchAllDocsQuery());
                assertSampledSelectivity(0f, filter, empty, 0f);
            }
            Document doc = new Document();
            doc.add(new StringField("tag", "pass", Field.Store.NO));
            writer.addDocument(doc);
            try (IndexReader reader = DirectoryReader.open(writer)) {
                Weight filter = selectivityFilterWeight(new IndexSearcher(reader), new MatchAllDocsQuery());
                assertSampledSelectivity(0f, filter, reader, 0f);
                assertEquals(0f, KnnQueryUtils.estimateFilterSelectivityBySampling(filter, reader.leaves(), "tag"), 0f);
            }
        }
    }

    public void testEstimateSelectivityBySampleWeightsLeavesByVectorCount() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(2000);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < 10; i++) {
                writer.addDocument(selectivityVectorDoc());
            }
            writer.commit();
            for (int i = 0; i < 990; i++) {
                Document doc = selectivityVectorDoc();
                doc.add(new StringField("tag", "pass", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();
            Document noVector = new Document();
            noVector.add(new StringField("tag", "pass", Field.Store.NO));
            writer.addDocument(noVector);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertEquals(3, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                Weight filter = selectivityFilterWeight(searcher, new TermQuery(new Term("tag", "pass")));
                // Equal per-leaf sampling would estimate 0.5, instead of the true vector coverage 0.99.
                assertSampledSelectivity(0.99f, filter, reader, 0.03f);
            }
        }
    }

    public void testEstimateSelectivityBySamplingExcludesDeletedVectorsFromBothCounts() throws IOException {
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < 20; i++) {
                Document doc = selectivityVectorDoc();
                doc.add(new StringField("tag", i < 5 ? "keep" : "delete", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();
            writer.deleteDocuments(new Term("tag", "delete"));
            try (IndexReader reader = DirectoryReader.open(writer)) {
                assertEquals(20, reader.maxDoc());
                assertEquals(5, reader.numDocs());
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                assertSampledSelectivity(1f, selectivityFilterWeight(searcher, new TermQuery(new Term("tag", "keep"))), reader, 0f);
                assertSampledSelectivity(0f, selectivityFilterWeight(searcher, new TermQuery(new Term("tag", "delete"))), reader, 0f);
            }
        }
    }

    public void testEstimateSelectivityBySampleReturnsZeroWhenEverySamplingIsDeleted() throws IOException {
        long[] sampledOrdinals = KnnQueryUtils.sampleOrdinals(1000);
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(2000);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < 1000; i++) {
                Document doc = selectivityVectorDoc();
                // Deliberately remove just the deterministic sample, leaving other vectors searchable.
                doc.add(new StringField("tag", Arrays.binarySearch(sampledOrdinals, i) >= 0 ? "delete" : "keep", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.commit();
            writer.deleteDocuments(new Term("tag", "delete"));
            try (IndexReader reader = DirectoryReader.open(writer)) {
                assertEquals(1000, reader.maxDoc());
                assertEquals(1000 - sampledOrdinals.length, reader.numDocs());
                Weight filter = selectivityFilterWeight(new IndexSearcher(reader), new MatchAllDocsQuery());
                assertSampledSelectivity(0f, filter, reader, 0f);
            }
        }
    }

    public void testEstimateSelectivityBySamplingRestrictsOrdinalRanges() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 15; i++) {
                writer.addDocument(new Document()); // Make vector ordinals differ from doc IDs.
                Document doc = selectivityVectorDoc();
                doc.add(new StringField("tag", i >= 10 ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(writer)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                Weight filter = selectivityFilterWeight(searcher, new TermQuery(new Term("tag", "pass")));
                LeafReaderContext leaf = reader.leaves().getFirst();
                var vectors = leaf.reader().getFloatVectorValues("floats");
                // Disjoint ranges in reverse doc order also require sorting before sequential filter access.
                List<KnnQueryUtils.VectorRange> ranges = List.of(
                    new KnnQueryUtils.VectorRange(leaf, vectors, 10, 15),
                    new KnnQueryUtils.VectorRange(leaf, vectors, 5, 5),
                    new KnnQueryUtils.VectorRange(leaf, vectors, 0, 5)
                );
                assertEquals(0.5f, KnnQueryUtils.estimateFilterSelectivityBySampling(filter, ranges), 0f);
                assertEquals(0f, KnnQueryUtils.estimateFilterSelectivityBySampling(filter, List.of()), 0f);
            }
        }
    }

    private static Document selectivityVectorDoc() {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("floats", new float[] { 1, 2 }));
        doc.add(new KnnByteVectorField("bytes", new byte[] { 1, 2 }));
        return doc;
    }

    private static Weight selectivityFilterWeight(IndexSearcher searcher, Query filter) throws IOException {
        return searcher.createWeight(searcher.rewrite(filter), ScoreMode.COMPLETE_NO_SCORES, 1f);
    }

    private static void assertSampledSelectivity(float expected, Weight filter, IndexReader reader, float delta) throws IOException {
        for (String field : List.of("floats", "bytes")) {
            float estimate = KnnQueryUtils.estimateFilterSelectivityBySampling(filter, reader.leaves(), field);
            assertEquals(expected, estimate, delta);
            assertEquals(estimate, KnnQueryUtils.estimateFilterSelectivityBySampling(filter, reader.leaves(), field), 0f);
        }
    }

    public void testCreateFilterWeightNullFilter() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("tag", "a", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertNull(KnnQueryUtils.createFilterWeight(searcher, null, "tag"));
            }
        }
    }

    public void testCreateFilterWeightMatchNoDocsCollapsesToMatchNoDocs() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("tag", "a", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                // A MatchNoDocsQuery as one of the FILTER clauses collapses the whole boolean
                // query to MatchNoDocsQuery on rewrite, which createFilterWeight signals via MATCH_NO_DOCS.
                assertSame(
                    KnnQueryUtils.FilterWeight.MATCH_NO_DOCS,
                    KnnQueryUtils.createFilterWeight(searcher, MatchNoDocsQuery.INSTANCE, "tag")
                );
            }
        }
    }

    public void testCreateFilterWeightValidFilter() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            // indexedField gives the field both a term index (for the TermQuery) and doc-values,
            // which FieldExistsQuery — added by createFilterWeight — needs to resolve.
            doc.add(SortedDocValuesField.indexedField("tag", new BytesRef("a")));
            writer.addDocument(doc);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                KnnQueryUtils.FilterWeight result = KnnQueryUtils.createFilterWeight(searcher, new TermQuery(new Term("tag", "a")), "tag");
                assertNotNull(result);
                assertNotNull(result.weight());
            }
        }
    }

    /**
     * Counting the wrong encoding yields 0, which {@link KnnQueryUtils#computeSelectivity} turns into a
     * selectivity of 0 and the orchestrator treats as "no estimate, do not post-filter". That silent
     * degradation is why the counters are split by encoding rather than shared.
     */
    public void testVectorCountersAreEncodingSpecific() throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("floats", new float[] { i, i }));
                    doc.add(new KnnByteVectorField("bytes", new byte[] { (byte) i, (byte) i }));
                    writer.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(dir)) {
                List<LeafReaderContext> leaves = reader.leaves();
                assertEquals(5, KnnQueryUtils.countFloatVectors("floats", leaves));
                assertEquals(5, KnnQueryUtils.countByteVectors("bytes", leaves));
                assertEquals("float counter on a byte field sees nothing", 0, KnnQueryUtils.countFloatVectors("bytes", leaves));
                assertEquals("byte counter on a float field sees nothing", 0, KnnQueryUtils.countByteVectors("floats", leaves));
                assertEquals("absent field", 0, KnnQueryUtils.countFloatVectors("missing", leaves));
            }
        }
    }
}
