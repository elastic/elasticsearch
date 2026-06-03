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
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

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

    public void testDedupAndSelectTopKWithoutParentsDeduplicatesByDocId() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 5; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ScoreDoc[] input = new ScoreDoc[] {
                    new ScoreDoc(1, 0.9f),
                    new ScoreDoc(3, 0.7f),
                    new ScoreDoc(2, 0.8f),
                    new ScoreDoc(4, 0.6f) };
                ScoreDoc[] result = KnnQueryUtils.dedupAndSelectTopK(input, reader, null, 4);
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
        }
    }

    public void testDedupAndSelectTopKDocIdCollisionKeepsHighestScore() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 5; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                // doc 2 appears twice with scores 0.7 and 0.8; the higher must win
                ScoreDoc[] input = new ScoreDoc[] {
                    new ScoreDoc(1, 0.9f),
                    new ScoreDoc(2, 0.7f),
                    new ScoreDoc(4, 0.7f),
                    new ScoreDoc(2, 0.8f),
                    new ScoreDoc(3, 0.6f) };
                ScoreDoc[] result = KnnQueryUtils.dedupAndSelectTopK(input, reader, null, 5);
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
        }
    }

    public void testDedupAndSelectTopKPartialSelectionKeepsTopK() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 10; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
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
                ScoreDoc[] result = KnnQueryUtils.dedupAndSelectTopK(input, reader, null, 3);
                assertEquals(3, result.length);
                // Top 3 scores: 0.95 (doc 9), 0.9 (doc 1), 0.8 (doc 3), in descending order.
                assertEquals(9, result[0].doc);
                assertEquals(0.95f, result[0].score, 0.001f);
                assertEquals(1, result[1].doc);
                assertEquals(0.9f, result[1].score, 0.001f);
                assertEquals(3, result[2].doc);
                assertEquals(0.8f, result[2].score, 0.001f);
            }
        }
    }

    public void testDedupAndSelectTopKEmptyInputOrZeroK() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                assertEquals(0, KnnQueryUtils.dedupAndSelectTopK(new ScoreDoc[0], reader, null, 5).length);
                ScoreDoc[] result = KnnQueryUtils.dedupAndSelectTopK(new ScoreDoc[] { new ScoreDoc(0, 1f) }, reader, null, 0);
                assertEquals(0, result.length);
            }
        }
    }

    public void testApplyFilterPassesMatchingDocs() throws IOException {
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

                ScoreDoc[] candidates = new ScoreDoc[] {
                    new ScoreDoc(0, 0.9f),
                    new ScoreDoc(1, 0.8f),
                    new ScoreDoc(2, 0.7f),
                    new ScoreDoc(3, 0.6f),
                    new ScoreDoc(4, 0.5f) };

                ScoreDoc[] result = KnnQueryUtils.applyFilter(candidates, filterWeight, searcher);
                // Even docs (0, 2, 4) pass; output order is unspecified, only set membership matters
                // since dedupAndSelectTopK is the step that imposes ordering downstream.
                assertEquals(3, result.length);
                assertPassingDocsMatch(result, new int[] { 0, 2, 4 }, new float[] { 0.9f, 0.7f, 0.5f });
            }
        }
    }

    private static void assertPassingDocsMatch(ScoreDoc[] actual, int[] expectedDocs, float[] expectedScores) {
        assertEquals("doc count", expectedDocs.length, actual.length);
        for (int i = 0; i < expectedDocs.length; i++) {
            int doc = expectedDocs[i];
            float score = expectedScores[i];
            boolean found = false;
            for (ScoreDoc sd : actual) {
                if (sd.doc == doc) {
                    assertEquals("score for doc " + doc, score, sd.score, 0.001f);
                    found = true;
                    break;
                }
            }
            assertTrue("missing doc " + doc, found);
        }
    }

    public void testApplyFilterReturnsEmptyWhenNoneMatch() throws IOException {
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
                ScoreDoc[] candidates = new ScoreDoc[] { new ScoreDoc(0, 0.9f) };
                ScoreDoc[] result = KnnQueryUtils.applyFilter(candidates, filterWeight, searcher);
                assertEquals(0, result.length);
            }
        }
    }

    public void testDedupAndSelectTopKByParentKeepsHighestScoringChild() throws IOException {
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

                // Input order intentionally jumbled — max-score per parent should still win.
                ScoreDoc[] docs = new ScoreDoc[] {
                    new ScoreDoc(0, 0.8f),  // child → parent 2
                    new ScoreDoc(3, 0.6f),  // child → parent 5
                    new ScoreDoc(1, 0.9f),  // child → parent 2 (higher than 0.8)
                    new ScoreDoc(4, 0.7f),  // child → parent 5 (higher than 0.6)
                };

                ScoreDoc[] result = KnnQueryUtils.dedupAndSelectTopK(docs, reader, parentsFilter, 5);
                assertEquals(2, result.length);
                assertEquals(1, result[0].doc);
                assertEquals(0.9f, result[0].score, 0.001f);
                assertEquals(4, result[1].doc);
                assertEquals(0.7f, result[1].score, 0.001f);
            }
        }
    }

    public void testDedupAndSelectTopKEmptyParentBitsetDropsAllDocs() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                // No parent bits set → every child resolves to NO_MORE_DOCS and is dropped.
                BitSetProducer parentsFilter = context -> new FixedBitSet(context.reader().maxDoc());

                ScoreDoc[] docs = new ScoreDoc[] { new ScoreDoc(0, 0.9f), new ScoreDoc(1, 0.8f) };
                ScoreDoc[] result = KnnQueryUtils.dedupAndSelectTopK(docs, reader, parentsFilter, 5);
                assertEquals(0, result.length);
            }
        }
    }

    public void testDedupAndSelectTopKByParentRespectsTopK() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // 9 docs: children 0,1 → parent 2; child 3 → parent 4; children 5,6,7 → parent 8
            for (int i = 0; i < 9; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                BitSetProducer parentsFilter = context -> {
                    FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
                    bits.set(2);
                    bits.set(4);
                    bits.set(8);
                    return bits;
                };

                ScoreDoc[] docs = new ScoreDoc[] {
                    new ScoreDoc(0, 0.8f), // → parent 2
                    new ScoreDoc(1, 0.95f), // → parent 2 (winning child)
                    new ScoreDoc(3, 0.7f), // → parent 4
                    new ScoreDoc(5, 0.5f), // → parent 8
                    new ScoreDoc(6, 0.9f), // → parent 8 (winning child)
                    new ScoreDoc(7, 0.4f), // → parent 8
                };

                // 3 unique parents available; ask for only 2 — top 2 by winning-child score.
                ScoreDoc[] result = KnnQueryUtils.dedupAndSelectTopK(docs, reader, parentsFilter, 2);
                assertEquals(2, result.length);
                assertEquals(1, result[0].doc);
                assertEquals(0.95f, result[0].score, 0.001f);
                assertEquals(6, result[1].doc);
                assertEquals(0.9f, result[1].score, 0.001f);
            }
        }
    }

    public void testApplyFilterAcrossLeaves() throws IOException {
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

                // Scores intentionally non-monotonic with docId — applyFilter only reports the
                // passing-set, regardless of order.
                ScoreDoc[] candidates = new ScoreDoc[] {
                    new ScoreDoc(0, 0.5f),   // seg 0, pass
                    new ScoreDoc(1, 0.95f),  // seg 0, fail
                    new ScoreDoc(2, 0.6f),   // seg 0, pass
                    new ScoreDoc(4, 0.9f),   // seg 1, pass
                    new ScoreDoc(5, 0.7f),   // seg 1, fail
                };
                ScoreDoc[] result = KnnQueryUtils.applyFilter(candidates, filterWeight, searcher);
                assertPassingDocsMatch(result, new int[] { 0, 2, 4 }, new float[] { 0.5f, 0.6f, 0.9f });
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

    public void testAugmentFilterReturnsBaseWhenExcludedIsEmptyOrNull() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                Query base = new TermQuery(new Term("tag", "a"));
                assertSame(base, KnnQueryUtils.augmentFilter(base, new int[0], reader));
                assertSame(base, KnnQueryUtils.augmentFilter(base, null, reader));
            }
        }
    }

    public void testAugmentFilterReturnsBareExcludeWhenBaseIsNull() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                Query result = KnnQueryUtils.augmentFilter(null, new int[] { 0 }, reader);
                assertTrue("expected ExcludeDocsQuery but got " + result.getClass(), result instanceof ExcludeDocsQuery);
            }
        }
    }

    public void testAugmentFilterCombinesBaseAndExclude() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                Query base = new TermQuery(new Term("tag", "a"));
                Query result = KnnQueryUtils.augmentFilter(base, new int[] { 0 }, reader);
                assertTrue("expected BooleanQuery but got " + result.getClass(), result instanceof BooleanQuery);
                BooleanQuery bq = (BooleanQuery) result;
                assertEquals(2, bq.clauses().size());
                for (BooleanClause c : bq.clauses()) {
                    assertEquals(BooleanClause.Occur.FILTER, c.occur());
                }
            }
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
}
