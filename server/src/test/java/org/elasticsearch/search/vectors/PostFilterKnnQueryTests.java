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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PostFilterKnnQueryTests extends ESTestCase {

    public void testMergeResults() {
        {
            ScoreDoc[] result = KnnQueryUtils.mergeResults(new ScoreDoc[0], new ScoreDoc[0]);
            assertEquals(0, result.length);
        }
        {
            ScoreDoc[] input = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.8f) };
            ScoreDoc[] result = KnnQueryUtils.mergeResults(new ScoreDoc[0], input);
            assertEquals(2, result.length);
            assertEquals(1, result[0].doc);
            assertEquals(2, result[1].doc);
        }
        {
            ScoreDoc[] existing = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(3, 0.7f) };
            ScoreDoc[] incoming = new ScoreDoc[] { new ScoreDoc(2, 0.8f), new ScoreDoc(4, 0.6f) };
            ScoreDoc[] result = KnnQueryUtils.mergeResults(existing, incoming);
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
        {
            ScoreDoc[] existing = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.7f), new ScoreDoc(4, 0.7f) };
            ScoreDoc[] incoming = new ScoreDoc[] { new ScoreDoc(2, 0.8f), new ScoreDoc(3, 0.6f) };
            ScoreDoc[] result = KnnQueryUtils.mergeResults(existing, incoming);
            // doc 2 appears in both; the higher-ranked one (0.8 from incoming) wins
            // Merge order: 1(0.9) from existing, 2(0.8) from incoming, 2(0.7) from existing (dup skipped), 3(0.6)
            assertEquals(4, result.length);
            assertEquals(1, result[0].doc);
            assertEquals(0.9f, result[0].score, 0.001f);
            assertEquals(2, result[1].doc);
            assertEquals(0.8f, result[1].score, 0.001f);
            assertEquals(4, result[2].doc);
            assertEquals(0.7f, result[2].score, 0.001f);
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
                // Even docs (0, 2, 4) pass, sorted by score descending
                assertEquals(3, result.length);
                assertEquals(0, result[0].doc);
                assertEquals(0.9f, result[0].score, 0.001f);
                assertEquals(2, result[1].doc);
                assertEquals(0.7f, result[1].score, 0.001f);
                assertEquals(4, result[2].doc);
                assertEquals(0.5f, result[2].score, 0.001f);
            }
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

    public void testDeduplicateByParentKeepsHighestScoring() throws IOException {
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

                // Sorted by score descending
                ScoreDoc[] docs = new ScoreDoc[] {
                    new ScoreDoc(1, 0.9f),  // child → parent 2
                    new ScoreDoc(0, 0.8f),  // child → parent 2 (dup)
                    new ScoreDoc(4, 0.7f),  // child → parent 5
                    new ScoreDoc(3, 0.6f),  // child → parent 5 (dup)
                };

                ScoreDoc[] result = KnnQueryUtils.deduplicateByParent(docs, reader, parentsFilter);
                assertEquals(2, result.length);
                assertEquals(1, result[0].doc);
                assertEquals(0.9f, result[0].score, 0.001f);
                assertEquals(4, result[1].doc);
                assertEquals(0.7f, result[1].score, 0.001f);
            }
        }
    }

    public void testDeduplicateByParentNoParents() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                // No parent bits set → all docs filtered out (no parent found)
                BitSetProducer parentsFilter = context -> new FixedBitSet(context.reader().maxDoc());

                ScoreDoc[] docs = new ScoreDoc[] { new ScoreDoc(0, 0.9f), new ScoreDoc(1, 0.8f) };
                ScoreDoc[] result = KnnQueryUtils.deduplicateByParent(docs, reader, parentsFilter);
                // nextSetBit returns NO_MORE_DOCS for empty bitset, so all are filtered
                assertEquals(0, result.length);
            }
        }
    }

    public void testExcludeDocsQueryFiltersCorrectly() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 5; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);

                int[] excluded = new int[] { 1, 3 };

                ExcludeDocsQuery query = new ExcludeDocsQuery(excluded, searcher.getIndexReader());
                Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);

                LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
                ScorerSupplier ss = weight.scorerSupplier(leaf);
                assertNotNull(ss);

                Scorer scorer = ss.get(0);
                DocIdSetIterator iter = scorer.iterator();

                assertEquals(0, iter.nextDoc());
                assertEquals(2, iter.nextDoc());
                assertEquals(4, iter.nextDoc());
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, iter.nextDoc());
            }
        }
    }

    public void testExcludeDocsQueryReturnsNullWhenAllExcluded() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);

                int[] excluded = new int[] { 0, 1, 2, };

                ExcludeDocsQuery query = new ExcludeDocsQuery(excluded, searcher.getIndexReader());
                Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);

                LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
                assertNull(weight.scorerSupplier(leaf));
            }
        }
    }

    public void testExcludeDocsQueryEquality() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 5; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                int[] excluded1 = new int[] { 1 };
                int[] excluded2 = new int[] { 1 };
                int[] excluded3 = new int[] { 2 };

                ExcludeDocsQuery q1 = new ExcludeDocsQuery(excluded1, reader);
                ExcludeDocsQuery q2 = new ExcludeDocsQuery(excluded2, reader);
                ExcludeDocsQuery q3 = new ExcludeDocsQuery(excluded3, reader);

                assertEquals(q1, q2);
                assertEquals(q1.hashCode(), q2.hashCode());
                assertNotEquals(q1, q3);
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

                // Scores are intentionally not monotonic with docId so we also exercise the
                // "sort back by score descending" tail of applyFilter.
                ScoreDoc[] candidates = new ScoreDoc[] {
                    new ScoreDoc(0, 0.5f),   // seg 0, pass
                    new ScoreDoc(1, 0.95f),  // seg 0, fail
                    new ScoreDoc(2, 0.6f),   // seg 0, pass
                    new ScoreDoc(4, 0.9f),   // seg 1, pass
                    new ScoreDoc(5, 0.7f),   // seg 1, fail
                };
                ScoreDoc[] result = KnnQueryUtils.applyFilter(candidates, filterWeight, searcher);
                assertEquals(3, result.length);
                assertEquals(4, result[0].doc);
                assertEquals(0.9f, result[0].score, 0.001f);
                assertEquals(2, result[1].doc);
                assertEquals(0.6f, result[1].score, 0.001f);
                assertEquals(0, result[2].doc);
                assertEquals(0.5f, result[2].score, 0.001f);
            }
        }
    }

    public void testDeduplicateByParentNullFilterReturnsInputUnchanged() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ScoreDoc[] docs = new ScoreDoc[] { new ScoreDoc(0, 0.9f), new ScoreDoc(1, 0.8f) };
                ScoreDoc[] result = KnnQueryUtils.deduplicateByParent(docs, reader, null);
                assertSame(docs, result);
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

    public void testCreateFilterWeightMatchNoDocsCollapsesToNull() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("tag", "a", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                // A MatchNoDocsQuery as one of the FILTER clauses collapses the whole boolean
                // query to MatchNoDocsQuery on rewrite, which createFilterWeight maps to null.
                assertNull(KnnQueryUtils.createFilterWeight(searcher, MatchNoDocsQuery.INSTANCE, "tag"));
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
                Weight weight = KnnQueryUtils.createFilterWeight(searcher, new TermQuery(new Term("tag", "a")), "tag");
                assertNotNull(weight);
            }
        }
    }

    public void testExcludeDocsQueryExplain() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                ExcludeDocsQuery q = new ExcludeDocsQuery(new int[] { 1 }, searcher.getIndexReader());
                Weight weight = searcher.createWeight(q, ScoreMode.COMPLETE_NO_SCORES, 1f);
                LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);

                Explanation excluded = weight.explain(leaf, 1);
                assertFalse(excluded.isMatch());

                Explanation kept = weight.explain(leaf, 0);
                assertTrue(kept.isMatch());
            }
        }
    }

    public void testExcludeDocsQueryAcrossLeaves() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals(2, searcher.getIndexReader().leaves().size());

                // Exclude global doc 1 (seg 0) and global doc 4 (= local 1 in seg 1) — verifies
                // that the per-leaf binary-search slice correctly maps global to local IDs.
                ExcludeDocsQuery q = new ExcludeDocsQuery(new int[] { 1, 4 }, searcher.getIndexReader());
                Weight weight = searcher.createWeight(q, ScoreMode.COMPLETE_NO_SCORES, 1f);

                LeafReaderContext leaf0 = searcher.getIndexReader().leaves().get(0);
                DocIdSetIterator it0 = weight.scorerSupplier(leaf0).get(0).iterator();
                assertEquals(0, it0.nextDoc());
                assertEquals(2, it0.nextDoc());
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, it0.nextDoc());

                LeafReaderContext leaf1 = searcher.getIndexReader().leaves().get(1);
                DocIdSetIterator it1 = weight.scorerSupplier(leaf1).get(0).iterator();
                assertEquals(0, it1.nextDoc());
                assertEquals(2, it1.nextDoc());
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, it1.nextDoc());
            }
        }
    }

    public void testExcludeDocsQueryLeafWithNoExcludedDocs() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals(2, searcher.getIndexReader().leaves().size());

                // Only seg 0 docs are excluded — seg 1 sees an empty slice of excludedDocs and
                // should pass every doc through unchanged.
                ExcludeDocsQuery q = new ExcludeDocsQuery(new int[] { 0, 1 }, searcher.getIndexReader());
                Weight weight = searcher.createWeight(q, ScoreMode.COMPLETE_NO_SCORES, 1f);

                LeafReaderContext leaf1 = searcher.getIndexReader().leaves().get(1);
                DocIdSetIterator it = weight.scorerSupplier(leaf1).get(0).iterator();
                assertEquals(0, it.nextDoc());
                assertEquals(1, it.nextDoc());
                assertEquals(2, it.nextDoc());
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
            }
        }
    }

    public void testExcludeDocsQueryRejectsForeignReader() throws IOException {
        try (Directory dir1 = newDirectory(); Directory dir2 = newDirectory()) {
            try (IndexWriter w1 = new IndexWriter(dir1, new IndexWriterConfig())) {
                w1.addDocument(new Document());
                w1.commit();
            }
            try (IndexWriter w2 = new IndexWriter(dir2, new IndexWriterConfig())) {
                w2.addDocument(new Document());
                w2.commit();
            }
            try (IndexReader r1 = DirectoryReader.open(dir1); IndexReader r2 = DirectoryReader.open(dir2)) {
                ExcludeDocsQuery q = new ExcludeDocsQuery(new int[] { 0 }, r1);
                IndexSearcher s2 = newSearcher(r2);
                expectThrows(IllegalStateException.class, () -> s2.createWeight(q, ScoreMode.COMPLETE_NO_SCORES, 1f));
            }
        }
    }

    public void testExcludeDocsQueryToString() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ExcludeDocsQuery q = new ExcludeDocsQuery(new int[] { 1, 2, 3 }, reader);
                assertEquals("ExcludeDocsQuery[count=3]", q.toString("any-field"));
            }
        }
    }
}
