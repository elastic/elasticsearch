/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class PostFilterAwareKnnQueryTests extends ESTestCase {

    // ==================== mergeResults tests ====================

    public void testMergeResultsBothEmpty() {
        ScoreDoc[] result = PostFilterAwareKnnQuery.mergeResults(new ScoreDoc[0], new ScoreDoc[0]);
        assertEquals(0, result.length);
    }

    public void testMergeResultsFirstEmpty() {
        ScoreDoc[] input = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.8f) };
        ScoreDoc[] result = PostFilterAwareKnnQuery.mergeResults(new ScoreDoc[0], input);
        assertEquals(2, result.length);
        assertEquals(1, result[0].doc);
        assertEquals(2, result[1].doc);
    }

    public void testMergeResultsSecondEmpty() {
        ScoreDoc[] input = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.8f) };
        ScoreDoc[] result = PostFilterAwareKnnQuery.mergeResults(input, new ScoreDoc[0]);
        assertEquals(2, result.length);
        assertEquals(1, result[0].doc);
        assertEquals(2, result[1].doc);
    }

    public void testMergeResultsMergesByScoreDescending() {
        ScoreDoc[] existing = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(3, 0.7f) };
        ScoreDoc[] incoming = new ScoreDoc[] { new ScoreDoc(2, 0.8f), new ScoreDoc(4, 0.6f) };
        ScoreDoc[] result = PostFilterAwareKnnQuery.mergeResults(existing, incoming);
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

    public void testMergeResultsDeduplicatesByDocId() {
        ScoreDoc[] existing = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.7f) };
        ScoreDoc[] incoming = new ScoreDoc[] { new ScoreDoc(2, 0.8f), new ScoreDoc(3, 0.6f) };
        ScoreDoc[] result = PostFilterAwareKnnQuery.mergeResults(existing, incoming);
        // doc 2 appears in both; the higher-ranked one (0.8 from incoming) wins
        // Merge order: 1(0.9) from existing, 2(0.8) from incoming, 2(0.7) from existing (dup skipped), 3(0.6)
        assertEquals(3, result.length);
        assertEquals(1, result[0].doc);
        assertEquals(0.9f, result[0].score, 0.001f);
        assertEquals(2, result[1].doc);
        assertEquals(0.8f, result[1].score, 0.001f);
        assertEquals(3, result[2].doc);
        assertEquals(0.6f, result[2].score, 0.001f);
    }

    // ==================== applyFilter tests ====================

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

                ScoreDoc[] result = PostFilterAwareKnnQuery.applyFilter(candidates, filterWeight, searcher);
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
                ScoreDoc[] result = PostFilterAwareKnnQuery.applyFilter(candidates, filterWeight, searcher);
                assertEquals(0, result.length);
            }
        }
    }

    // ==================== deduplicateByParent tests ====================

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

                ScoreDoc[] result = PostFilterAwareKnnQuery.deduplicateByParent(docs, reader, parentsFilter);
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
                BitSetProducer parentsFilter = context -> { return new FixedBitSet(context.reader().maxDoc()); };

                ScoreDoc[] docs = new ScoreDoc[] { new ScoreDoc(0, 0.9f), new ScoreDoc(1, 0.8f) };
                ScoreDoc[] result = PostFilterAwareKnnQuery.deduplicateByParent(docs, reader, parentsFilter);
                // nextSetBit returns NO_MORE_DOCS for empty bitset, so all are filtered
                assertEquals(0, result.length);
            }
        }
    }

    // ==================== ExcludeDocsQuery tests ====================

    public void testExcludeDocsQueryFiltersCorrectly() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 5; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);

                FixedBitSet excluded = new FixedBitSet(reader.maxDoc());
                excluded.set(1);
                excluded.set(3);

                ExcludeDocsQuery query = new ExcludeDocsQuery(excluded, reader);
                Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);

                LeafReaderContext leaf = reader.leaves().get(0);
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

                FixedBitSet excluded = new FixedBitSet(reader.maxDoc());
                excluded.set(0);
                excluded.set(1);
                excluded.set(2);

                ExcludeDocsQuery query = new ExcludeDocsQuery(excluded, reader);
                Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);

                LeafReaderContext leaf = reader.leaves().get(0);
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
                FixedBitSet excluded1 = new FixedBitSet(reader.maxDoc());
                excluded1.set(1);
                FixedBitSet excluded2 = new FixedBitSet(reader.maxDoc());
                excluded2.set(1);
                FixedBitSet excluded3 = new FixedBitSet(reader.maxDoc());
                excluded3.set(2);

                ExcludeDocsQuery q1 = new ExcludeDocsQuery(excluded1, reader);
                ExcludeDocsQuery q2 = new ExcludeDocsQuery(excluded2, reader);
                ExcludeDocsQuery q3 = new ExcludeDocsQuery(excluded3, reader);

                assertEquals(q1, q2);
                assertEquals(q1.hashCode(), q2.hashCode());
                assertNotEquals(q1, q3);
            }
        }
    }

    // ==================== Retry loop integration tests ====================

    public void testRetryLoopRetriesAndAccumulatesResults() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // 10 docs: even indices tagged "pass", odd tagged "fail"
            for (int i = 0; i < 10; i++) {
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

                // Round 2: docs [0, 4, 6, 7, 8] → filter passes [0, 4, 6, 8]
                FakePostFilterableQuery round2 = new FakePostFilterableQuery(
                    new TopDocs(
                        new TotalHits(5, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            new ScoreDoc(0, 0.85f),
                            new ScoreDoc(4, 0.75f),
                            new ScoreDoc(6, 0.65f),
                            new ScoreDoc(7, 0.55f),
                            new ScoreDoc(8, 0.45f) }
                    ),
                    80,
                    null
                );

                // Round 1: docs [1, 2, 3, 5, 9] → filter passes [2]
                FakePostFilterableQuery round1 = new FakePostFilterableQuery(
                    new TopDocs(
                        new TotalHits(5, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            new ScoreDoc(1, 0.9f),
                            new ScoreDoc(2, 0.8f),
                            new ScoreDoc(3, 0.7f),
                            new ScoreDoc(5, 0.6f),
                            new ScoreDoc(9, 0.5f) }
                    ),
                    100,
                    round2
                );

                int k = 3;
                AtomicLong capturedOps = new AtomicLong();
                PostFilterAwareKnnQuery query = new PostFilterAwareKnnQuery(round1, filterWeight, k, reader, capturedOps::set, null);

                Query result = query.rewrite(searcher);
                assertTrue("Expected KnnScoreDocQuery but got " + result.getClass(), result instanceof KnnScoreDocQuery);

                KnnScoreDocQuery knnResult = (KnnScoreDocQuery) result;
                assertEquals(3, knnResult.docs().length);
                // Vector ops accumulated: 100 + 80 = 180
                assertEquals(180L, capturedOps.get());
            }
        }
    }

    public void testRetryLoopStopsOnEmptyResults() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("tag", "pass", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);

                Weight filterWeight = searcher.createWeight(
                    searcher.rewrite(new TermQuery(new Term("tag", "pass"))),
                    ScoreMode.COMPLETE_NO_SCORES,
                    1f
                );

                // Round 1: empty results
                FakePostFilterableQuery round1 = new FakePostFilterableQuery(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]),
                    0,
                    null
                );

                PostFilterAwareKnnQuery query = new PostFilterAwareKnnQuery(round1, filterWeight, 5, reader, ops -> {}, null);
                Query result = query.rewrite(searcher);
                assertTrue("Expected MatchNoDocsQuery but got " + result.getClass(), result instanceof MatchNoDocsQuery);
            }
        }
    }

    public void testRetryLoopRespectsMaxRounds() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // All docs tagged "fail" — nothing passes the filter
            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                doc.add(new StringField("tag", "fail", Field.Store.NO));
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

                // Build a chain of MAX_ROUNDS fake queries, each returning results that won't pass the filter
                FakePostFilterableQuery[] rounds = new FakePostFilterableQuery[PostFilterAwareKnnQuery.MAX_ROUNDS];
                for (int i = PostFilterAwareKnnQuery.MAX_ROUNDS - 1; i >= 0; i--) {
                    FakePostFilterableQuery next = i < PostFilterAwareKnnQuery.MAX_ROUNDS - 1 ? rounds[i + 1] : null;
                    rounds[i] = new FakePostFilterableQuery(
                        new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(i, 0.5f) }),
                        10,
                        next
                    );
                }

                AtomicLong capturedOps = new AtomicLong();
                PostFilterAwareKnnQuery query = new PostFilterAwareKnnQuery(rounds[0], filterWeight, 5, reader, capturedOps::set, null);

                Query result = query.rewrite(searcher);
                // No docs pass the filter → should return no-docs
                assertTrue("Expected MatchNoDocsQuery but got " + result.getClass(), result instanceof MatchNoDocsQuery);
                // Vector ops: 10 * MAX_ROUNDS
                assertEquals(10L * PostFilterAwareKnnQuery.MAX_ROUNDS, capturedOps.get());
            }
        }
    }

    public void testRetryLoopWithParentDeduplication() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // 6 docs, all pass the filter
            for (int i = 0; i < 6; i++) {
                Document doc = new Document();
                doc.add(new StringField("tag", "pass", Field.Store.NO));
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

                // Parent docs at positions 2 and 5
                BitSetProducer parentsFilter = context -> {
                    FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
                    bits.set(2);
                    bits.set(5);
                    return bits;
                };

                // Round 1: 4 child docs from 2 parents → after dedup, only 2 unique parents
                FakePostFilterableQuery round1 = new FakePostFilterableQuery(
                    new TopDocs(
                        new TotalHits(4, TotalHits.Relation.EQUAL_TO),
                        new ScoreDoc[] {
                            new ScoreDoc(0, 0.9f),  // child → parent 2
                            new ScoreDoc(1, 0.8f),  // child → parent 2 (dup)
                            new ScoreDoc(3, 0.7f),  // child → parent 5
                            new ScoreDoc(4, 0.6f),  // child → parent 5 (dup)
                        }
                    ),
                    50,
                    null
                );

                int k = 2;
                PostFilterAwareKnnQuery query = new PostFilterAwareKnnQuery(round1, filterWeight, k, reader, ops -> {}, parentsFilter);

                Query result = query.rewrite(searcher);
                assertTrue(result instanceof KnnScoreDocQuery);

                KnnScoreDocQuery knnResult = (KnnScoreDocQuery) result;
                assertEquals(2, knnResult.docs().length);
            }
        }
    }

    // ==================== End-to-end IVF integration tests ====================

    public void testIVFPostFilterEndToEnd() throws IOException {
        // Create an IVF index with 200 vectors: 160 tagged "common", 40 tagged "rare"
        // selectivity = 160/200 = 0.8 > 0.7 threshold → post-filtering should activate
        // vectorsPerCluster minimum is 64, so we need enough docs
        KnnVectorsFormat format = new ES920DiskBBQVectorsFormat(128, 4);
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 200; i++) {
                    Document doc = new Document();
                    float[] vector = new float[] { (float) i, (float) (200 - i) };
                    doc.add(new KnnFloatVectorField("vec", vector, VectorSimilarityFunction.EUCLIDEAN));
                    doc.add(new StringField("tag", i < 160 ? "common" : "rare", Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Query filter = new TermQuery(new Term("tag", "common"));
                IVFKnnFloatVectorQuery query = new IVFKnnFloatVectorQuery("vec", new float[] { 0f, 200f }, 5, 10, filter, 0.5f, false);
                TopDocs topDocs = searcher.search(query, 5);
                assertTrue("Expected at least 1 result", topDocs.scoreDocs.length > 0);
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    String tag = reader.storedFields().document(sd.doc).get("tag");
                    assertEquals("All results should match filter", "common", tag);
                }
            }
        }
    }

    public void testIVFPreFilterWithLowSelectivity() throws IOException {
        // Create an IVF index where selectivity is low → pre-filtering, not post-filtering
        KnnVectorsFormat format = new ES920DiskBBQVectorsFormat(128, 4);
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 200; i++) {
                    Document doc = new Document();
                    float[] vector = new float[] { (float) i, (float) (200 - i) };
                    doc.add(new KnnFloatVectorField("vec", vector, VectorSimilarityFunction.EUCLIDEAN));
                    // Only 50 out of 200 match → selectivity = 0.25
                    doc.add(new StringField("tag", i < 50 ? "match" : "nomatch", Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Query filter = new TermQuery(new Term("tag", "match"));
                IVFKnnFloatVectorQuery query = new IVFKnnFloatVectorQuery("vec", new float[] { 0f, 200f }, 3, 5, filter, 0.5f, false);
                TopDocs topDocs = searcher.search(query, 3);
                assertTrue("Expected at least 1 result", topDocs.scoreDocs.length > 0);
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    String tag = reader.storedFields().document(sd.doc).get("tag");
                    assertEquals("All results should match filter", "match", tag);
                }
            }
        }
    }

    public void testHNSWPostFilterEndToEnd() throws IOException {
        // Create an HNSW index with 20 vectors: 16 tagged "common", 4 tagged "rare"
        // selectivity = 16/20 = 0.8 > 0.7 threshold → post-filtering should activate
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 20; i++) {
                    Document doc = new Document();
                    float[] vector = new float[] { (float) i, (float) (20 - i) };
                    doc.add(new KnnFloatVectorField("vec", vector, VectorSimilarityFunction.EUCLIDEAN));
                    doc.add(new StringField("tag", i < 16 ? "common" : "rare", Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Query filter = new TermQuery(new Term("tag", "common"));
                ESKnnFloatVectorQuery query = new ESKnnFloatVectorQuery(
                    "vec",
                    new float[] { 0f, 20f },
                    5,
                    10,
                    filter,
                    new KnnSearchStrategy.Hnsw(10)
                );
                TopDocs topDocs = searcher.search(query, 5);
                assertTrue("Expected at least 1 result", topDocs.scoreDocs.length > 0);
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    String tag = reader.storedFields().document(sd.doc).get("tag");
                    assertEquals("All results should match filter", "common", tag);
                }
            }
        }
    }

    // ==================== Helper: fake query for retry testing ====================

    static class FakePostFilterableQuery extends Query implements PostFilterableKnnQuery {
        private final TopDocs results;
        private final long opsCount;
        private final FakePostFilterableQuery nextRetry;

        FakePostFilterableQuery(TopDocs results, long opsCount, FakePostFilterableQuery nextRetry) {
            this.results = results;
            this.opsCount = opsCount;
            this.nextRetry = nextRetry;
        }

        @Override
        public Query rewrite(IndexSearcher searcher) {
            if (results == null || results.scoreDocs.length == 0) {
                return MatchNoDocsQuery.INSTANCE;
            }
            return new KnnScoreDocQuery(results.scoreDocs, searcher.getIndexReader());
        }

        @Override
        public ScoreDoc[] findCandidates(IndexSearcher searcher) throws IOException {
            Query rewritten = searcher.rewrite(this);
            if (rewritten instanceof KnnScoreDocQuery knnResult) {
                int[] docs = knnResult.docs();
                float[] scores = knnResult.scores();
                ScoreDoc[] scoreDocs = new ScoreDoc[docs.length];
                for (int i = 0; i < docs.length; i++) {
                    scoreDocs[i] = new ScoreDoc(docs[i], scores[i]);
                }
                return scoreDocs;
            }
            return new ScoreDoc[0];
        }

        @Override
        public PostFilterableKnnQuery createRetryQuery(IndexReader reader, ScoreDoc[] previousResults) {
            return nextRetry;
        }

        @Override
        public long vectorOpsCount() {
            return opsCount;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            throw new UnsupportedOperationException("FakePostFilterableQuery should be rewritten");
        }

        @Override
        public String toString(String field) {
            return "FakePostFilterableQuery";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object o) {
            return this == o;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }
}
