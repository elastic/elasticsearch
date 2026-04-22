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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PostFilterKnnQueryTests extends ESTestCase {

    public void testMergeResults() {
        {
            ScoreDoc[] result = PostFilterKnnQuery.mergeResults(new ScoreDoc[0], new ScoreDoc[0]);
            assertEquals(0, result.length);
        }
        {
            ScoreDoc[] input = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.8f) };
            ScoreDoc[] result = PostFilterKnnQuery.mergeResults(new ScoreDoc[0], input);
            assertEquals(2, result.length);
            assertEquals(1, result[0].doc);
            assertEquals(2, result[1].doc);
        }
        {
            ScoreDoc[] existing = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(3, 0.7f) };
            ScoreDoc[] incoming = new ScoreDoc[] { new ScoreDoc(2, 0.8f), new ScoreDoc(4, 0.6f) };
            ScoreDoc[] result = PostFilterKnnQuery.mergeResults(existing, incoming);
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
            ScoreDoc[] existing = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.7f) };
            ScoreDoc[] incoming = new ScoreDoc[] { new ScoreDoc(2, 0.8f), new ScoreDoc(3, 0.6f) };
            ScoreDoc[] result = PostFilterKnnQuery.mergeResults(existing, incoming);
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

                ScoreDoc[] result = PostFilterKnnQuery.applyFilter(candidates, filterWeight, searcher);
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
                ScoreDoc[] result = PostFilterKnnQuery.applyFilter(candidates, filterWeight, searcher);
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

                ScoreDoc[] result = PostFilterKnnQuery.deduplicateByParent(docs, reader, parentsFilter);
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
                ScoreDoc[] result = PostFilterKnnQuery.deduplicateByParent(docs, reader, parentsFilter);
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

                int[] excluded = new int[] { 0, 1, 2, };

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

    public void testCreateInnerQueryExcludesPreviouslySeenDocs() throws IOException {
        // Validate that ESKnnFloatVectorQuery.createInnerQuery() excludes doc IDs from prior rounds
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 10; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("vec", new float[] { (float) i, 0 }, VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                ESKnnFloatVectorQuery initial = new ESKnnFloatVectorQuery(
                    "vec",
                    new float[] { 5f, 0f },
                    5,
                    10,
                    null,
                    new KnnSearchStrategy.Hnsw(10)
                );

                // Simulate retry: exclude the 3 docs closest to query [5,0]
                int[] previouslySeenDocs = new int[] { 4, 5, 6 };
                Query retryQuery = initial.createInnerQuery(reader, previouslySeenDocs);
                TopDocs retryResults = searcher.search(retryQuery, 10);

                for (ScoreDoc sd : retryResults.scoreDocs) {
                    assertNotEquals("Doc 4 should be excluded", 4, sd.doc);
                    assertNotEquals("Doc 5 should be excluded", 5, sd.doc);
                    assertNotEquals("Doc 6 should be excluded", 6, sd.doc);
                }
                assertTrue("Should find results outside excluded set", retryResults.scoreDocs.length > 0);
            }
        }
    }

    public void testCreateInnerQuerySeedsDocs() throws IOException {
        // Validate that createInnerQuery passes docsToSeed to the new query,
        // which influences the SeededRetryCollectorManager starting points
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 20; i++) {
                    Document doc = new Document();
                    doc.add(
                        new KnnFloatVectorField("vec", new float[] { (float) i, (float) (20 - i) }, VectorSimilarityFunction.EUCLIDEAN)
                    );
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                ESKnnFloatVectorQuery initial = new ESKnnFloatVectorQuery(
                    "vec",
                    new float[] { 0f, 20f },
                    5,
                    10,
                    null,
                    new KnnSearchStrategy.Hnsw(10)
                );

                // First round: get initial results
                TopDocs round1 = searcher.search(initial, 5);
                assertTrue(round1.scoreDocs.length > 0);

                // Create retry query seeded with round1 doc IDs
                int[] round1DocIds = new int[round1.scoreDocs.length];
                for (int i = 0; i < round1.scoreDocs.length; i++) {
                    round1DocIds[i] = round1.scoreDocs[i].doc;
                }
                Query retryQuery = initial.createInnerQuery(reader, round1DocIds);
                TopDocs round2 = searcher.search(retryQuery, 10);

                // Round 2 should NOT contain any of round 1's docs (excluded by ExcludeDocsQuery)
                for (ScoreDoc sd : round2.scoreDocs) {
                    for (int excludedDoc : round1DocIds) {
                        assertNotEquals("Seeded retry should exclude round 1 doc " + excludedDoc, excludedDoc, sd.doc);
                    }
                }
            }
        }
    }

    public void testHNSWPostFilterTruncatesToK() throws IOException {
        // When post-filtering passes more results than k, verify only top-k are returned
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < 20; i++) {
                    Document doc = new Document();
                    float[] vector = new float[] { (float) i, (float) (20 - i) };
                    doc.add(new KnnFloatVectorField("vec", vector, VectorSimilarityFunction.EUCLIDEAN));
                    // All docs match filter → all pass post-filter, but only k returned
                    doc.add(new StringField("tag", "match", Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Query filter = new TermQuery(new Term("tag", "match"));
                int k = 3;
                ESKnnFloatVectorQuery query = new ESKnnFloatVectorQuery(
                    "vec",
                    new float[] { 0f, 20f },
                    k,
                    20,
                    filter,
                    new KnnSearchStrategy.Hnsw(10)
                );
                TopDocs topDocs = searcher.search(query, k);
                // selectivity = 1.0 > 0.7, all docs match, but only k should be returned
                assertEquals(k, topDocs.scoreDocs.length);
            }
        }
    }

    public void testHNSWNestedPostFilter() throws IOException {
        // Test post-filtering with nested (parent/child) document structure using
        // ESDiversifyingChildrenFloatKnnVectorQuery which deduplicates by parent.
        // Doc layout: [child, child, PARENT, child, child, PARENT, ...]
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                // Parent 0: children at 0,1 → parent at 2
                addChildDoc(writer, new float[] { 1, 0 }, "common");
                addChildDoc(writer, new float[] { 2, 0 }, "common");
                writer.addDocument(new Document()); // parent doc 2

                // Parent 1: children at 3,4 → parent at 5
                addChildDoc(writer, new float[] { 3, 0 }, "common");
                addChildDoc(writer, new float[] { 4, 0 }, "common");
                writer.addDocument(new Document()); // parent doc 5

                // Parent 2: children at 6,7 → parent at 8
                addChildDoc(writer, new float[] { 5, 0 }, "common");
                addChildDoc(writer, new float[] { 6, 0 }, "common");
                writer.addDocument(new Document()); // parent doc 8

                // Parent 3: children at 9,10 → parent at 11 (rare, nearest to query)
                addChildDoc(writer, new float[] { 10, 0 }, "rare");
                addChildDoc(writer, new float[] { 11, 0 }, "rare");
                writer.addDocument(new Document()); // parent doc 11

                writer.forceMerge(1);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                // 8 children total: 6 "common" + 2 "rare" → selectivity = 0.75 > 0.7
                Query filter = new TermQuery(new Term("tag", "common"));
                BitSetProducer parentsFilter = context -> {
                    FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
                    bits.set(2);
                    bits.set(5);
                    bits.set(8);
                    bits.set(11);
                    return bits;
                };
                // Query nearest to rare docs — post-filter must exclude them
                ESDiversifyingChildrenFloatKnnVectorQuery query = new ESDiversifyingChildrenFloatKnnVectorQuery(
                    "vec",
                    new float[] { 11f, 0f },
                    filter,
                    2,
                    10,
                    parentsFilter,
                    new KnnSearchStrategy.Hnsw(10)
                );
                TopDocs topDocs = searcher.search(query, 2);
                assertTrue("Expected at least 1 result", topDocs.scoreDocs.length > 0);
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    String tag = reader.storedFields().document(sd.doc).get("tag");
                    assertEquals("Nested post-filter must only return matching children", "common", tag);
                }
            }
        }
    }

    private static void addChildDoc(IndexWriter writer, float[] vector, String tag) throws IOException {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("vec", vector, VectorSimilarityFunction.EUCLIDEAN));
        doc.add(new StringField("tag", tag, Field.Store.YES));
        writer.addDocument(doc);
    }

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
}
