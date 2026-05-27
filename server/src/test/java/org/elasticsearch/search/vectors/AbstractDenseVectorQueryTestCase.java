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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Bits;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

abstract class AbstractDenseVectorQueryTestCase extends ESTestCase {

    abstract DenseVectorQuery getDenseVectorQuery(String field, float[] query);

    abstract DenseVectorQuery getDenseVectorQuery(String field, float[] query, Query filter);

    abstract float[] randomVector(int dim);

    abstract Field getKnnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction);

    public void testEquals() {
        DenseVectorQuery q1 = getDenseVectorQuery("f1", new float[] { 0, 1 });
        DenseVectorQuery q2 = getDenseVectorQuery("f1", new float[] { 0, 1 });

        assertEquals(q2, q1);

        assertNotEquals(null, q1);
        assertNotEquals(q1, new TermQuery(new Term("f1", "x")));

        assertNotEquals(q1, getDenseVectorQuery("f2", new float[] { 0, 1 }));
        assertNotEquals(q1, getDenseVectorQuery("f1", new float[] { 1, 1 }));
    }

    public void testEmptyIndex() throws IOException {
        try (Directory indexStore = getIndexStore("field"); IndexReader reader = DirectoryReader.open(indexStore)) {
            IndexSearcher searcher = newSearcher(reader);
            DenseVectorQuery kvq = getDenseVectorQuery("field", new float[] { 1, 2 });
            assertMatches(searcher, kvq, 0);
        }
    }

    /** testDimensionMismatch */
    public void testDimensionMismatch() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            DenseVectorQuery kvq = getDenseVectorQuery("field", new float[] { 0 });
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> searcher.search(kvq, 10));
            assertEquals("vector query dimension: 1 differs from field dimension: 2", e.getMessage());
        }
    }

    /** testNonVectorField */
    public void testNonVectorField() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            assertMatches(searcher, getDenseVectorQuery("xyzzy", new float[] { 0 }), 0);
            assertMatches(searcher, getDenseVectorQuery("id", new float[] { 0 }), 0);
        }
    }

    public void testScoreEuclidean() throws IOException {
        float[][] vectors = new float[5][];
        for (int j = 0; j < 5; j++) {
            vectors[j] = new float[] { j, j };
        }
        try (
            Directory d = getStableIndexStore("field", VectorSimilarityFunction.EUCLIDEAN, vectors);
            IndexReader reader = DirectoryReader.open(d)
        ) {
            IndexSearcher searcher = new IndexSearcher(reader);
            float[] queryVector = new float[] { 2, 3 };
            DenseVectorQuery query = getDenseVectorQuery("field", queryVector);
            Query rewritten = query.rewrite(searcher);
            Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
            Scorer scorer = weight.scorer(reader.leaves().get(0));

            // prior to advancing, score is 0
            assertEquals(-1, scorer.docID());

            DocIdSetIterator it = scorer.iterator();
            assertEquals(5, it.cost());
            it.nextDoc();
            int curDoc = 0;
            // iterate the docs and assert the scores are what we expect
            while (it.docID() != NO_MORE_DOCS) {
                assertEquals(VectorSimilarityFunction.EUCLIDEAN.compare(vectors[curDoc], queryVector), scorer.score(), 0.0001);
                curDoc++;
                it.nextDoc();
            }
        }
    }

    public void testScoreCosine() throws IOException {
        float[][] vectors = new float[5][];
        for (int j = 1; j <= 5; j++) {
            vectors[j - 1] = new float[] { j, j * j };
        }
        try (Directory d = getStableIndexStore("field", COSINE, vectors)) {
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                float[] queryVector = new float[] { 2, 3 };
                DenseVectorQuery query = getDenseVectorQuery("field", queryVector);
                Query rewritten = query.rewrite(searcher);
                Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
                Scorer scorer = weight.scorer(reader.leaves().get(0));

                // prior to advancing, score is undefined
                assertEquals(-1, scorer.docID());
                DocIdSetIterator it = scorer.iterator();
                assertEquals(5, it.cost());
                it.nextDoc();
                int curDoc = 0;
                // iterate the docs and assert the scores are what we expect
                while (it.docID() != NO_MORE_DOCS) {
                    assertEquals(COSINE.compare(vectors[curDoc], queryVector), scorer.score(), 0.0001);
                    curDoc++;
                    it.nextDoc();
                }
            }
        }
    }

    public void testScoreMIP() throws IOException {
        try (
            Directory indexStore = getIndexStore(
                "field",
                VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
                new float[] { 0, 1 },
                new float[] { 1, 2 },
                new float[] { 0, 0 }
            );
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            DenseVectorQuery kvq = getDenseVectorQuery("field", new float[] { 0, -1 });
            assertMatches(searcher, kvq, 3);
            ScoreDoc[] scoreDocs = searcher.search(kvq, 3).scoreDocs;
            assertIdMatches(reader, "id2", scoreDocs[0]);
            assertIdMatches(reader, "id0", scoreDocs[1]);
            assertIdMatches(reader, "id1", scoreDocs[2]);

            assertEquals(1.0, scoreDocs[0].score, 1e-7);
            assertEquals(1 / 2f, scoreDocs[1].score, 1e-7);
            assertEquals(1 / 3f, scoreDocs[2].score, 1e-7);
        }
    }

    public void testExplain() throws IOException {
        float[][] vectors = new float[5][];
        for (int j = 0; j < 5; j++) {
            vectors[j] = new float[] { j, j };
        }
        try (Directory d = getStableIndexStore("field", VectorSimilarityFunction.EUCLIDEAN, vectors)) {
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                DenseVectorQuery query = getDenseVectorQuery("field", new float[] { 2, 3 });
                Explanation matched = searcher.explain(query, 2);
                assertTrue(matched.isMatch());
                assertEquals(1 / 2f, matched.getValue());
                assertEquals(0, matched.getDetails().length);

                Explanation nomatch = searcher.explain(query, 6);
                assertFalse(nomatch.isMatch());

                nomatch = searcher.explain(getDenseVectorQuery("someMissingField", new float[] { 2, 3 }), 6);
                assertFalse(nomatch.isMatch());
            }
        }
    }

    public void testRandom() throws IOException {
        int numDocs = atLeast(100);
        int dimension = atLeast(5);
        int numIters = atLeast(10);
        boolean everyDocHasAVector = random().nextBoolean();
        try (Directory d = newDirectoryForTest()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), d);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                if (everyDocHasAVector || random().nextInt(10) != 2) {
                    doc.add(getKnnVectorField("field", randomVector(dimension), VectorSimilarityFunction.EUCLIDEAN));
                }
                w.addDocument(doc);
            }
            w.close();
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = newSearcher(reader);
                for (int i = 0; i < numIters; i++) {
                    DenseVectorQuery query = getDenseVectorQuery("field", randomVector(dimension));
                    int n = random().nextInt(100) + 1;
                    TopDocs results = searcher.search(query, n);
                    assert reader.hasDeletions() == false;
                    assertTrue(results.totalHits.value() >= results.scoreDocs.length);
                    // verify the results are in descending score order
                    float last = Float.MAX_VALUE;
                    for (ScoreDoc scoreDoc : results.scoreDocs) {
                        assertTrue(scoreDoc.score <= last);
                        last = scoreDoc.score;
                    }
                }
            }
        }
    }

    void assertIdMatches(IndexReader reader, String expectedId, ScoreDoc scoreDoc) throws IOException {
        String actualId = reader.storedFields().document(scoreDoc.doc).get("id");
        assertEquals(expectedId, actualId);
    }

    private void assertMatches(IndexSearcher searcher, Query q, int expectedMatches) throws IOException {
        ScoreDoc[] result = searcher.search(q, 1000).scoreDocs;
        assertEquals(expectedMatches, result.length);
    }

    Directory getIndexStore(String field, float[]... contents) throws IOException {
        return getIndexStore(field, VectorSimilarityFunction.EUCLIDEAN, contents);
    }

    private Directory getStableIndexStore(String field, VectorSimilarityFunction vectorSimilarityFunction, float[]... contents)
        throws IOException {
        Directory indexStore = newDirectoryForTest();
        try (IndexWriter writer = new IndexWriter(indexStore, new IndexWriterConfig())) {
            for (int i = 0; i < contents.length; ++i) {
                Document doc = new Document();
                doc.add(getKnnVectorField(field, contents[i], vectorSimilarityFunction));
                doc.add(new StringField("id", "id" + i, Field.Store.YES));
                writer.addDocument(doc);
            }
            // Add some documents without a vector
            for (int i = 0; i < 5; i++) {
                Document doc = new Document();
                doc.add(new StringField("other", "value", Field.Store.NO));
                writer.addDocument(doc);
            }
        }
        return indexStore;
    }

    Directory getIndexStore(String field, VectorSimilarityFunction vectorSimilarityFunction, float[]... contents) throws IOException {
        Directory indexStore = newDirectoryForTest();
        RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
        for (int i = 0; i < contents.length; ++i) {
            Document doc = new Document();
            doc.add(getKnnVectorField(field, contents[i], vectorSimilarityFunction));
            doc.add(new StringField("id", "id" + i, Field.Store.YES));
            writer.addDocument(doc);
            if (randomBoolean()) {
                // Add some documents without a vector
                for (int j = 0; j < randomIntBetween(1, 5); j++) {
                    doc = new Document();
                    doc.add(new StringField("other", "value", Field.Store.NO));
                    // Add fields that will be matched by our test filters but won't have vectors
                    doc.add(new StringField("id", "id" + j, Field.Store.YES));
                    writer.addDocument(doc);
                }
            }
        }
        // Add some documents without a vector
        for (int i = 0; i < 5; i++) {
            Document doc = new Document();
            doc.add(new StringField("other", "value", Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.close();
        return indexStore;
    }

    /**
     * Verifies that a {@link DenseVectorQuery} combined with another scoring query in a {@link BooleanQuery}
     * using two {@link BooleanClause.Occur#MUST} clauses returns the correct documents with positive scores.
     * Uses a large document set (100+) to exercise Lucene's block-max conjunction scoring paths.
     */
    public void testBooleanMustCombination() throws IOException {
        int numDocs = atLeast(200);
        int numGoodDocs = 0;
        try (Directory d = newDirectoryForTest()) {
            try (IndexWriter writer = new IndexWriter(d, new IndexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { i % 10, (i + 1) % 10 }, VectorSimilarityFunction.EUCLIDEAN));
                    doc.add(new StringField("id", "id" + i, Field.Store.YES));
                    boolean good = i % 2 == 0;
                    doc.add(new StringField("tag", good ? "good" : "other", Field.Store.NO));
                    if (good) numGoodDocs++;
                    writer.addDocument(doc);
                }
                // Add docs without vectors to verify the vector iterator correctly skips them
                for (String tag : new String[] { "good", "other" }) {
                    Document doc = new Document();
                    doc.add(new StringField("tag", tag, Field.Store.NO));
                    writer.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                float[] queryVector = new float[] { 1, 0 };
                Query termQuery = new TermQuery(new Term("tag", "good"));

                // without prefilter on the DenseVectorQuery
                BooleanQuery boolQuery = new BooleanQuery.Builder().add(getDenseVectorQuery("field", queryVector), BooleanClause.Occur.MUST)
                    .add(termQuery, BooleanClause.Occur.MUST)
                    .build();
                assertBooleanMustResults(searcher, boolQuery, numGoodDocs);

                // with prefilter matching the same documents
                BooleanQuery boolQueryWithFilter = new BooleanQuery.Builder().add(
                    getDenseVectorQuery("field", queryVector, termQuery),
                    BooleanClause.Occur.MUST
                ).add(termQuery, BooleanClause.Occur.MUST).build();
                assertBooleanMustResults(searcher, boolQueryWithFilter, numGoodDocs);
            }
        }
    }

    /**
     * Directly exercises {@link DenseVectorQuery.DenseVectorScorer#nextDocsAndScores},
     * the batch path underlying {@link DenseVectorQuery.DenseVectorBulkScorer}.
     * Uses a large document set (100+) with interspersed non-vector docs across multiple
     * segments. Verifies that every (doc, score) pair returned by the batch path matches
     * the per-document {@link Scorer#score()} result for both the unfiltered and filtered
     * query variants, confirming that boost is applied correctly throughout.
     */
    public void testNextDocsAndScores() throws IOException {
        int numDocs = atLeast(200);
        int dimension = 4;
        float boost = 1.5f;

        try (Directory d = newDirectoryForTest()) {
            RandomIndexWriter writer = new RandomIndexWriter(random(), d);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(getKnnVectorField("field", randomVector(dimension), VectorSimilarityFunction.EUCLIDEAN));
                if (i % 3 == 0) {
                    doc.add(new StringField("tag", "filtered", Field.Store.NO));
                }
                writer.addDocument(doc);
                if (randomBoolean()) {
                    // Intersperse docs without vectors to exercise iterator skipping
                    Document noVec = new Document();
                    noVec.add(new StringField("tag", "filtered", Field.Store.NO));
                    writer.addDocument(noVec);
                }
            }
            writer.close();

            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                float[] queryVector = randomVector(dimension);

                // Without filter
                assertNextDocsAndScoresMatchPerDocScores(searcher, reader, getDenseVectorQuery("field", queryVector), boost);

                // With filter — only docs that also have tag="filtered" should be scored
                Query filter = new TermQuery(new Term("tag", "filtered"));
                assertNextDocsAndScoresMatchPerDocScores(searcher, reader, getDenseVectorQuery("field", queryVector, filter), boost);
            }
        }
    }

    /**
     * Verifies that {@link DenseVectorQuery.DenseVectorScorer#nextDocsAndScores} only emits
     * documents with {@code docID < upTo}, and that a subsequent call with a larger upper
     * bound correctly resumes and delivers the remaining documents.
     */
    public void testNextDocsAndScoresUpToBoundary() throws IOException {
        int numDocs = 10;
        try (Directory d = newDirectoryForTest()) {
            try (IndexWriter writer = new IndexWriter(d, new IndexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { i, i + 1 }, VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                LeafReaderContext ctx = reader.leaves().get(0);
                IndexSearcher searcher = new IndexSearcher(reader);
                Query rewritten = searcher.rewrite(getDenseVectorQuery("field", new float[] { 0, 0 }));
                Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1f);
                Scorer scorer = weight.scorer(ctx);
                assertNotNull(scorer);

                int upTo = 5;
                DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
                int[] firstBatch = new int[numDocs];
                int firstCount = 0;
                for (scorer.nextDocsAndScores(upTo, null, buffer); buffer.size > 0; scorer.nextDocsAndScores(upTo, null, buffer)) {
                    for (int i = 0; i < buffer.size; i++) {
                        firstBatch[firstCount++] = buffer.docs[i];
                    }
                }
                assertEquals(upTo, firstCount);
                for (int i = 0; i < firstCount; i++) {
                    assertTrue("doc " + firstBatch[i] + " must be < " + upTo, firstBatch[i] < upTo);
                }

                int[] secondBatch = new int[numDocs];
                int secondCount = 0;
                for (scorer.nextDocsAndScores(ctx.reader().maxDoc(), null, buffer); buffer.size > 0; scorer.nextDocsAndScores(
                    ctx.reader().maxDoc(),
                    null,
                    buffer
                )) {
                    for (int i = 0; i < buffer.size; i++) {
                        secondBatch[secondCount++] = buffer.docs[i];
                    }
                }
                assertEquals(numDocs - upTo, secondCount);
                for (int i = 0; i < secondCount; i++) {
                    assertTrue("doc " + secondBatch[i] + " must be >= " + upTo, secondBatch[i] >= upTo);
                }
            }
        }
    }

    /**
     * Verifies that {@link DenseVectorQuery.DenseVectorScorer#nextDocsAndScores} honors
     * {@code liveDocs} {@link Bits} mask, skipping any document for which
     * {@code liveDocs.get(docID)} returns {@code false}.
     */
    public void testNextDocsAndScoresLiveDocs() throws IOException {
        int numDocs = 10;
        try (Directory d = newDirectoryForTest()) {
            try (IndexWriter writer = new IndexWriter(d, new IndexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { i, i + 1 }, VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                LeafReaderContext ctx = reader.leaves().get(0);
                IndexSearcher searcher = new IndexSearcher(reader);
                Query rewritten = searcher.rewrite(getDenseVectorQuery("field", new float[] { 0, 0 }));
                Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1f);
                Scorer scorer = weight.scorer(ctx);
                assertNotNull(scorer);

                // Mark even-numbered docs as deleted; only odd doc IDs should be returned
                Bits liveDocs = new Bits() {
                    @Override
                    public boolean get(int index) {
                        return index % 2 != 0;
                    }

                    @Override
                    public int length() {
                        return numDocs;
                    }
                };

                DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
                int[] collected = new int[numDocs];
                int count = 0;
                for (scorer.nextDocsAndScores(ctx.reader().maxDoc(), liveDocs, buffer); buffer.size > 0; scorer.nextDocsAndScores(
                    ctx.reader().maxDoc(),
                    liveDocs,
                    buffer
                )) {
                    for (int i = 0; i < buffer.size; i++) {
                        collected[count++] = buffer.docs[i];
                    }
                }

                assertEquals(numDocs / 2, count);
                for (int i = 0; i < count; i++) {
                    assertEquals("doc " + collected[i] + " should be odd (live)", 1, collected[i] % 2);
                }
            }
        }
    }

    /**
     * Verifies that scores returned by {@link DenseVectorQuery.DenseVectorScorer#nextDocsAndScores}
     * are exactly {@code boost × rawScore}: running with boost=2 should yield scores that are
     * precisely double those from boost=1 for every document.
     */
    public void testNextDocsAndScoresBoostScaling() throws IOException {
        int numDocs = 5;
        float boost = 2f;
        try (Directory d = newDirectoryForTest()) {
            try (IndexWriter writer = new IndexWriter(d, new IndexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { i + 1, i + 1 }, VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                LeafReaderContext ctx = reader.leaves().get(0);
                IndexSearcher searcher = new IndexSearcher(reader);
                Query rewritten = searcher.rewrite(getDenseVectorQuery("field", new float[] { 0, 0 }));
                DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();

                float[] baseScores = new float[ctx.reader().maxDoc()];
                int baseCount = 0;
                Scorer baseScorer = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1f).scorer(ctx);
                assertNotNull(baseScorer);
                for (baseScorer.nextDocsAndScores(ctx.reader().maxDoc(), null, buffer); buffer.size > 0; baseScorer.nextDocsAndScores(
                    ctx.reader().maxDoc(),
                    null,
                    buffer
                )) {
                    for (int i = 0; i < buffer.size; i++) {
                        baseScores[buffer.docs[i]] = buffer.features[i];
                        baseCount++;
                    }
                }
                assertEquals(numDocs, baseCount);

                float[] boostedScores = new float[ctx.reader().maxDoc()];
                int boostedCount = 0;
                Scorer boostedScorer = searcher.createWeight(rewritten, ScoreMode.COMPLETE, boost).scorer(ctx);
                assertNotNull(boostedScorer);
                for (boostedScorer.nextDocsAndScores(ctx.reader().maxDoc(), null, buffer); buffer.size > 0; boostedScorer.nextDocsAndScores(
                    ctx.reader().maxDoc(),
                    null,
                    buffer
                )) {
                    for (int i = 0; i < buffer.size; i++) {
                        boostedScores[buffer.docs[i]] = buffer.features[i];
                        boostedCount++;
                    }
                }
                assertEquals(numDocs, boostedCount);

                for (int docId = 0; docId < numDocs; docId++) {
                    assertEquals("score for doc " + docId + " must scale by boost", baseScores[docId] * boost, boostedScores[docId], 1e-5f);
                }
            }
        }
    }

    /**
     * Verifies that {@link DenseVectorQuery.DenseVectorBulkScorer#score} collects only
     * documents in the [{@code min}, {@code max}) window, and that two consecutive calls
     * with adjacent non-overlapping windows together cover all documents exactly once.
     */
    public void testBulkScorerWindow() throws IOException {
        int numDocs = 10;
        try (Directory d = newDirectoryForTest()) {
            try (IndexWriter writer = new IndexWriter(d, new IndexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { i, i + 1 }, VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                LeafReaderContext ctx = reader.leaves().get(0);
                IndexSearcher searcher = new IndexSearcher(reader);
                Query rewritten = searcher.rewrite(getDenseVectorQuery("field", new float[] { 0, 0 }));
                ScorerSupplier supplier = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1f).scorerSupplier(ctx);
                assertNotNull(supplier);
                BulkScorer bulkScorer = supplier.bulkScorer();
                assertNotNull(bulkScorer);

                int mid = 5;
                int[] firstDocs = new int[numDocs];
                int[] firstCount = { 0 };
                bulkScorer.score(new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) {
                        firstDocs[firstCount[0]++] = doc;
                    }
                }, null, 0, mid);
                assertEquals(mid, firstCount[0]);
                for (int i = 0; i < firstCount[0]; i++) {
                    assertTrue("doc " + firstDocs[i] + " must be < " + mid, firstDocs[i] < mid);
                }

                int[] secondDocs = new int[numDocs];
                int[] secondCount = { 0 };
                bulkScorer.score(new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) {
                        secondDocs[secondCount[0]++] = doc;
                    }
                }, null, mid, ctx.reader().maxDoc());
                assertEquals(numDocs - mid, secondCount[0]);
                for (int i = 0; i < secondCount[0]; i++) {
                    assertTrue("doc " + secondDocs[i] + " must be >= " + mid, secondDocs[i] >= mid);
                }
            }
        }
    }

    /**
     * Verifies that {@link DenseVectorQuery.DenseVectorBulkScorer#score} honors the
     * {@code acceptDocs} {@link Bits} mask, skipping documents for which
     * {@code acceptDocs.get(docID)} returns {@code false}.
     */
    public void testBulkScorerAcceptDocs() throws IOException {
        int numDocs = 10;
        try (Directory d = newDirectoryForTest()) {
            try (IndexWriter writer = new IndexWriter(d, new IndexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { i, i + 1 }, VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                LeafReaderContext ctx = reader.leaves().get(0);
                IndexSearcher searcher = new IndexSearcher(reader);
                Query rewritten = searcher.rewrite(getDenseVectorQuery("field", new float[] { 0, 0 }));
                ScorerSupplier supplier = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1f).scorerSupplier(ctx);
                assertNotNull(supplier);
                BulkScorer bulkScorer = supplier.bulkScorer();
                assertNotNull(bulkScorer);

                // Mark even-numbered docs as deleted; only odd doc IDs should be collected
                Bits acceptDocs = new Bits() {
                    @Override
                    public boolean get(int index) {
                        return index % 2 != 0;
                    }

                    @Override
                    public int length() {
                        return numDocs;
                    }
                };

                int[] collected = new int[numDocs];
                int[] count = { 0 };
                bulkScorer.score(new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int doc) {
                        collected[count[0]++] = doc;
                    }
                }, acceptDocs, 0, ctx.reader().maxDoc());

                assertEquals(numDocs / 2, count[0]);
                for (int i = 0; i < count[0]; i++) {
                    assertEquals("doc " + collected[i] + " should be odd (live)", 1, collected[i] % 2);
                }
            }
        }
    }

    /**
     * Verifies that scores delivered to the {@link LeafCollector} by
     * {@link DenseVectorQuery.DenseVectorBulkScorer#score} are exactly
     * {@code boost × rawScore}: running at boost=2 should yield scores precisely double
     * those at boost=1 for every document.
     */
    public void testBulkScorerBoostScaling() throws IOException {
        int numDocs = 5;
        float boost = 2f;
        try (Directory d = newDirectoryForTest()) {
            try (IndexWriter writer = new IndexWriter(d, new IndexWriterConfig())) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { i + 1, i + 1 }, VectorSimilarityFunction.EUCLIDEAN));
                    writer.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                LeafReaderContext ctx = reader.leaves().get(0);
                IndexSearcher searcher = new IndexSearcher(reader);
                Query rewritten = searcher.rewrite(getDenseVectorQuery("field", new float[] { 0, 0 }));

                float[] baseScores = new float[ctx.reader().maxDoc()];
                int[] baseCount = { 0 };
                BulkScorer base = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1f).scorerSupplier(ctx).bulkScorer();
                assertNotNull(base);
                base.score(new LeafCollector() {
                    Scorable sc;

                    @Override
                    public void setScorer(Scorable scorer) {
                        sc = scorer;
                    }

                    @Override
                    public void collect(int doc) throws IOException {
                        baseScores[doc] = sc.score();
                        baseCount[0]++;
                    }
                }, null, 0, ctx.reader().maxDoc());
                assertEquals(numDocs, baseCount[0]);

                float[] boostedScores = new float[ctx.reader().maxDoc()];
                int[] boostedCount = { 0 };
                BulkScorer boosted = searcher.createWeight(rewritten, ScoreMode.COMPLETE, boost).scorerSupplier(ctx).bulkScorer();
                assertNotNull(boosted);
                boosted.score(new LeafCollector() {
                    Scorable sc;

                    @Override
                    public void setScorer(Scorable scorer) {
                        sc = scorer;
                    }

                    @Override
                    public void collect(int doc) throws IOException {
                        boostedScores[doc] = sc.score();
                        boostedCount[0]++;
                    }
                }, null, 0, ctx.reader().maxDoc());
                assertEquals(numDocs, boostedCount[0]);

                for (int docId = 0; docId < numDocs; docId++) {
                    assertEquals("score for doc " + docId + " must scale by boost", baseScores[docId] * boost, boostedScores[docId], 1e-5f);
                }
            }
        }
    }

    private void assertNextDocsAndScoresMatchPerDocScores(IndexSearcher searcher, IndexReader reader, DenseVectorQuery query, float boost)
        throws IOException {
        Query rewritten = searcher.rewrite(query);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, boost);

        for (LeafReaderContext ctx : reader.leaves()) {
            Scorer batchScorer = weight.scorer(ctx);
            if (batchScorer == null) continue;

            // Collect all (doc, score) pairs via nextDocsAndScores
            DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
            float[] scoreByDocId = new float[ctx.reader().maxDoc()];
            int batchCount = 0;
            for (batchScorer.nextDocsAndScores(ctx.reader().maxDoc(), null, buffer); buffer.size > 0; batchScorer.nextDocsAndScores(
                ctx.reader().maxDoc(),
                null,
                buffer
            )) {
                for (int i = 0; i < buffer.size; i++) {
                    scoreByDocId[buffer.docs[i]] = buffer.features[i];
                    batchCount++;
                }
            }

            // Verify each score against per-doc score(), confirming boost is applied
            Scorer perDocScorer = weight.scorer(ctx);
            DocIdSetIterator it = perDocScorer.iterator();
            int perDocCount = 0;
            while (it.nextDoc() != NO_MORE_DOCS) {
                assertEquals(
                    "nextDocsAndScores score must match score() for doc " + it.docID(),
                    perDocScorer.score(),
                    scoreByDocId[it.docID()],
                    1e-5f
                );
                perDocCount++;
            }
            assertEquals("nextDocsAndScores must visit the same number of docs as iterator", perDocCount, batchCount);
        }
    }

    private void assertBooleanMustResults(IndexSearcher searcher, BooleanQuery query, int expectedCount) throws IOException {
        ScoreDoc[] results = searcher.search(query, expectedCount + 1).scoreDocs;
        assertEquals("expected docs with vector and 'good' tag", expectedCount, results.length);
        float lastScore = Float.MAX_VALUE;
        for (ScoreDoc sd : results) {
            assertTrue("score must be positive", sd.score > 0f);
            assertTrue("results must be in descending score order", sd.score <= lastScore);
            lastScore = sd.score;
        }
    }

    protected BaseDirectoryWrapper newDirectoryForTest() {
        return LuceneTestCase.newDirectory(random());
    }

}
