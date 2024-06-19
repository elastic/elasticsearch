/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

abstract class AbstractExactKnnQueryTestCase extends LuceneTestCase {

    abstract ExactKnnQuery getExactVectorQuery(String field, float[] query);

    abstract float[] randomVector(int dim);

    abstract Field getKnnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction);

    public void testEquals() {
        ExactKnnQuery q1 = getExactVectorQuery("f1", new float[] { 0, 1 });
        ExactKnnQuery q2 = getExactVectorQuery("f1", new float[] { 0, 1 });

        assertEquals(q2, q1);

        assertNotEquals(null, q1);
        assertNotEquals(q1, new TermQuery(new Term("f1", "x")));

        assertNotEquals(q1, getExactVectorQuery("f2", new float[] { 0, 1 }));
        assertNotEquals(q1, getExactVectorQuery("f1", new float[] { 1, 1 }));
    }

    public void testEmptyIndex() throws IOException {
        try (Directory indexStore = getIndexStore("field"); IndexReader reader = DirectoryReader.open(indexStore)) {
            IndexSearcher searcher = newSearcher(reader);
            ExactKnnQuery kvq = getExactVectorQuery("field", new float[] { 1, 2 });
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
            ExactKnnQuery kvq = getExactVectorQuery("field", new float[] { 0 });
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
            assertMatches(searcher, getExactVectorQuery("xyzzy", new float[] { 0 }), 0);
            assertMatches(searcher, getExactVectorQuery("id", new float[] { 0 }), 0);
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
            ExactKnnQuery query = getExactVectorQuery("field", new float[] { 2, 3 });
            Query rewritten = query.rewrite(searcher);
            Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
            Scorer scorer = weight.scorer(reader.leaves().get(0));

            // prior to advancing, score is 0
            assertEquals(-1, scorer.docID());
            expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);

            // This is 1 / ((l2distance((2,3), (2, 2)) = 1) + 1) = 0.5
            assertEquals(1 / 2f, scorer.getMaxScore(2), 0);
            assertEquals(1 / 2f, scorer.getMaxScore(Integer.MAX_VALUE), 0);

            DocIdSetIterator it = scorer.iterator();
            assertEquals(3, it.cost());
            int firstDoc = it.nextDoc();
            if (firstDoc == 1) {
                assertEquals(1 / 6f, scorer.score(), 0);
                assertEquals(3, it.advance(3));
                assertEquals(1 / 2f, scorer.score(), 0);
                assertEquals(NO_MORE_DOCS, it.advance(4));
            } else {
                assertEquals(2, firstDoc);
                assertEquals(1 / 2f, scorer.score(), 0);
                assertEquals(4, it.advance(4));
                assertEquals(1 / 6f, scorer.score(), 0);
                assertEquals(NO_MORE_DOCS, it.advance(5));
            }
            expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);
        }
    }

    public void testScoreCosine() throws IOException {
        float[][] vectors = new float[5][];
        for (int j = 0; j < 5; j++) {
            vectors[j] = new float[] { j, j * j };
        }
        try (Directory d = getStableIndexStore("field", COSINE, vectors)) {
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                ExactKnnQuery query = getExactVectorQuery("field", new float[] { 2, 3 });
                Query rewritten = query.rewrite(searcher);
                Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
                Scorer scorer = weight.scorer(reader.leaves().get(0));

                // prior to advancing, score is undefined
                assertEquals(-1, scorer.docID());
                expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);

                /* score0 = ((2,3) * (1, 1) = 5) / (||2, 3|| * ||1, 1|| = sqrt(26)), then
                 * normalized by (1 + x) /2.
                 */
                float score0 = (float) ((1 + (2 * 1 + 3 * 1) / Math.sqrt((2 * 2 + 3 * 3) * (1 * 1 + 1 * 1))) / 2);

                /* score1 = ((2,3) * (2, 4) = 16) / (||2, 3|| * ||2, 4|| = sqrt(260)), then
                 * normalized by (1 + x) /2
                 */
                float score1 = (float) ((1 + (2 * 2 + 3 * 4) / Math.sqrt((2 * 2 + 3 * 3) * (2 * 2 + 4 * 4))) / 2);

                // doc 1 happens to have the maximum score
                assertEquals(score1, scorer.getMaxScore(2), 0.0001);
                assertEquals(score1, scorer.getMaxScore(Integer.MAX_VALUE), 0.0001);

                DocIdSetIterator it = scorer.iterator();
                assertEquals(3, it.cost());
                assertEquals(0, it.nextDoc());
                // doc 0 has (1, 1)
                assertEquals(score0, scorer.score(), 0.0001);
                assertEquals(1, it.advance(1));
                assertEquals(score1, scorer.score(), 0.0001);

                // since topK was 3
                assertEquals(NO_MORE_DOCS, it.advance(4));
                expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);
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
            ExactKnnQuery kvq = getExactVectorQuery("field", new float[] { 0, -1 });
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
                ExactKnnQuery query = getExactVectorQuery("field", new float[] { 2, 3 });
                Explanation matched = searcher.explain(query, 2);
                assertTrue(matched.isMatch());
                assertEquals(1 / 2f, matched.getValue());
                assertEquals(0, matched.getDetails().length);

                Explanation nomatch = searcher.explain(query, 6);
                assertFalse(nomatch.isMatch());

                nomatch = searcher.explain(getExactVectorQuery("someMissingField", new float[] { 2, 3 }), 6);
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
                    int k = random().nextInt(80) + 1;
                    ExactKnnQuery query = getExactVectorQuery("field", randomVector(dimension));
                    int n = random().nextInt(100) + 1;
                    TopDocs results = searcher.search(query, n);
                    int expected = Math.min(n, reader.numDocs());
                    // we may get fewer results than requested if there are deletions, but this test doesn't
                    // test that
                    assert reader.hasDeletions() == false;
                    assertEquals(expected, results.scoreDocs.length);
                    assertTrue(results.totalHits.value >= results.scoreDocs.length);
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

    protected BaseDirectoryWrapper newDirectoryForTest() {
        return LuceneTestCase.newDirectory(random());
    }

}
