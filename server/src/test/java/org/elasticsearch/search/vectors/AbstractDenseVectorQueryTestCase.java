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
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

abstract class AbstractDenseVectorQueryTestCase extends ESTestCase {

    abstract DenseVectorQuery getDenseVectorQuery(String field, float[] query);

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

    protected BaseDirectoryWrapper newDirectoryForTest() {
        return LuceneTestCase.newDirectory(random());
    }

}
