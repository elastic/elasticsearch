/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.carrotsearch.randomizedtesting.RandomizedTest.frequently;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class VectorRadiusQueryTests extends LuceneTestCase {

    public void testEquals() {
        VectorRadiusQuery q1 = new VectorRadiusQuery("f1", new float[] { 0, 1 }, 0.25f, 10);

        assertEquals(q1, new VectorRadiusQuery("f1", new float[] { 0, 1 }, 0.25f, 10));
        assertNotEquals(null, q1);
        assertNotEquals(q1, new TermQuery(new Term("f1", "x")));

        assertNotEquals(q1, new VectorRadiusQuery("f2", new float[] { 0, 1 }, 0.25f, 10));
        assertNotEquals(q1, new VectorRadiusQuery("f1", new float[] { 1, 1 }, 0.25f, 10));
        assertNotEquals(q1, new VectorRadiusQuery("f1", new float[] { 0, 1 }, 0.5f, 2));
        assertNotEquals(q1, new VectorRadiusQuery("f1", new float[] { 0, 1 }, 0.25f, 2));
        assertNotEquals(q1, new VectorRadiusQuery("f1", new float[] { 0 }, 0.25f, 10));
    }

    public void testToString() {
        VectorRadiusQuery q1 = new VectorRadiusQuery("f1", new float[] { 0, 1 }, 0.25f, 10);
        assertEquals("VectorRadiusQuery:f1[0.0,...][0.25][10]", q1.toString("ignored"));
    }

    public void testDimensionMismatch() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            VectorRadiusQuery kvq = new VectorRadiusQuery("field", new float[] { 0 }, 0.5f, 10);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> searcher.search(kvq, 10));
            assertEquals("vector dimensions differ: 1!=2", e.getMessage());
        }
    }

    public void testNonVectorField() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            assertMatches(searcher, new VectorRadiusQuery("xyzzy", new float[] { 0 }, 0.3f, 10), 0);
            assertMatches(searcher, new VectorRadiusQuery("id", new float[] { 0 }, 0.3f, 10), 0);
        }
    }

    public void testEmptyIndex() throws IOException {
        try (Directory indexStore = getIndexStore("field"); IndexReader reader = DirectoryReader.open(indexStore)) {
            IndexSearcher searcher = newSearcher(reader);
            VectorRadiusQuery query = new VectorRadiusQuery("field", new float[] { 1, 2 }, -1.0f, 10);
            assertMatches(searcher, query, 0);
        }
    }

    public void testFindAll() throws IOException {
        try (
            Directory indexStore = getIndexStore(
                "field",
                new float[] { 0.0f, 1.0f },
                new float[] { 1.0f, 2.0f },
                new float[] { 0.1f, 0.1f }
            );
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);

            VectorRadiusQuery query = new VectorRadiusQuery("field", new float[] { 0.1f, 1.0f }, 0.0f, 10);
            ScoreDoc[] scoreDocs = searcher.search(query, 3).scoreDocs;
            assertIdsMatch(reader, scoreDocs, "id0", "id1", "id2");
        }
    }

    public void testFindNone() throws IOException {
        try (
            Directory indexStore = getIndexStore(
                "field",
                new float[] { 0.0f, 1.0f },
                new float[] { 1.0f, 2.0f },
                new float[] { 0.1f, 0.1f }
            );
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            VectorRadiusQuery query = new VectorRadiusQuery("field", new float[] { 0.1f, 1.0f }, 1.0f, 10);
            assertMatches(searcher, query, 0);
        }
    }

    public void testBasicQuery() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, new IndexWriterConfig())) {
                int id = 0;
                for (int i = 1; i < 6; i++) {
                    for (int j = 0; j < 5; j++) {
                        Document doc = new Document();
                        doc.add(new KnnVectorField("field", new float[] { i, j }, COSINE));
                        doc.add(new StringField("id", "id" + id++, Field.Store.YES));
                        w.addDocument(doc);
                    }
                    w.flush();
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = newSearcher(reader);
                TopDocs results = searcher.search(new VectorRadiusQuery("field", new float[] { 2.0f, 0.0f }, 0.99f, 10), 50);
                assertIdsMatch(reader, results.scoreDocs, "id0", "id5", "id10", "id15", "id20");
            }
        }
    }

    /**
     * Tests with random vectors, number of documents, etc. Uses RandomIndexWriter.
     */
    public void testRandom() throws IOException {
        int numDocs = atLeast(500);
        int dimension = atLeast(5);
        int numIters = atLeast(10);
        boolean everyDocHasAVector = random().nextBoolean();
        try (Directory d = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), d);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                if (everyDocHasAVector || random().nextInt(10) != 2) {
                    doc.add(new KnnVectorField("field", randomVector(dimension), COSINE));
                }
                w.addDocument(doc);
            }
            w.forceMerge(1);
            w.close();

            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = newSearcher(reader);
                for (int i = 0; i < numIters; i++) {
                    float[] queryVector = randomVector(dimension);
                    int numCands = random().nextInt(50) + 1;
                    // Choose a generous radius so we're very likely to match some documents
                    VectorRadiusQuery query = new VectorRadiusQuery("field", queryVector, 0.5f, numCands);

                    int n = random().nextInt(100) + 1;
                    TopDocs results = searcher.search(query, n);
                    assertTrue(results.totalHits.value >= results.scoreDocs.length);

                    // verify the results are in descending doc ID order and the vectors are within the radius
                    assert reader.leaves().size() == 1;
                    LeafReaderContext context = reader.leaves().get(0);
                    VectorValues vectorValues = context.reader().getVectorValues("field");

                    int last = -1;
                    for (ScoreDoc scoreDoc : results.scoreDocs) {
                        int nextDoc = vectorValues.advance(scoreDoc.doc);
                        assertNotEquals(NO_MORE_DOCS, nextDoc);

                        float similarity = COSINE.compare(queryVector, vectorValues.vectorValue());
                        assertTrue(similarity >= 0.5f);

                        assertTrue(scoreDoc.doc > last);
                        last = scoreDoc.doc;
                    }
                }
            }
        }
    }

    public void testDeletes() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            final int numDocs = atLeast(100);
            final int dim = 30;
            for (int i = 0; i < numDocs; ++i) {
                Document d = new Document();
                d.add(new StringField("index", String.valueOf(i), Field.Store.YES));
                if (frequently()) {
                    d.add(new KnnVectorField("vector", randomVector(dim), COSINE));
                }
                w.addDocument(d);
            }
            w.commit();

            // Delete some documents at random, both those with and without vectors
            Set<Term> toDelete = new HashSet<>();
            for (int i = 0; i < 25; i++) {
                int index = random().nextInt(numDocs);
                toDelete.add(new Term("index", String.valueOf(index)));
            }
            w.deleteDocuments(toDelete.toArray(new Term[0]));
            w.commit();

            int hits = 50;
            try (IndexReader reader = DirectoryReader.open(dir)) {
                Set<String> allIds = new HashSet<>();
                IndexSearcher searcher = new IndexSearcher(reader);
                VectorRadiusQuery query = new VectorRadiusQuery("vector", randomVector(dim), 0.0f, 100);
                TopDocs topDocs = searcher.search(query, hits);
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    Document doc = reader.document(scoreDoc.doc, Set.of("index"));
                    String index = doc.get("index");
                    assertFalse("search returned a deleted document: " + index, toDelete.contains(new Term("index", index)));
                    allIds.add(index);
                }
                assertEquals("search missed some documents", hits, allIds.size());
            }
        }
    }

    public void testAllDeletes() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            final int numDocs = atLeast(100);
            final int dim = 30;
            for (int i = 0; i < numDocs; ++i) {
                Document d = new Document();
                d.add(new KnnVectorField("vector", randomVector(dim)));
                w.addDocument(d);
            }
            w.commit();

            w.deleteDocuments(new MatchAllDocsQuery());
            w.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                VectorRadiusQuery query = new VectorRadiusQuery("vector", randomVector(dim), 0.0f, numDocs);
                TopDocs topDocs = searcher.search(query, numDocs);
                assertEquals(0, topDocs.scoreDocs.length);
            }
        }
    }

    /**
     * Creates a new directory and adds documents with the given vectors as kNN vector fields
     */
    private Directory getIndexStore(String field, float[]... contents) throws IOException {
        Directory indexStore = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore);
        for (int i = 0; i < contents.length; ++i) {
            Document doc = new Document();
            doc.add(new KnnVectorField(field, contents[i], COSINE));
            doc.add(new StringField("id", "id" + i, Field.Store.YES));
            writer.addDocument(doc);
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

    private static float[] randomVector(int dimension) {
        float[] result = new float[dimension];
        result[0] = random().nextFloat() + 0.01f;
        for (int i = 1; i < dimension; i++) {
            result[i] = random().nextFloat();
        }
        return result;
    }

    private void assertMatches(IndexSearcher searcher, Query q, int expectedMatches) throws IOException {
        ScoreDoc[] result = searcher.search(q, 1000).scoreDocs;
        assertEquals(expectedMatches, result.length);
    }

    private void assertIdsMatch(IndexReader reader, ScoreDoc[] scoreDocs, String... expectedIds) throws IOException {
        Set<String> actualIds = new HashSet<>();
        for (ScoreDoc scoreDoc : scoreDocs) {
            String actualId = reader.document(scoreDoc.doc).get("id");
            boolean exists = actualIds.add(actualId);
            assertTrue("the results contained a duplicate ID, which should never happen", exists);
        }

        assertEquals("wrong number of results", expectedIds.length, actualIds.size());
        for (String expectedId : expectedIds) {
            assertTrue("ID " + expectedId + " is missing in results", actualIds.contains(expectedId));
        }
    }
}
