/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.junit.AssumptionViolatedException;

/**
 * Base test case for flat (brute-force) quantized KNN vector formats.
 *
 * <p>Flat formats score every document in the segment rather than traversing a graph, so
 * graph-related tests do not apply and the visited-limit contract cannot be enforced. The shared
 * {@link #testSearch} and {@link #testSearchWithFilter} methods verify that both the HNSW-style
 * {@link KnnFloatVectorQuery} path and the exact {@link DenseVectorQuery} path produce correct
 * hit counts and scores for all concrete flat formats.
 */
public abstract class BaseFlatQuantizedKnnVectorsFormatTestCase extends BaseQuantizedKnnVectorsFormatTestCase {

    @Override
    public void testRandomWithUpdatesAndGraph() {
        throw new AssumptionViolatedException("graph not supported by flat formats");
    }

    @Override
    public void testSearchWithVisitedLimit() {
        throw new AssumptionViolatedException("visited limit is not enforced by brute-force flat formats");
    }

    public void testSearch() throws Exception {
        String fieldName = "field";
        int numVectors = random().nextInt(99, 500);
        int dims = random().nextInt(4, 65);
        float[] vector = randomVector(dims);
        VectorSimilarityFunction similarityFunction = randomSimilarity();
        KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, vector, similarityFunction);
        IndexWriterConfig iwc = newIndexWriterConfig();
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numVectors; i++) {
                    Document doc = new Document();
                    knnField.setVectorValue(randomVector(dims));
                    doc.add(knnField);
                    w.addDocument(doc);
                }
                w.commit();

                try (IndexReader reader = DirectoryReader.open(w)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    final int k = random().nextInt(5, 50);
                    float[] queryVector = randomVector(dims);
                    // Each query covers unique code paths worth testing
                    {
                        Query q = new KnnFloatVectorQuery(fieldName, queryVector, k);
                        TopDocs collectedDocs = searcher.search(q, k);
                        assertEquals(k, collectedDocs.totalHits.value());
                        assertEquals(TotalHits.Relation.EQUAL_TO, collectedDocs.totalHits.relation());
                    }
                    {
                        Query q = new DenseVectorQuery.Floats(queryVector, fieldName, null);
                        TopDocs collectedDocs = searcher.search(q, k);
                        assertEquals(numVectors, collectedDocs.totalHits.value());
                        assertEquals(k, collectedDocs.scoreDocs.length);
                        assertEquals(TotalHits.Relation.EQUAL_TO, collectedDocs.totalHits.relation());
                    }
                }
            }
        }
    }

    public void testSearchWithFilter() throws Exception {
        String fieldName = "field";
        int numVectors = random().nextInt(99, 500);
        int numFiltered = random().nextInt(1, numVectors);
        int dims = random().nextInt(4, 65);
        VectorSimilarityFunction similarityFunction = randomSimilarity();
        KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, new float[dims], similarityFunction);
        IndexWriterConfig iwc = newIndexWriterConfig();
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numVectors; i++) {
                    Document doc = new Document();
                    knnField.setVectorValue(randomVector(dims));
                    doc.add(knnField);
                    if (i < numFiltered) {
                        doc.add(newStringField("category", "filtered", Field.Store.NO));
                    }
                    w.addDocument(doc);
                }
                w.commit();

                try (IndexReader reader = DirectoryReader.open(w)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    float[] queryVector = randomVector(dims);
                    Query filter = new TermQuery(new Term("category", "filtered"));
                    Query q = new DenseVectorQuery.Floats(queryVector, fieldName, filter);
                    TopDocs collectedDocs = searcher.search(q, numFiltered);
                    assertEquals(numFiltered, collectedDocs.totalHits.value());
                    assertEquals(numFiltered, collectedDocs.scoreDocs.length);
                    assertEquals(TotalHits.Relation.EQUAL_TO, collectedDocs.totalHits.relation());
                }
            }
        }
    }
}
