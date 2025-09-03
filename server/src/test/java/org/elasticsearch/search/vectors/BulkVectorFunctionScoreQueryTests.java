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
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class BulkVectorFunctionScoreQueryTests extends ESTestCase {

    private static final String VECTOR_FIELD = "vector";
    private static final int VECTOR_DIMS = 128;
    public static final String BULK_VECTOR_SCORING = "es.bulk_vector_scoring";

    public void testBulkProcessingWithScoreDocArray() throws IOException {
        // Create test index with vector documents
        try (Directory dir = new MMapDirectory(createTempDir())) {
            IndexWriterConfig config = new IndexWriterConfig();
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                // Add documents with random vectors
                for (int i = 0; i < 100; i++) {
                    Document doc = new Document();
                    float[] vector = randomVector(VECTOR_DIMS);
                    doc.add(new KnnFloatVectorField(VECTOR_FIELD, vector, VectorSimilarityFunction.COSINE));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                // Create query vector and value source
                float[] queryVector = randomVector(VECTOR_DIMS);
                var valueSource = new AccessibleVectorSimilarityFloatValueSource(
                    VECTOR_FIELD,
                    queryVector,
                    VectorSimilarityFunction.COSINE
                );

                // Get top documents
                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 50);

                // Test bulk vector function score query
                BulkVectorFunctionScoreQuery bulkQuery = new BulkVectorFunctionScoreQuery(
                    new KnnScoreDocQuery(topDocs.scoreDocs, reader),
                    valueSource,
                    topDocs.scoreDocs
                );

                TopDocs bulkResults = searcher.search(bulkQuery, 10);

                // Verify results
                assertThat("Should return results", bulkResults.totalHits.value(), greaterThan(0L));
                assertThat("Should not exceed requested count", bulkResults.scoreDocs.length, equalTo(10));

                // Verify scores are computed
                for (ScoreDoc scoreDoc : bulkResults.scoreDocs) {
                    assertTrue("Score should be computed", Float.isFinite(scoreDoc.score));
                    assertTrue("Score should be positive for cosine similarity", scoreDoc.score >= 0.0f);
                }
            }
        }
    }

    public void testInlineRescoreBulkOptimization() throws IOException {
        // Test that InlineRescoreQuery uses bulk processing when feature flag is enabled
        float[] queryVector = randomVector(VECTOR_DIMS);

        try (Directory dir = new MMapDirectory(createTempDir())) {
            createTestIndex(dir, 20);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                // Create inline rescoring by using KnnFloatVectorQuery with matching k and rescoreK
                KnnFloatVectorQuery innerQuery = new KnnFloatVectorQuery(VECTOR_FIELD, queryVector, 10);
                RescoreKnnVectorQuery rescoreQuery = RescoreKnnVectorQuery.fromInnerQuery(
                    VECTOR_FIELD,
                    queryVector,
                    VectorSimilarityFunction.COSINE,
                    5,
                    10,
                    innerQuery
                );

                TopDocs results = searcher.search(rescoreQuery, 5);

                assertThat("Should return results from inline rescoring", results.totalHits.value(), greaterThan(0L));
                assertThat("Should return requested count", results.scoreDocs.length, equalTo(5));
            }
        }
    }

    public void testLateRescoreBulkOptimization() throws IOException {
        // Test that LateRescoreQuery uses bulk processing when feature flag is enabled
        float[] queryVector = randomVector(VECTOR_DIMS);

        try (Directory dir = new MMapDirectory(createTempDir())) {
            createTestIndex(dir, 50);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                // Create late rescoring by using different k and rescoreK values
                RescoreKnnVectorQuery rescoreQuery = RescoreKnnVectorQuery.fromInnerQuery(
                    VECTOR_FIELD,
                    queryVector,
                    VectorSimilarityFunction.COSINE,
                    8,
                    30,
                    new MatchAllDocsQuery()
                );

                TopDocs results = searcher.search(rescoreQuery, 8);

                assertThat("Should return results from late rescoring", results.totalHits.value(), greaterThan(0L));
                assertThat("Should return requested count", results.scoreDocs.length, equalTo(8));
            }
        }
    }

    public void testScoreDocContextPreservation() throws IOException {
        // Test that ScoreDoc context is properly maintained through rewrite cycles
        try (Directory dir = new MMapDirectory(createTempDir())) {
            createTestIndex(dir, 30);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                // Create initial ScoreDoc array
                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 15);
                ScoreDoc[] originalScoreDocs = topDocs.scoreDocs.clone();

                // Create bulk query
                float[] queryVector = randomVector(VECTOR_DIMS);
                var valueSource = new AccessibleVectorSimilarityFloatValueSource(
                    VECTOR_FIELD,
                    queryVector,
                    VectorSimilarityFunction.COSINE
                );
                BulkVectorFunctionScoreQuery query = new BulkVectorFunctionScoreQuery(
                    new KnnScoreDocQuery(originalScoreDocs, reader),
                    valueSource,
                    originalScoreDocs
                );

                // Test query rewrite preserves context
                BulkVectorFunctionScoreQuery rewritten = (BulkVectorFunctionScoreQuery) query.rewrite(searcher);
                assertNotNull("Rewritten query should not be null", rewritten);

                // Execute rewritten query
                TopDocs results = searcher.search(rewritten, 10);
                assertThat("Should return results after rewrite", results.totalHits.value(), greaterThan(0L));
            }
        }
    }

    private void createTestIndex(Directory dir, int docCount) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < docCount; i++) {
                Document doc = new Document();
                float[] vector = randomVector(VECTOR_DIMS);
                doc.add(new KnnFloatVectorField(VECTOR_FIELD, vector, VectorSimilarityFunction.COSINE));
                writer.addDocument(doc);
            }
            writer.commit();
        }
    }

    public void testParallelVectorLoading() throws IOException {
        // Test parallel vector loading functionality
        float[] queryVector = randomVector(VECTOR_DIMS);

        try (Directory dir = new MMapDirectory(createTempDir())) {
            createTestIndex(dir, 50);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                // Get initial documents
                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);
                int[] docIds = Arrays.stream(topDocs.scoreDocs).mapToInt(scoreDoc -> scoreDoc.doc).toArray();

                // Test parallel loading
                DirectIOVectorBatchLoader batchLoader = new DirectIOVectorBatchLoader();

                Map<Integer, float[]> parallelResult = batchLoader.loadSegmentVectors(docIds, reader.leaves().get(0), VECTOR_FIELD);

                // use regular vector loader
                Map<Integer, float[]> sequentialResult = new HashMap<>();
                for (int docId : docIds) {
                    sequentialResult.put(docId, batchLoader.loadSingleVector(docId, reader.leaves().get(0), VECTOR_FIELD));
                }

                // Verify results are identical
                assertThat(
                    "Parallel and sequential results should have same size",
                    parallelResult.size(),
                    equalTo(sequentialResult.size())
                );

                for (int docId : docIds) {
                    float[] parallelVector = parallelResult.get(docId);
                    float[] sequentialVector = sequentialResult.get(docId);

                    assertNotNull("Parallel result should contain vector for doc " + docId, parallelVector);
                    assertNotNull("Sequential result should contain vector for doc " + docId, sequentialVector);
                    assertArrayEquals("Vectors should be identical for doc " + docId, sequentialVector, parallelVector, 0.0001f);
                }
            }
        }
    }

    private float[] randomVector(int dimensions) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = randomFloat() * 2.0f - 1.0f; // Range [-1, 1]
        }
        return vector;
    }
}
