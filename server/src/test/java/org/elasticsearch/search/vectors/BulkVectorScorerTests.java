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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThan;

public class BulkVectorScorerTests extends ESTestCase {

    private static final String VECTOR_FIELD = "vector";
    private static final int VECTOR_DIMS = 64;

    public void testBulkScorerImplementation() throws IOException {
        // Enable bulk processing for this test
        System.setProperty("es.bulk_vector_scoring", "true");

        try (Directory dir = new MMapDirectory(createTempDir())) {
            createTestIndex(dir, 30);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                // Get top documents for bulk processing
                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 20);

                // Create bulk vector query
                float[] queryVector = randomVector(VECTOR_DIMS);
                var valueSource = new AccessibleVectorSimilarityFloatValueSource(
                    VECTOR_FIELD,
                    queryVector,
                    VectorSimilarityFunction.COSINE
                );

                BulkVectorFunctionScoreQuery bulkQuery = new BulkVectorFunctionScoreQuery(
                    new KnnScoreDocQuery(topDocs.scoreDocs, reader),
                    valueSource,
                    topDocs.scoreDocs
                );

                Weight weight = bulkQuery.createWeight(searcher, org.apache.lucene.search.ScoreMode.COMPLETE, 1.0f);

                LeafReaderContext leafContext = reader.leaves().get(0);
                BulkScorer bulkScorer = weight.scorerSupplier(leafContext).bulkScorer();

                assertNotNull("BulkScorer should be created", bulkScorer);
                assertTrue("Should be BulkVectorScorer instance", bulkScorer instanceof BulkVectorScorer);

                // Test bulk scoring execution
                TestCollector collector = new TestCollector();
                int result = bulkScorer.score(collector, null, 0, Integer.MAX_VALUE);

                // Verify bulk processing occurred
                assertThat("Should collect documents", collector.collectedDocs.size(), greaterThan(0));
                assertTrue("Should have precomputed scores", collector.hasValidScores());

            }
        } finally {
            System.clearProperty("es.bulk_vector_scoring");
        }
    }

    public void testBulkProcessorCollectorInterception() throws IOException {
        // Test that BulkVectorScorer properly intercepts collector calls
        System.setProperty("es.bulk_vector_scoring", "true");

        try (Directory dir = new MMapDirectory(createTempDir())) {
            createTestIndex(dir, 25);

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);

                // Create bulk query with known documents
                TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 15);
                float[] queryVector = randomVector(VECTOR_DIMS);
                var valueSource = new AccessibleVectorSimilarityFloatValueSource(
                    VECTOR_FIELD,
                    queryVector,
                    VectorSimilarityFunction.DOT_PRODUCT
                );

                BulkVectorFunctionScoreQuery bulkQuery = new BulkVectorFunctionScoreQuery(
                    new KnnScoreDocQuery(topDocs.scoreDocs, reader),
                    valueSource,
                    topDocs.scoreDocs
                );

                // Execute with custom collector to verify interception
                TestCollector collector = new TestCollector();

                Weight weight = bulkQuery.createWeight(searcher, org.apache.lucene.search.ScoreMode.COMPLETE, 1.0f);
                LeafReaderContext leafContext = reader.leaves().get(0);
                BulkScorer bulkScorer = weight.scorerSupplier(leafContext).bulkScorer();

                bulkScorer.score(collector, null, 0, 50);

                // Verify collector received bulk-processed scores
                assertTrue("Collector should have received documents", collector.collectedDocs.size() > 0);

                for (CollectedDoc doc : collector.collectedDocs) {
                    assertTrue("All scores should be finite", Float.isFinite(doc.score));
                    // For DOT_PRODUCT, scores can be negative, so just check they're computed
                    assertNotEquals("Score should not be exactly zero (indicating computation)", 0.0f, doc.score, 0.001f);
                }
            }
        } finally {
            System.clearProperty("es.bulk_vector_scoring");
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

    private float[] randomVector(int dimensions) {
        float[] vector = new float[dimensions];
        for (int i = 0; i < dimensions; i++) {
            vector[i] = randomFloat() * 2.0f - 1.0f;
        }
        return vector;
    }

    /**
     * Test collector that captures documents and scores for verification
     */
    private static class TestCollector implements LeafCollector {
        final List<CollectedDoc> collectedDocs = new ArrayList<>();
        private Scorable scorer;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            float score = scorer.score();
            collectedDocs.add(new CollectedDoc(doc, score));
        }

        boolean hasValidScores() {
            return collectedDocs.stream().allMatch(doc -> Float.isFinite(doc.score));
        }
    }

    private static class CollectedDoc {
        final int docId;
        final float score;

        CollectedDoc(int docId, float score) {
            this.docId = docId;
            this.score = score;
        }
    }
}
