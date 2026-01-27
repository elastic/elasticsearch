/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.lance.storage;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Unit tests for {@link FakeLanceDataset}.
 */
public class FakeLanceDatasetTests extends ESTestCase {

    public void testLoadFromLocalFile() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] },
              { "id": "doc2", "vector": [0.0, 1.0, 0.0] },
              { "id": "doc3", "vector": [0.0, 0.0, 1.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        assertThat(dataset.dims(), equalTo(3));
    }

    public void testLoadWithoutFilePrefix() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load(tempFile.toString(), 3);
        assertThat(dataset.dims(), equalTo(3));
    }

    public void testDimensionValidation() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        // Expect 5 dims but dataset has 3
        IOException ex = expectThrows(IOException.class, () -> FakeLanceDataset.load("file://" + tempFile.toString(), 5));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("Expected dims=5"));
    }

    public void testDimensionValidationZeroMeansAny() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        // expectedDims=0 means accept any dimension
        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 0);
        assertThat(dataset.dims(), equalTo(3));
    }

    public void testEmptyDatasetThrows() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, "[]");

        IOException ex = expectThrows(IOException.class, () -> FakeLanceDataset.load("file://" + tempFile.toString(), 0));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("Dataset empty"));
    }

    public void testMissingIdFieldThrows() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        expectThrows(NullPointerException.class, () -> FakeLanceDataset.load("file://" + tempFile.toString(), 3));
    }

    public void testMissingVectorFieldThrows() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1" }
            ]
            """);

        IOException ex = expectThrows(IOException.class, () -> FakeLanceDataset.load("file://" + tempFile.toString(), 0));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("vector missing"));
    }

    public void testInvalidJsonThrows() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, "not valid json");

        expectThrows(Exception.class, () -> FakeLanceDataset.load("file://" + tempFile.toString(), 0));
    }

    public void testNonArrayJsonThrows() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            { "id": "doc1", "vector": [1.0] }
            """);

        IOException ex = expectThrows(IOException.class, () -> FakeLanceDataset.load("file://" + tempFile.toString(), 0));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("Expected JSON array"));
    }

    public void testFileNotFoundThrows() throws Exception {
        // Create a temp directory and use a non-existent file path within it
        // This avoids entitlement issues with checking arbitrary filesystem paths
        Path tempDir = createTempDir();
        Path nonExistentFile = tempDir.resolve("nonexistent_dataset.json");

        IOException ex = expectThrows(IOException.class, () -> FakeLanceDataset.load("file://" + nonExistentFile.toString(), 3));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("not found"));
    }

    public void testCosineSimilarityIdenticalVectors() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "cosine");

        assertThat(results, hasSize(1));
        assertThat((double) results.get(0).score(), closeTo(1.0, 0.0001));
    }

    public void testCosineSimilarityOrthogonalVectors() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [0.0, 1.0, 0.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "cosine");

        assertThat(results, hasSize(1));
        assertThat((double) results.get(0).score(), closeTo(0.0, 0.0001));
    }

    public void testCosineSimilarityOppositeVectors() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [-1.0, 0.0, 0.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "cosine");

        assertThat(results, hasSize(1));
        assertThat((double) results.get(0).score(), closeTo(-1.0, 0.0001));
    }

    public void testCosineSimilarityZeroVector() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [0.0, 0.0, 0.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "cosine");

        assertThat(results, hasSize(1));
        // Zero vector should have 0 similarity
        assertThat((double) results.get(0).score(), closeTo(0.0, 0.0001));
    }

    public void testDotProductSimilarity() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [2.0, 3.0, 0.0] },
              { "id": "doc2", "vector": [1.0, 1.0, 1.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 2.0f, 0.0f };
        // dot(query, doc1) = 1*2 + 2*3 + 0*0 = 8
        // dot(query, doc2) = 1*1 + 2*1 + 0*1 = 3
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "dot_product");

        assertThat(results, hasSize(2));
        assertThat(results.get(0).id(), equalTo("doc1"));
        assertThat((double) results.get(0).score(), closeTo(8.0, 0.0001));
        assertThat(results.get(1).id(), equalTo("doc2"));
        assertThat((double) results.get(1).score(), closeTo(3.0, 0.0001));
    }

    public void testDotSimilarityAlias() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 1.0, 1.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 1.0f, 1.0f };

        // "dot" should be alias for "dot_product"
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "dot");
        assertThat(results, hasSize(1));
        assertThat((double) results.get(0).score(), closeTo(3.0, 0.0001));
    }

    public void testDefaultSimilarityIsCosine() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 0.0f, 0.0f };

        // "unknown" should default to cosine
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "unknown_similarity");
        assertThat(results, hasSize(1));
        assertThat((double) results.get(0).score(), closeTo(1.0, 0.0001));
    }

    public void testSearchOrdersByScoreDescending() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [0.5, 0.5, 0.0] },
              { "id": "doc2", "vector": [0.1, 0.0, 0.9] },
              { "id": "doc3", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "cosine");

        assertThat(results, hasSize(3));
        // doc3 is identical to query, should be first
        assertThat(results.get(0).id(), equalTo("doc3"));
        // Verify descending order
        for (int i = 1; i < results.size(); i++) {
            assertThat(results.get(i).score(), lessThanOrEqualTo(results.get(i - 1).score()));
        }
    }

    public void testSearchLimitsToNumCandidates() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] },
              { "id": "doc2", "vector": [0.9, 0.1, 0.0] },
              { "id": "doc3", "vector": [0.8, 0.2, 0.0] },
              { "id": "doc4", "vector": [0.7, 0.3, 0.0] },
              { "id": "doc5", "vector": [0.6, 0.4, 0.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 0.0f, 0.0f };

        List<LanceDataset.Candidate> results = dataset.search(query, 2, "cosine");
        assertThat(results, hasSize(2));

        // Should get top 2 by score
        assertThat(results.get(0).id(), equalTo("doc1"));
        assertThat(results.get(1).id(), equalTo("doc2"));
    }

    public void testSearchWithNumCandidatesLargerThanDataset() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] },
              { "id": "doc2", "vector": [0.0, 1.0, 0.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 0.0f, 0.0f };

        // Request more than available
        List<LanceDataset.Candidate> results = dataset.search(query, 100, "cosine");
        assertThat(results, hasSize(2));
    }

    public void testVectorDimensionMismatchThrows() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] wrongDimsQuery = { 1.0f, 0.0f }; // 2 dims instead of 3

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> dataset.search(wrongDimsQuery, 10, "cosine"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("mismatch"));
    }

    public void testMetadataFieldsAreIgnored() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0], "title": "Test", "count": 42, "tags": ["a", "b"] }
            ]
            """);

        // Should load successfully, ignoring extra fields
        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        assertThat(dataset.dims(), equalTo(3));
    }

    public void testIntegerVectorValues() throws Exception {
        Path tempFile = createTempFile();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1, 2, 3] }
            ]
            """);

        FakeLanceDataset dataset = FakeLanceDataset.load("file://" + tempFile.toString(), 3);
        float[] query = { 1.0f, 2.0f, 3.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "cosine");

        assertThat(results, hasSize(1));
        assertThat((double) results.get(0).score(), closeTo(1.0, 0.0001));
    }

    public void testEmbeddedResourceNotFound() {
        IOException ex = expectThrows(IOException.class, () -> FakeLanceDataset.load("embedded:nonexistent-resource.json", 0));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.containsString("Embedded resource not found"));
    }

    public void testCandidateRecord() {
        LanceDataset.Candidate c = new LanceDataset.Candidate("test-id", 0.95f);
        assertThat(c.id(), equalTo("test-id"));
        assertThat(c.score(), equalTo(0.95f));
    }

    public void testEntryRecord() {
        float[] vec = { 1.0f, 2.0f, 3.0f };
        FakeLanceDataset.Entry e = new FakeLanceDataset.Entry("test-id", vec);
        assertThat(e.id(), equalTo("test-id"));
        assertThat(e.vector(), equalTo(vec));
    }
}
