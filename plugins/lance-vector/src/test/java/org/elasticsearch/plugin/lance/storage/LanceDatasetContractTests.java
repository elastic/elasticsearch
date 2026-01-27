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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Contract tests for the {@link LanceDataset} interface.
 * <p>
 * These tests validate that any implementation of LanceDataset
 * correctly implements the interface contract. Currently tests
 * FakeLanceDataset; future implementations (RealLanceDataset)
 * should also pass these tests.
 */
public class LanceDatasetContractTests extends ESTestCase {

    private Path tempFile;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempFile = createTempFile();
    }

    /**
     * Create a LanceDataset implementation for testing.
     * This method can be overridden in subclasses to test other implementations.
     */
    protected LanceDataset createDataset(String json, int dims) throws IOException {
        Files.writeString(tempFile, json);
        return FakeLanceDataset.load("file://" + tempFile.toString(), dims);
    }

    // ========== dims() contract ==========

    public void testDimsReturnsCorrectDimensionality() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 2.0, 3.0, 4.0, 5.0] }
            ]
            """, 5);
        assertThat(dataset.dims(), equalTo(5));
    }

    public void testDimsConsistentAcrossCalls() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 2.0, 3.0] }
            ]
            """, 3);
        int dims1 = dataset.dims();
        int dims2 = dataset.dims();
        assertThat(dims1, equalTo(dims2));
    }

    // ========== size() contract ==========

    public void testSizeReturnsVectorCount() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] },
              { "id": "doc2", "vector": [0.0, 1.0, 0.0] },
              { "id": "doc3", "vector": [0.0, 0.0, 1.0] }
            ]
            """, 3);
        assertThat(dataset.size(), equalTo(3L));
    }

    public void testSizeSingleVector() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """, 3);
        assertThat(dataset.size(), equalTo(1L));
    }

    // ========== uri() contract ==========

    public void testUriReturnsLoadUri() throws Exception {
        String uri = "file://" + tempFile.toString();
        Files.writeString(tempFile, """
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """);
        LanceDataset dataset = FakeLanceDataset.load(uri, 3);
        assertThat(dataset.uri(), equalTo(uri));
    }

    public void testUriNotNull() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """, 3);
        assertThat(dataset.uri(), notNullValue());
    }

    // ========== search() contract ==========

    public void testSearchReturnsRequestedCandidates() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] },
              { "id": "doc2", "vector": [0.9, 0.1, 0.0] },
              { "id": "doc3", "vector": [0.8, 0.2, 0.0] },
              { "id": "doc4", "vector": [0.7, 0.3, 0.0] },
              { "id": "doc5", "vector": [0.6, 0.4, 0.0] }
            ]
            """, 3);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 3, "cosine");

        assertThat(results, hasSize(3));
    }

    public void testSearchReturnsAllWhenNumCandidatesExceedsSize() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] },
              { "id": "doc2", "vector": [0.0, 1.0, 0.0] }
            ]
            """, 3);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 100, "cosine");

        assertThat(results, hasSize(2));
    }

    public void testSearchResultsOrderedByScoreDescending() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [0.5, 0.5, 0.0] },
              { "id": "doc2", "vector": [0.1, 0.9, 0.0] },
              { "id": "doc3", "vector": [1.0, 0.0, 0.0] }
            ]
            """, 3);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "cosine");

        for (int i = 1; i < results.size(); i++) {
            assertThat(
                "Results should be ordered by score descending",
                results.get(i).score(),
                lessThanOrEqualTo(results.get(i - 1).score())
            );
        }
    }

    public void testSearchCandidatesHaveNonNullId() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """, 3);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "cosine");

        for (LanceDataset.Candidate c : results) {
            assertThat(c.id(), notNullValue());
        }
    }

    public void testSearchWithCosineReturnsScoresInValidRange() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] },
              { "id": "doc2", "vector": [0.0, 1.0, 0.0] },
              { "id": "doc3", "vector": [-1.0, 0.0, 0.0] }
            ]
            """, 3);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 10, "cosine");

        for (LanceDataset.Candidate c : results) {
            assertThat("Cosine score should be >= -1", c.score(), greaterThanOrEqualTo(-1.0f));
            assertThat("Cosine score should be <= 1", c.score(), lessThanOrEqualTo(1.0f));
        }
    }

    public void testSearchDimensionMismatchThrows() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """, 3);

        float[] wrongDimsQuery = { 1.0f, 0.0f }; // 2 dims instead of 3

        expectThrows(IllegalArgumentException.class, () -> dataset.search(wrongDimsQuery, 10, "cosine"));
    }

    // ========== Candidate record contract ==========

    public void testCandidateRecordAccessors() {
        LanceDataset.Candidate c = new LanceDataset.Candidate("test-id", 0.95f);
        assertThat(c.id(), equalTo("test-id"));
        assertThat(c.score(), equalTo(0.95f));
    }

    public void testCandidateRecordEquality() {
        LanceDataset.Candidate c1 = new LanceDataset.Candidate("id", 0.5f);
        LanceDataset.Candidate c2 = new LanceDataset.Candidate("id", 0.5f);
        assertThat(c1, equalTo(c2));
        assertThat(c1.hashCode(), equalTo(c2.hashCode()));
    }

    // ========== close() contract ==========

    public void testCloseDoesNotThrow() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """, 3);

        // Should not throw
        dataset.close();
    }

    // ========== hasIndex() contract ==========

    public void testHasIndexReturnsFalseForFakeDataset() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 0.0, 0.0] }
            ]
            """, 3);

        // FakeLanceDataset uses brute-force, so hasIndex should be false
        assertThat(dataset.hasIndex(), equalTo(false));
    }

    // ========== Similarity functions ==========

    public void testCosineIdenticalVectors() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 2.0, 3.0] }
            ]
            """, 3);

        float[] query = { 1.0f, 2.0f, 3.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 1, "cosine");

        // Identical vectors should have cosine similarity of 1.0
        assertThat(results.get(0).score(), greaterThan(0.999f));
    }

    public void testCosineOrthogonalVectors() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [0.0, 1.0, 0.0] }
            ]
            """, 3);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 1, "cosine");

        // Orthogonal vectors should have cosine similarity close to 0
        assertThat(Math.abs(results.get(0).score()), lessThanOrEqualTo(0.001f));
    }

    public void testDotProductCalculation() throws Exception {
        LanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [2.0, 3.0, 0.0] }
            ]
            """, 3);

        float[] query = { 1.0f, 2.0f, 0.0f };
        // Expected: 1*2 + 2*3 + 0*0 = 8
        List<LanceDataset.Candidate> results = dataset.search(query, 1, "dot_product");

        assertThat(results.get(0).score(), greaterThan(7.99f));
        assertThat(results.get(0).score(), lessThanOrEqualTo(8.01f));
    }
}
