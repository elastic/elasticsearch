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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Mathematical correctness tests for similarity calculations.
 * <p>
 * These tests verify that cosine similarity and dot product are
 * calculated correctly using known mathematical results.
 */
public class SimilarityMathTests extends ESTestCase {

    private Path tempFile;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempFile = createTempFile();
    }

    private FakeLanceDataset createDataset(String json) throws Exception {
        Files.writeString(tempFile, json);
        return FakeLanceDataset.load("file://" + tempFile.toString(), 0);
    }

    // ========== Cosine Similarity Mathematical Tests ==========

    /**
     * Cosine similarity: cos(θ) = (A · B) / (||A|| × ||B||)
     * For unit vectors: cos(θ) = A · B
     */
    public void testCosineUnitVectorsAlongAxes() throws Exception {
        // Unit vectors along each axis
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "x", "vector": [1.0, 0.0, 0.0] },
              { "id": "y", "vector": [0.0, 1.0, 0.0] },
              { "id": "z", "vector": [0.0, 0.0, 1.0] }
            ]
            """);

        // Query along x-axis
        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 3, "cosine");

        // x should have similarity 1.0 (identical)
        LanceDataset.Candidate xResult = results.stream().filter(c -> c.id().equals("x")).findFirst().get();
        assertThat((double) xResult.score(), closeTo(1.0, 0.0001));

        // y should have similarity 0.0 (orthogonal)
        LanceDataset.Candidate yResult = results.stream().filter(c -> c.id().equals("y")).findFirst().get();
        assertThat((double) yResult.score(), closeTo(0.0, 0.0001));

        // z should have similarity 0.0 (orthogonal)
        LanceDataset.Candidate zResult = results.stream().filter(c -> c.id().equals("z")).findFirst().get();
        assertThat((double) zResult.score(), closeTo(0.0, 0.0001));
    }

    /**
     * Opposite vectors should have cosine similarity of -1.
     */
    public void testCosineOppositeVectors() throws Exception {
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "opposite", "vector": [-1.0, 0.0, 0.0] }
            ]
            """);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 1, "cosine");

        assertThat((double) results.get(0).score(), closeTo(-1.0, 0.0001));
    }

    /**
     * 45-degree angle vectors should have cosine similarity of √2/2 ≈ 0.7071.
     * Vector (1,1,0) normalized is (1/√2, 1/√2, 0)
     * cos(45°) = 0.7071...
     */
    public void testCosine45DegreeAngle() throws Exception {
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "diagonal", "vector": [1.0, 1.0, 0.0] }
            ]
            """);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 1, "cosine");

        // cos(45°) = 1/√2 ≈ 0.7071
        double expected = 1.0 / Math.sqrt(2.0);
        assertThat((double) results.get(0).score(), closeTo(expected, 0.0001));
    }

    /**
     * Vectors with different magnitudes should have same cosine as normalized.
     * (2, 0, 0) and (1, 0, 0) should both have similarity 1.0 to query (1, 0, 0)
     */
    public void testCosineMagnitudeIndependent() throws Exception {
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "unit", "vector": [1.0, 0.0, 0.0] },
              { "id": "scaled", "vector": [100.0, 0.0, 0.0] }
            ]
            """);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 2, "cosine");

        // Both should have similarity 1.0
        for (LanceDataset.Candidate c : results) {
            assertThat((double) c.score(), closeTo(1.0, 0.0001));
        }
    }

    /**
     * Zero vector should have similarity 0 with any vector.
     */
    public void testCosineZeroVector() throws Exception {
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "zero", "vector": [0.0, 0.0, 0.0] }
            ]
            """);

        float[] query = { 1.0f, 2.0f, 3.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 1, "cosine");

        assertThat((double) results.get(0).score(), closeTo(0.0, 0.0001));
    }

    // ========== Dot Product Mathematical Tests ==========

    /**
     * Dot product: A · B = Σ(ai × bi)
     */
    public void testDotProductBasicCalculation() throws Exception {
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [2.0, 3.0, 4.0] }
            ]
            """);

        float[] query = { 1.0f, 2.0f, 3.0f };
        // Expected: 1*2 + 2*3 + 3*4 = 2 + 6 + 12 = 20
        List<LanceDataset.Candidate> results = dataset.search(query, 1, "dot_product");

        assertThat((double) results.get(0).score(), closeTo(20.0, 0.0001));
    }

    /**
     * Dot product of orthogonal vectors is 0.
     */
    public void testDotProductOrthogonal() throws Exception {
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "orthogonal", "vector": [0.0, 1.0, 0.0] }
            ]
            """);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 1, "dot_product");

        assertThat((double) results.get(0).score(), closeTo(0.0, 0.0001));
    }

    /**
     * Dot product is magnitude-dependent (unlike cosine).
     */
    public void testDotProductMagnitudeDependent() throws Exception {
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "unit", "vector": [1.0, 0.0, 0.0] },
              { "id": "scaled", "vector": [10.0, 0.0, 0.0] }
            ]
            """);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 2, "dot_product");

        // Scaled should be first (higher dot product)
        assertThat(results.get(0).id(), equalTo("scaled"));
        assertThat((double) results.get(0).score(), closeTo(10.0, 0.0001));
        assertThat((double) results.get(1).score(), closeTo(1.0, 0.0001));
    }

    /**
     * Dot product with negative values.
     */
    public void testDotProductNegative() throws Exception {
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "negative", "vector": [-2.0, -3.0, -4.0] }
            ]
            """);

        float[] query = { 1.0f, 1.0f, 1.0f };
        // Expected: 1*(-2) + 1*(-3) + 1*(-4) = -9
        List<LanceDataset.Candidate> results = dataset.search(query, 1, "dot_product");

        assertThat((double) results.get(0).score(), closeTo(-9.0, 0.0001));
    }

    /**
     * "dot" alias should work same as "dot_product".
     */
    public void testDotAliasEqualsDotProduct() throws Exception {
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "doc1", "vector": [1.0, 2.0, 3.0] }
            ]
            """);

        float[] query = { 1.0f, 1.0f, 1.0f };

        List<LanceDataset.Candidate> dotResults = dataset.search(query, 1, "dot");
        List<LanceDataset.Candidate> dotProductResults = dataset.search(query, 1, "dot_product");

        assertThat(dotResults.get(0).score(), equalTo(dotProductResults.get(0).score()));
    }

    // ========== Ranking Correctness Tests ==========

    /**
     * Verify that cosine similarity correctly ranks vectors by angle.
     */
    public void testCosineRankingByAngle() throws Exception {
        // Create vectors at known angles from query
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "angle0", "vector": [1.0, 0.0, 0.0] },
              { "id": "angle45", "vector": [1.0, 1.0, 0.0] },
              { "id": "angle90", "vector": [0.0, 1.0, 0.0] },
              { "id": "angle135", "vector": [-1.0, 1.0, 0.0] },
              { "id": "angle180", "vector": [-1.0, 0.0, 0.0] }
            ]
            """);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 5, "cosine");

        // Should be ordered: angle0, angle45, angle90, angle135, angle180
        assertThat(results.get(0).id(), equalTo("angle0"));
        assertThat(results.get(1).id(), equalTo("angle45"));
        assertThat(results.get(2).id(), equalTo("angle90"));
        assertThat(results.get(3).id(), equalTo("angle135"));
        assertThat(results.get(4).id(), equalTo("angle180"));
    }

    /**
     * Verify that dot product correctly ranks by projection magnitude.
     */
    public void testDotProductRankingByMagnitude() throws Exception {
        FakeLanceDataset dataset = createDataset("""
            [
              { "id": "small", "vector": [1.0, 0.0, 0.0] },
              { "id": "medium", "vector": [5.0, 0.0, 0.0] },
              { "id": "large", "vector": [10.0, 0.0, 0.0] }
            ]
            """);

        float[] query = { 1.0f, 0.0f, 0.0f };
        List<LanceDataset.Candidate> results = dataset.search(query, 3, "dot_product");

        // Should be ordered by magnitude: large, medium, small
        assertThat(results.get(0).id(), equalTo("large"));
        assertThat(results.get(1).id(), equalTo("medium"));
        assertThat(results.get(2).id(), equalTo("small"));
    }

    // ========== Higher Dimension Tests ==========

    /**
     * Test with higher dimensional vectors (768 dims like BERT).
     */
    public void testHighDimensionalVectors() throws Exception {
        // Create two high-dimensional vectors
        StringBuilder json = new StringBuilder("[");

        // Create a unit vector along first dimension
        json.append("{ \"id\": \"doc1\", \"vector\": [1.0");
        for (int i = 1; i < 768; i++) {
            json.append(", 0.0");
        }
        json.append("] },");

        // Create a similar vector
        json.append("{ \"id\": \"doc2\", \"vector\": [0.9");
        for (int i = 1; i < 768; i++) {
            json.append(", 0.1");
        }
        json.append("] }");
        json.append("]");

        FakeLanceDataset dataset = createDataset(json.toString());
        assertThat(dataset.dims(), equalTo(768));

        // Create query vector
        float[] query = new float[768];
        query[0] = 1.0f;

        List<LanceDataset.Candidate> results = dataset.search(query, 2, "cosine");

        // doc1 should be first (identical to query in first dim, zeros elsewhere)
        assertThat(results.get(0).id(), equalTo("doc1"));
        assertThat((double) results.get(0).score(), closeTo(1.0, 0.0001));
    }
}
