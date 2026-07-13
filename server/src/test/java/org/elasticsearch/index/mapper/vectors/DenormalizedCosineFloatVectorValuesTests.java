/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.VectorScorer;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.elasticsearch.index.mapper.vectors.KnnDenseVectorScriptDocValuesTests.wrap;

public class DenormalizedCosineFloatVectorValuesTests extends ESTestCase {

    public void testEmptyVectors() throws IOException {
        DenormalizedCosineFloatVectorValues normalizedCosineFloatVectorValues = new DenormalizedCosineFloatVectorValues(
            wrap(new float[0][0]),
            wrapMagnitudes(new float[0])
        );
        assertEquals(NO_MORE_DOCS, normalizedCosineFloatVectorValues.iterator().nextDoc());
    }

    public void testRandomVectors() throws IOException {
        int dims = randomIntBetween(64, 2048);
        int numVectors = randomIntBetween(1, 24);
        float[][] vectors = new float[numVectors][];
        float[][] normalizedVectors = new float[numVectors][];
        float[] magnitudes = new float[numVectors];
        for (int i = 0; i < numVectors; i++) {
            float[] vector = new float[dims];
            float mag = randomVector(vector);
            magnitudes[i] = mag;
            vectors[i] = vector;
            normalizedVectors[i] = copyAndNormalize(vector, mag);
        }

        DenormalizedCosineFloatVectorValues normalizedCosineFloatVectorValues = new DenormalizedCosineFloatVectorValues(
            wrap(normalizedVectors),
            wrapMagnitudes(magnitudes)
        );

        KnnVectorValues.DocIndexIterator iterator = normalizedCosineFloatVectorValues.iterator();
        for (int i = 0; i < numVectors; i++) {
            assertEquals(i, iterator.advance(i));
            assertArrayEquals(vectors[i], normalizedCosineFloatVectorValues.vectorValue(iterator.index()), (float) 1e-6);
            assertEquals(magnitudes[i], normalizedCosineFloatVectorValues.magnitude(), (float) 1e-6);
        }

    }

    public static float[] copyAndNormalize(float[] in, float mag) {
        float[] copy = Arrays.copyOf(in, in.length);
        for (int i = 0; i < copy.length; i++) {
            copy[i] = copy[i] / mag;
        }
        return copy;
    }

    private static float randomVector(float[] in) {
        float magnitude = 0f;
        for (int i = 0; i < in.length; i++) {
            float v = randomFloat() * randomIntBetween(1, 5);
            in[i] = v;
            magnitude += v * v;
        }
        return (float) Math.sqrt(magnitude);
    }

    public static NumericDocValues wrapMagnitudes(float[] magnitudes) {
        return new NumericDocValues() {
            int index = -1;

            @Override
            public long longValue() throws IOException {
                return Float.floatToRawIntBits(magnitudes[index]);
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return advance(target) != NO_MORE_DOCS;
            }

            @Override
            public int docID() {
                return index;
            }

            @Override
            public int nextDoc() {
                return advance(index + 1);
            }

            @Override
            public int advance(int target) {
                if (target >= magnitudes.length) {
                    return NO_MORE_DOCS;
                }
                return index = target;
            }

            @Override
            public long cost() {
                return magnitudes.length;
            }
        };
    }

    public void testOrdToDocWithSparseVectors() throws IOException {
        // This test simulates a real-world scenario where some documents don't have vector fields
        // After force merge, the ord (ordinal) and docId mapping becomes crucial

        // Simulate a scenario where we have 6 documents, but only documents 1, 3, 4 have vectors
        // Doc 0: no vector
        // Doc 1: vector [0.6, 0.8, 0.0, 0.0], magnitude 5.0 -> original [3.0, 4.0, 0.0, 0.0]
        // Doc 2: no vector
        // Doc 3: vector [1.0, 0.0, 0.0, 0.0], magnitude 2.0 -> original [2.0, 0.0, 0.0, 0.0]
        // Doc 4: vector [0.0, 0.0, 0.6, 0.8], magnitude 10.0 -> original [0.0, 0.0, 6.0, 8.0]
        // Doc 5: no vector

        // After merge, the vector ordinals will be 0, 1, 2 but they correspond to docIds 1, 3, 4
        int totalDocs = 6;
        int[] docIdsWithVectors = { 1, 3, 4 }; // Document IDs that have vectors
        int numVectors = docIdsWithVectors.length;

        float[][] normalizedVectors = new float[numVectors][];
        float[] magnitudes = new float[numVectors];

        normalizedVectors[0] = new float[] { 0.6f, 0.8f, 0.0f, 0.0f }; // Doc 1
        magnitudes[0] = 5.0f;

        normalizedVectors[1] = new float[] { 1.0f, 0.0f, 0.0f, 0.0f }; // Doc 3
        magnitudes[1] = 2.0f;

        normalizedVectors[2] = new float[] { 0.0f, 0.0f, 0.6f, 0.8f }; // Doc 4
        magnitudes[2] = 10.0f;

        // Expected original vectors after denormalization
        float[][] expectedVectors = new float[numVectors][];
        expectedVectors[0] = new float[] { 3.0f, 4.0f, 0.0f, 0.0f }; // Doc 1
        expectedVectors[1] = new float[] { 2.0f, 0.0f, 0.0f, 0.0f }; // Doc 3
        expectedVectors[2] = new float[] { 0.0f, 0.0f, 6.0f, 8.0f }; // Doc 4

        // Create a custom FloatVectorValues that simulates post-merge sparse vector scenario
        FloatVectorValues sparseVectorValues = new FloatVectorValues() {
            @Override
            public int dimension() {
                return 4;
            }

            @Override
            public int size() {
                return numVectors;
            }

            @Override
            public DocIndexIterator iterator() {
                return new DocIndexIterator() {
                    private int index = -1;

                    @Override
                    public int docID() {
                        return index;
                    }

                    @Override
                    public int index() {
                        return index;
                    }

                    @Override
                    public int nextDoc() {
                        return advance(index + 1);
                    }

                    @Override
                    public int advance(int target) {
                        if (target >= numVectors) return NO_MORE_DOCS;
                        return index = target;
                    }

                    @Override
                    public long cost() {
                        return numVectors;
                    }
                };
            }

            @Override
            public FloatVectorValues copy() {
                throw new UnsupportedOperationException();
            }

            @Override
            public VectorScorer scorer(float[] floats) {
                throw new UnsupportedOperationException();
            }

            // This is the key method - it maps ordinals to actual document IDs
            @Override
            public int ordToDoc(int ord) {
                // ord 0 -> docId 1, ord 1 -> docId 3, ord 2 -> docId 4
                return docIdsWithVectors[ord];
            }

            @Override
            public float[] vectorValue(int ord) {
                return normalizedVectors[ord];
            }
        };

        // Create magnitudes that correspond to the actual document IDs
        NumericDocValues sparseMagnitudes = new NumericDocValues() {
            private int docId = -1;

            @Override
            public long longValue() {
                // Find which vector index corresponds to this docId
                for (int i = 0; i < docIdsWithVectors.length; i++) {
                    if (docIdsWithVectors[i] == docId) {
                        return Float.floatToRawIntBits(magnitudes[i]);
                    }
                }
                return Float.floatToRawIntBits(1.0f); // Default magnitude
            }

            @Override
            public boolean advanceExact(int target) {
                docId = target;
                // Check if this docId has a vector
                for (int vectorDocId : docIdsWithVectors) {
                    if (vectorDocId == target) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public int docID() {
                return docId;
            }

            @Override
            public int nextDoc() {
                return advance(docId + 1);
            }

            @Override
            public int advance(int target) {
                for (int vectorDocId : docIdsWithVectors) {
                    if (vectorDocId >= target) {
                        docId = vectorDocId;
                        return docId;
                    }
                }
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return totalDocs;
            }
        };

        // Test the fixed version (with ordToDoc)
        DenormalizedCosineFloatVectorValues vectorValues = new DenormalizedCosineFloatVectorValues(sparseVectorValues, sparseMagnitudes);

        // Test that ordToDoc method properly maps ordinals to document IDs
        KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();

        for (int ord = 0; ord < numVectors; ord++) {
            iterator.advance(ord);

            // Verify that ordToDoc works correctly
            int expectedDocId = docIdsWithVectors[ord];
            int actualDocId = vectorValues.ordToDoc(ord);
            assertEquals("ordToDoc should correctly map ord " + ord + " to docId " + expectedDocId, expectedDocId, actualDocId);

            // Get the denormalized vector - this relies on ordToDoc working correctly
            float[] actualVector = vectorValues.vectorValue(iterator.index());
            float actualMagnitude = vectorValues.magnitude();

            // Verify the denormalized vector is correct
            assertArrayEquals(
                "Vector at ord " + ord + " (docId " + expectedDocId + ") should be correctly denormalized",
                expectedVectors[ord],
                actualVector,
                1e-6f
            );

            // Verify the magnitude is correct
            assertEquals(
                "Magnitude at ord " + ord + " (docId " + expectedDocId + ") should be correct",
                magnitudes[ord],
                actualMagnitude,
                1e-6f
            );
        }
    }

}
