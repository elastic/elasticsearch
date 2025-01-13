/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.NumericDocValues;
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

}
