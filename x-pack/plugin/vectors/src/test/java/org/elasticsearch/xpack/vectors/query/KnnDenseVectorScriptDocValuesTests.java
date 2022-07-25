/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.VectorValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class KnnDenseVectorScriptDocValuesTests extends ESTestCase {

    public void testGetVectorValueAndGetMagnitude() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        float[] expectedMagnitudes = { 1.7320f, 2.4495f, 3.3166f };

        DenseVectorDocValuesField field = new KnnDenseVectorDocValuesField(wrap(vectors), "test", dims);
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            assertEquals(1, field.size());
            assertEquals(dims, scriptDocValues.dims());
            assertArrayEquals(vectors[i], scriptDocValues.getVectorValue(), 0.0001f);
            assertEquals(expectedMagnitudes[i], scriptDocValues.getMagnitude(), 0.0001f);
        }
    }

    public void testMetadataAndIterator() throws IOException {
        int dims = 3;
        float[][] vectors = fill(new float[randomIntBetween(1, 5)][dims]);
        KnnDenseVectorDocValuesField field = new KnnDenseVectorDocValuesField(wrap(vectors), "test", dims);
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            DenseVector dv = field.get();
            assertEquals(1, dv.size());
            assertFalse(dv.isEmpty());
            assertEquals(dims, dv.getDims());
            UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, field::iterator);
            assertEquals("Cannot iterate over single valued dense_vector field, use get() instead", e.getMessage());
        }
        assertEquals(1, field.size());
        field.setNextDocId(vectors.length);
        DenseVector dv = field.get();
        assertEquals(dv, DenseVector.EMPTY);
    }

    protected float[][] fill(float[][] vectors) {
        for (float[] vector : vectors) {
            for (int i = 0; i < vector.length; i++) {
                vector[i] = randomFloat();
            }
        }
        return vectors;
    }

    public void testMissingValues() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        DenseVectorDocValuesField field = new KnnDenseVectorDocValuesField(wrap(vectors), "test", dims);
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(3);
        Exception e = expectThrows(IllegalArgumentException.class, () -> scriptDocValues.getVectorValue());
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> scriptDocValues.getMagnitude());
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
    }

    public void testGetFunctionIsNotAccessible() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        DenseVectorDocValuesField field = new KnnDenseVectorDocValuesField(wrap(vectors), "test", dims);
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(0);
        Exception e = expectThrows(UnsupportedOperationException.class, () -> scriptDocValues.get(0));
        assertThat(
            e.getMessage(),
            containsString(
                "accessing a vector field's value through 'get' or 'value' is not supported, use 'vectorValue' or 'magnitude' instead."
            )
        );
    }

    public void testSimilarityFunctions() throws IOException {
        int dims = 5;
        float[] docVector = new float[] { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f };
        float[] queryVector = new float[] { 0.5f, 111.3f, -13.0f, 14.8f, -156.0f };

        DenseVectorDocValuesField field = new KnnDenseVectorDocValuesField(wrap(new float[][] { docVector }), "test", dims);
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();
        field.setNextDocId(0);

        assertEquals("dotProduct result is not equal to the expected value!", 65425.624, scriptDocValues.dotProduct(queryVector), 0.001);
        assertEquals("l1norm result is not equal to the expected value!", 485.184, scriptDocValues.l1Norm(queryVector), 0.001);
        assertEquals("l2norm result is not equal to the expected value!", 301.361, scriptDocValues.l2Norm(queryVector), 0.001);
    }

    public void testMissingVectorValues() throws IOException {
        int dims = 7;
        KnnDenseVectorDocValuesField emptyKnn = new KnnDenseVectorDocValuesField(null, "test", dims);

        emptyKnn.setNextDocId(0);
        assertEquals(0, emptyKnn.toScriptDocValues().size());
        assertTrue(emptyKnn.toScriptDocValues().isEmpty());
        assertEquals(DenseVector.EMPTY, emptyKnn.get());
        assertNull(emptyKnn.get(null));
        assertNull(emptyKnn.getInternal());
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, emptyKnn::iterator);
        assertEquals("Cannot iterate over single valued dense_vector field, use get() instead", e.getMessage());
    }

    static VectorValues wrap(float[][] vectors) {
        return new VectorValues() {
            int index = 0;

            @Override
            public int dimension() {
                return 0;
            }

            @Override
            public int size() {
                return vectors.length;
            }

            @Override
            public float[] vectorValue() {
                return vectors[index];
            }

            @Override
            public BytesRef binaryValue() {
                // This value is never inspected, it's only used to check if the document has a vector
                return new BytesRef();
            }

            @Override
            public int docID() {
                return index;
            }

            @Override
            public int nextDoc() {
                throw new UnsupportedOperationException();
            }

            @Override
            public int advance(int target) {
                if (target >= size()) {
                    return NO_MORE_DOCS;
                }
                return index = target;
            }

            @Override
            public long cost() {
                return size();
            }
        };
    }
}
