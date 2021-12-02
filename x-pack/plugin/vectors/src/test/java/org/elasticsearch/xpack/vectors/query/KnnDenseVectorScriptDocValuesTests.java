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
import org.elasticsearch.xpack.vectors.query.KnnDenseVectorScriptDocValues.KnnDenseVectorSupplier;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class KnnDenseVectorScriptDocValuesTests extends ESTestCase {

    public void testGetVectorValueAndGetMagnitude() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        float[] expectedMagnitudes = { 1.7320f, 2.4495f, 3.3166f };

        KnnDenseVectorSupplier supplier = new KnnDenseVectorSupplier(wrap(vectors));
        DenseVectorScriptDocValues scriptDocValues = new KnnDenseVectorScriptDocValues(supplier, dims);
        for (int i = 0; i < vectors.length; i++) {
            supplier.setNextDocId(i);
            assertArrayEquals(vectors[i], scriptDocValues.getVectorValue(), 0.0001f);
            assertEquals(expectedMagnitudes[i], scriptDocValues.getMagnitude(), 0.0001f);
        }
    }

    public void testMissingValues() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        KnnDenseVectorSupplier supplier = new KnnDenseVectorSupplier(wrap(vectors));
        DenseVectorScriptDocValues scriptDocValues = new KnnDenseVectorScriptDocValues(supplier, dims);

        supplier.setNextDocId(3);
        Exception e = expectThrows(IllegalArgumentException.class, () -> scriptDocValues.getVectorValue());
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> scriptDocValues.getMagnitude());
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
    }

    public void testGetFunctionIsNotAccessible() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        KnnDenseVectorSupplier supplier = new KnnDenseVectorSupplier(wrap(vectors));
        DenseVectorScriptDocValues scriptDocValues = new KnnDenseVectorScriptDocValues(supplier, dims);

        supplier.setNextDocId(0);
        Exception e = expectThrows(UnsupportedOperationException.class, () -> scriptDocValues.get(0));
        assertThat(e.getMessage(), containsString("accessing a vector field's value through 'get' or 'value' is not supported!"));
    }

    public void testSimilarityFunctions() throws IOException {
        int dims = 5;
        float[] docVector = new float[] { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f };
        float[] queryVector = new float[] { 0.5f, 111.3f, -13.0f, 14.8f, -156.0f };

        KnnDenseVectorSupplier supplier = new KnnDenseVectorSupplier(wrap(new float[][] { docVector }));
        DenseVectorScriptDocValues scriptDocValues = new KnnDenseVectorScriptDocValues(supplier, dims);
        supplier.setNextDocId(0);

        assertEquals("dotProduct result is not equal to the expected value!", 65425.624, scriptDocValues.dotProduct(queryVector), 0.001);
        assertEquals("l1norm result is not equal to the expected value!", 485.184, scriptDocValues.l1Norm(queryVector), 0.001);
        assertEquals("l2norm result is not equal to the expected value!", 301.361, scriptDocValues.l2Norm(queryVector), 0.001);
    }

    private static VectorValues wrap(float[][] vectors) {
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
