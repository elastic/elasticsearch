/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;

public class DenseVectorScriptDocValuesTests extends ESTestCase {

    private static BinaryDocValues wrap(float[][] vectors, Version indexVersion) {
        return new BinaryDocValues() {
            int idx = -1;
            int maxIdx = vectors.length;
            @Override
            public BytesRef binaryValue() {
                if (idx >= maxIdx) {
                    throw new IllegalStateException("max index exceeded");
                }
                return DenseVectorFunctionTests.mockEncodeDenseVector(vectors[idx], indexVersion);
            }

            @Override
            public boolean advanceExact(int target) {
                idx = target;
                if (target < maxIdx) {
                    return true;
                }
                return false;
            }

            @Override
            public int docID() {
                return idx;
            }

            @Override
            public int nextDoc() {
                return idx++;
            }

            @Override
            public int advance(int target) {
                throw new IllegalArgumentException("not defined!");
            }

            @Override
            public long cost() {
                throw new IllegalArgumentException("not defined!");
            }
        };
    }

    public void testGetVectorValueAndGetMagnitude() throws IOException {
        final int dims = 3;
        float[][] vectors = {{ 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        float[] expectedMagnitudes = { 1.7320f, 2.4495f, 3.3166f };

        for (Version indexVersion : Arrays.asList(Version.V_7_4_0, Version.CURRENT)) {
            BinaryDocValues docValues = wrap(vectors, indexVersion);
            final DenseVectorScriptDocValues scriptDocValues = new DenseVectorScriptDocValues(docValues, indexVersion, dims);
            for (int i = 0; i < vectors.length; i++) {
                scriptDocValues.setNextDocId(i);
                assertArrayEquals(vectors[i], scriptDocValues.getVectorValue(), 0.0001f);
                assertEquals(expectedMagnitudes[i], scriptDocValues.getMagnitude(), 0.0001f);
            }
        }
    }

    public void testMissingValues() throws IOException {
        final int dims = 3;
        float[][] vectors = {{ 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        BinaryDocValues docValues = wrap(vectors, Version.CURRENT);
        final DenseVectorScriptDocValues scriptDocValues = new DenseVectorScriptDocValues(docValues, Version.CURRENT, dims);

        scriptDocValues.setNextDocId(3);
        Exception e = expectThrows(IllegalArgumentException.class, () -> scriptDocValues.getVectorValue());
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> scriptDocValues.getMagnitude());
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
    }

    public void testGetFunctionIsNotAccessible() throws IOException {
        final int dims = 3;
        float[][] vectors = {{ 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        BinaryDocValues docValues = wrap(vectors, Version.CURRENT);
        final DenseVectorScriptDocValues scriptDocValues = new DenseVectorScriptDocValues(docValues, Version.CURRENT, dims);

        scriptDocValues.setNextDocId(0);
        Exception e = expectThrows(UnsupportedOperationException.class, () -> scriptDocValues.get(0));
        assertThat(e.getMessage(), containsString("accessing a vector field's value through 'get' or 'value' is not supported!"));
    }
}
