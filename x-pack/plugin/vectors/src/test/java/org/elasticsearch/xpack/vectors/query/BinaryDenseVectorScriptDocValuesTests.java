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
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;

public class BinaryDenseVectorScriptDocValuesTests extends ESTestCase {

    public void testGetVectorValueAndGetMagnitude() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        float[] expectedMagnitudes = { 1.7320f, 2.4495f, 3.3166f };

        for (Version indexVersion : Arrays.asList(Version.V_7_4_0, Version.CURRENT)) {
            BinaryDocValues docValues = wrap(vectors, indexVersion);
            BinaryDenseVectorDocValuesField field = new BinaryDenseVectorDocValuesField(docValues, "test", dims, indexVersion);
            DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();
            for (int i = 0; i < vectors.length; i++) {
                field.setNextDocId(i);
                assertEquals(1, field.size());
                assertEquals(dims, scriptDocValues.dims());
                assertArrayEquals(vectors[i], scriptDocValues.getVectorValue(), 0.0001f);
                assertEquals(expectedMagnitudes[i], scriptDocValues.getMagnitude(), 0.0001f);
            }
        }
    }

    public void testMetadataAndIterator() throws IOException {
        int dims = 3;
        Version indexVersion = Version.CURRENT;
        float[][] vectors = fill(new float[randomIntBetween(1, 5)][dims]);
        BinaryDocValues docValues = wrap(vectors, indexVersion);
        BinaryDenseVectorDocValuesField field = new BinaryDenseVectorDocValuesField(docValues, "test", dims, indexVersion);
        for (int i = 0; i < vectors.length; i++) {
            field.setNextDocId(i);
            DenseVector dv = field.get();
            assertEquals(1, dv.size());
            assertFalse(dv.isEmpty());
            assertEquals(dims, dv.getDims());
            UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, field::iterator);
            assertEquals("Cannot iterate over single valued dense_vector field, use get() instead", e.getMessage());
        }
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
        BinaryDocValues docValues = wrap(vectors, Version.CURRENT);
        BinaryDenseVectorDocValuesField field = new BinaryDenseVectorDocValuesField(docValues, "test", dims, Version.CURRENT);
        DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

        field.setNextDocId(3);
        assertEquals(0, field.size());
        Exception e = expectThrows(IllegalArgumentException.class, scriptDocValues::getVectorValue);
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, scriptDocValues::getMagnitude);
        assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
    }

    public void testGetFunctionIsNotAccessible() throws IOException {
        int dims = 3;
        float[][] vectors = { { 1, 1, 1 }, { 1, 1, 2 }, { 1, 1, 3 } };
        BinaryDocValues docValues = wrap(vectors, Version.CURRENT);
        BinaryDenseVectorDocValuesField field = new BinaryDenseVectorDocValuesField(docValues, "test", dims, Version.CURRENT);
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

        for (Version indexVersion : Arrays.asList(Version.V_7_4_0, Version.CURRENT)) {
            BinaryDocValues docValues = wrap(new float[][] { docVector }, indexVersion);
            BinaryDenseVectorDocValuesField field = new BinaryDenseVectorDocValuesField(docValues, "test", dims, indexVersion);
            DenseVectorScriptDocValues scriptDocValues = field.toScriptDocValues();

            field.setNextDocId(0);

            assertEquals(
                "dotProduct result is not equal to the expected value!",
                65425.624,
                scriptDocValues.dotProduct(queryVector),
                0.001
            );
            assertEquals("l1norm result is not equal to the expected value!", 485.184, scriptDocValues.l1Norm(queryVector), 0.001);
            assertEquals("l2norm result is not equal to the expected value!", 301.361, scriptDocValues.l2Norm(queryVector), 0.001);
        }
    }

    static BinaryDocValues wrap(float[][] vectors, Version indexVersion) {
        return new BinaryDocValues() {
            int idx = -1;
            int maxIdx = vectors.length;

            @Override
            public BytesRef binaryValue() {
                if (idx >= maxIdx) {
                    throw new IllegalStateException("max index exceeded");
                }
                return mockEncodeDenseVector(vectors[idx], indexVersion);
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

    static BytesRef mockEncodeDenseVector(float[] values, Version indexVersion) {
        byte[] bytes = indexVersion.onOrAfter(Version.V_7_5_0)
            ? new byte[VectorEncoderDecoder.INT_BYTES * values.length + VectorEncoderDecoder.INT_BYTES]
            : new byte[VectorEncoderDecoder.INT_BYTES * values.length];
        double dotProduct = 0f;

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        for (float value : values) {
            byteBuffer.putFloat(value);
            dotProduct += value * value;
        }

        if (indexVersion.onOrAfter(Version.V_7_5_0)) {
            // encode vector magnitude at the end
            float vectorMagnitude = (float) Math.sqrt(dotProduct);
            byteBuffer.putFloat(vectorMagnitude);
        }
        return new BytesRef(bytes);
    }

}
