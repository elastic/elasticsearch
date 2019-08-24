/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

public class VectorEncoderDecoderTests extends ESTestCase {

    public void testDenseVectorEncodingDecoding() {
        Version indexVersion = Version.CURRENT;
        int dimCount = randomIntBetween(0, DenseVectorFieldMapper.MAX_DIMS_COUNT);
        float[] expectedValues = new float[dimCount];
        double dotProduct = 0f;
        for (int i = 0; i < dimCount; i++) {
            expectedValues[i] = randomFloat();
            dotProduct += expectedValues[i] * expectedValues[i];
        }
        float expectedMagnitude = (float) Math.sqrt(dotProduct);

        // test that values that went through encoding and decoding are equal to their original
        BytesRef encodedDenseVector = mockEncodeDenseVector(expectedValues);
        float[] decodedValues = VectorEncoderDecoder.decodeDenseVector(indexVersion, encodedDenseVector);
        float decodedMagnitude = VectorEncoderDecoder.getVectorMagnitude(indexVersion, encodedDenseVector, decodedValues);
        assertEquals(expectedMagnitude, decodedMagnitude, 0.0f);
        assertArrayEquals(
            "Decoded dense vector values are not equal to their original.",
            expectedValues,
            decodedValues,
            0.001f
        );
    }

    public void testDenseVectorEncodingDecodingBefore7_4() {
        Version indexVersion = Version.V_7_3_0;
        int dimCount = randomIntBetween(0, DenseVectorFieldMapper.MAX_DIMS_COUNT);
        float[] expectedValues = new float[dimCount];
        for (int i = 0; i < dimCount; i++) {
            expectedValues[i] = randomFloat();
        }
        // test that values that went through encoding and decoding are equal to their original
        BytesRef encodedDenseVector = mockEncodeDenseVectorBefore7_4(expectedValues);
        float[] decodedValues = VectorEncoderDecoder.decodeDenseVector(indexVersion, encodedDenseVector);
        assertArrayEquals(
            "Decoded dense vector values are not equal to their original.",
            expectedValues,
            decodedValues,
            0.001f
        );
    }

    public void testSparseVectorEncodingDecoding() {
        Version indexVersion = Version.CURRENT;
        int dimCount = randomIntBetween(0, 100);
        float[] expectedValues = new float[dimCount];
        int[] expectedDims = randomUniqueDims(dimCount);
        double dotProduct = 0.0f;
        for (int i = 0; i < dimCount; i++) {
            expectedValues[i] = randomFloat();
            dotProduct += expectedValues[i] * expectedValues[i];
        }
        float expectedMagnitude = (float) Math.sqrt(dotProduct);

        // test that sorting in the encoding works as expected
        int[] sortedDims = Arrays.copyOf(expectedDims, dimCount);
        Arrays.sort(sortedDims);
        VectorEncoderDecoder.sortSparseDimsValues(expectedDims, expectedValues, dimCount);
        assertArrayEquals(
            "Sparse vector dims are not properly sorted!",
            sortedDims,
            expectedDims
        );

        // test that values that went through encoding and decoding are equal to their original
        BytesRef encodedSparseVector = VectorEncoderDecoder.encodeSparseVector(indexVersion, expectedDims, expectedValues, dimCount);
        int[] decodedDims = VectorEncoderDecoder.decodeSparseVectorDims(indexVersion, encodedSparseVector);
        float[] decodedValues = VectorEncoderDecoder.decodeSparseVector(indexVersion, encodedSparseVector);
        float decodedMagnitude = VectorEncoderDecoder.getVectorMagnitude(indexVersion, encodedSparseVector, decodedValues);
        assertEquals(expectedMagnitude, decodedMagnitude, 0.0f);
        assertArrayEquals(
            "Decoded sparse vector dims are not equal to their original!",
            expectedDims,
            decodedDims
        );
        assertArrayEquals(
            "Decoded sparse vector values are not equal to their original.",
            expectedValues,
            decodedValues,
            0.001f
        );
    }

    public void testSparseVectorEncodingDecodingBefore7_4() {
        Version indexVersion = Version.V_7_3_0;
        int dimCount = randomIntBetween(0, 100);
        float[] expectedValues = new float[dimCount];
        int[] expectedDims = randomUniqueDims(dimCount);
        for (int i = 0; i < dimCount; i++) {
            expectedValues[i] = randomFloat();
        }

        // test that sorting in the encoding works as expected
        int[] sortedDims = Arrays.copyOf(expectedDims, dimCount);
        Arrays.sort(sortedDims);
        VectorEncoderDecoder.sortSparseDimsValues(expectedDims, expectedValues, dimCount);
        assertArrayEquals(
            "Sparse vector dims are not properly sorted!",
            sortedDims,
            expectedDims
        );

        // test that values that went through encoding and decoding are equal to their original
        BytesRef encodedSparseVector = VectorEncoderDecoder.encodeSparseVector(indexVersion, expectedDims, expectedValues, dimCount);
        int[] decodedDims = VectorEncoderDecoder.decodeSparseVectorDims(indexVersion, encodedSparseVector);
        float[] decodedValues = VectorEncoderDecoder.decodeSparseVector(indexVersion, encodedSparseVector);
        assertArrayEquals(
            "Decoded sparse vector dims are not equal to their original!",
            expectedDims,
            decodedDims
        );
        assertArrayEquals(
            "Decoded sparse vector values are not equal to their original.",
            expectedValues,
            decodedValues,
            0.001f
        );
    }

    // imitates the code in DenseVectorFieldMapper::parse
    public static BytesRef mockEncodeDenseVector(float[] values) {
        final short INT_BYTES = VectorEncoderDecoder.INT_BYTES;
        byte[] buf = new byte[INT_BYTES * values.length + INT_BYTES];
        int offset = 0;
        double dotProduct = 0f;
        int intValue;
        for (float value: values) {
            dotProduct += value * value;
            intValue = Float.floatToIntBits(value);
            buf[offset++] = (byte) (intValue >> 24);
            buf[offset++] = (byte) (intValue >> 16);
            buf[offset++] = (byte) (intValue >>  8);
            buf[offset++] = (byte) intValue;
        }
        // encode vector magnitude at the end
        float vectorMagnitude = (float) Math.sqrt(dotProduct);
        int vectorMagnitudeIntValue = Float.floatToIntBits(vectorMagnitude);
        buf[offset++] = (byte) (vectorMagnitudeIntValue >> 24);
        buf[offset++] = (byte) (vectorMagnitudeIntValue >> 16);
        buf[offset++] = (byte) (vectorMagnitudeIntValue >>  8);
        buf[offset++] = (byte) vectorMagnitudeIntValue;

        return new BytesRef(buf);
    }

    // imitates the code in DenseVectorFieldMapper::parse before version 7.4
    public static BytesRef mockEncodeDenseVectorBefore7_4(float[] values) {
        final short INT_BYTES = VectorEncoderDecoder.INT_BYTES;
        byte[] buf = new byte[INT_BYTES * values.length];
        int offset = 0;
        int intValue;
        for (float value: values) {
            intValue = Float.floatToIntBits(value);
            buf[offset++] = (byte) (intValue >> 24);
            buf[offset++] = (byte) (intValue >> 16);
            buf[offset++] = (byte) (intValue >> 8);
            buf[offset++] = (byte) intValue;
        }
        return new BytesRef(buf, 0, offset);
    }

    // generate unique random dims
    private static int[] randomUniqueDims(int dimCount) {
        int[] values = new int[dimCount];
        Set<Integer> usedValues = new HashSet<>();
        int value;
        for (int i = 0; i < dimCount; i++) {
            value = randomValueOtherThanMany(usedValues::contains, () -> randomIntBetween(0, SparseVectorFieldMapper.MAX_DIMS_NUMBER));
            usedValues.add(value);
            values[i] = value;
        }
        return values;
    }

}
