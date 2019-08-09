/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

public class VectorEncoderDecoderTests extends ESTestCase {

    public void testDenseVectorEncodingDecoding() {
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
        float[] decodedValues = VectorEncoderDecoder.decodeDenseVector(encodedDenseVector);
        float decodedMagnitude = VectorEncoderDecoder.decodeVectorMagnitude(encodedDenseVector);
        assertEquals(expectedMagnitude, decodedMagnitude, 0.001f);
        assertArrayEquals(
            "Decoded dense vector values are not equal to their original.",
            expectedValues,
            decodedValues,
            0.001f
        );
    }

    public void testSparseVectorEncodingDecoding() {
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
        BytesRef encodedSparseVector = VectorEncoderDecoder.encodeSparseVector(expectedDims, expectedValues, dimCount);
        int[] decodedDims = VectorEncoderDecoder.decodeSparseVectorDims(encodedSparseVector);
        float[] decodedValues = VectorEncoderDecoder.decodeSparseVector(encodedSparseVector);
        float decodedMagnitude = VectorEncoderDecoder.decodeVectorMagnitude(encodedSparseVector);
        assertEquals(expectedMagnitude, decodedMagnitude, 0.001f);
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
        byte[] buf = new byte[INT_BYTES * values.length  + INT_BYTES];
        int offset = 4;
        double dotProduct = 0f;
        int intValue;
        for (float value: values) {
            dotProduct += value * value;
            intValue = Float.floatToIntBits(value);
            buf[offset++] =  (byte) (intValue >> 24);
            buf[offset++] = (byte) (intValue >> 16);
            buf[offset++] = (byte) (intValue >>  8);
            buf[offset++] = (byte) intValue;
        }
        // encode vector magnitude at the beginning
        float vectorMagnitude = (float) Math.sqrt(dotProduct);
        int vectorMagnitudeIntValue = Float.floatToIntBits(vectorMagnitude);
        buf[0] = (byte) (vectorMagnitudeIntValue >> 24);
        buf[1] = (byte) (vectorMagnitudeIntValue >> 16);
        buf[2] = (byte) (vectorMagnitudeIntValue >>  8);
        buf[3] = (byte) vectorMagnitudeIntValue;

        return new BytesRef(buf, 0, offset);
    }

    // generate unique random dims
    private int[] randomUniqueDims(int dimCount) {
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
