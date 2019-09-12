/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

public class VectorEncoderDecoderTests extends ESTestCase {

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
        float decodedMagnitude = VectorEncoderDecoder.decodeVectorMagnitude(indexVersion, encodedSparseVector);
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

    public void testSparseVectorEncodingDecodingBefore_V_7_5_0() {
        Version indexVersion = Version.V_7_4_0;
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
    public static BytesRef mockEncodeDenseVector(float[] values, Version indexVersion) {
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
