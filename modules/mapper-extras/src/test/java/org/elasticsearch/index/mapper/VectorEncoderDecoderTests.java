/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

public class VectorEncoderDecoderTests extends ESTestCase {

    public void testDenseVectorEncodingDecoding() {
        int dimCount = randomIntBetween(0, 300);
        float[] expectedValues = new float[dimCount];
        for (int i = 0; i < dimCount; i++) {
            expectedValues[i] = randomFloat();
        }

        // test that values that went through encoding and decoding are equal to their original
        BytesRef encodedDenseVector =  mockEncodeDenseVector(expectedValues);
        float[] decodedValues = VectorEncoderDecoder.decodeDenseVector(encodedDenseVector);
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
        BytesRef encodedSparseVector = VectorEncoderDecoder.encodeSparseVector(expectedDims, expectedValues, dimCount);
        int[] decodedDims = VectorEncoderDecoder.decodeSparseVectorDims(encodedSparseVector);
        float[] decodedValues = VectorEncoderDecoder.decodeSparseVector(encodedSparseVector);
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
    private BytesRef mockEncodeDenseVector(float[] dims) {
        final short INT_BYTES = VectorEncoderDecoder.INT_BYTES;
        byte[] buf = new byte[INT_BYTES * dims.length];
        int offset = 0;
        int intValue;
        for (float value: dims) {
            intValue = Float.floatToIntBits(value);
            buf[offset] =  (byte) (intValue >> 24);
            buf[offset+1] = (byte) (intValue >> 16);
            buf[offset+2] = (byte) (intValue >>  8);
            buf[offset+3] = (byte) intValue;
            offset += INT_BYTES;
        }
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
