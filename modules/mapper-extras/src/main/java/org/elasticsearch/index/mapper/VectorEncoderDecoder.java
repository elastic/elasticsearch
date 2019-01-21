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
import org.apache.lucene.util.InPlaceMergeSorter;

// static utility functions for encoding and decoding dense_vector and sparse_vector fields
final class VectorEncoderDecoder {
    static final byte INT_BYTES = 4;
    static final byte SHORT_BYTES = 2;

    private VectorEncoderDecoder() { }

    /**
     * Encodes a sparse array represented by values, dims and dimCount into a bytes array - BytesRef
     * BytesRef: int[] floats encoded as integers values, 2 bytes for each dimension
     * @param values - values of the sparse array
     * @param dims - dims of the sparse array
     * @param dimCount - number of the dimension
     * @return BytesRef
     */
    static BytesRef encodeSparseVector(int[] dims, float[] values, int dimCount) {
        // 1. Sort dims and values
        sortSparseDimsValues(dims, values, dimCount);
        byte[] buf = new byte[dimCount * (INT_BYTES + SHORT_BYTES)];

        // 2. Encode dimensions
        // as each dimension is a positive value that doesn't exceed 65535, 2 bytes is enough for encoding it
        int offset = 0;
        for (int dim = 0; dim < dimCount; dim++) {
            buf[offset] = (byte) (dims[dim] >>  8);
            buf[offset+1] = (byte) dims[dim];
            offset += SHORT_BYTES;
        }

        // 3. Encode values
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = Float.floatToIntBits(values[dim]);
            buf[offset] =  (byte) (intValue >> 24);
            buf[offset+1] = (byte) (intValue >> 16);
            buf[offset+2] = (byte) (intValue >>  8);
            buf[offset+3] = (byte) intValue;
            offset += INT_BYTES;
        }

        return new BytesRef(buf);
    }

    /**
     * Decodes the first part of BytesRef into sparse vector dimensions
     * @param vectorBR - vector decoded in BytesRef
     */
    static int[] decodeSparseVectorDims(BytesRef vectorBR) {
        int dimCount = vectorBR.length / (INT_BYTES + SHORT_BYTES);
        int[] dims = new int[dimCount];
        int offset = vectorBR.offset;
        for (int dim = 0; dim < dimCount; dim++) {
            dims[dim] = ((vectorBR.bytes[offset] & 0xFF) << 8) | (vectorBR.bytes[offset+1] & 0xFF);
            offset += SHORT_BYTES;
        }
        return dims;
    }

    /**
     * Decodes the second part of the BytesRef into sparse vector values
     * @param vectorBR - vector decoded in BytesRef
     */
    static float[] decodeSparseVector(BytesRef vectorBR) {
        int dimCount = vectorBR.length / (INT_BYTES + SHORT_BYTES);
        int offset =  vectorBR.offset + SHORT_BYTES * dimCount; //calculate the offset from where values are encoded
        float[] vector = new float[dimCount];
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = ((vectorBR.bytes[offset] & 0xFF) << 24)   |
                ((vectorBR.bytes[offset+1] & 0xFF) << 16) |
                ((vectorBR.bytes[offset+2] & 0xFF) <<  8) |
                (vectorBR.bytes[offset+3] & 0xFF);
            vector[dim] = Float.intBitsToFloat(intValue);
            offset = offset + INT_BYTES;
        }
        return vector;
    }


    /**
    Sort dimensions in the ascending order and
    sort values in the same order as their corresponding dimensions
    **/
    static void sortSparseDimsValues(int[] dims, float[] values, int n) {
        new InPlaceMergeSorter() {
            @Override
            public int compare(int i, int j) {
                return Integer.compare(dims[i], dims[j]);
            }

            @Override
            public void swap(int i, int j) {
                int tempDim = dims[i];
                dims[i] = dims[j];
                dims[j] = tempDim;

                float tempValue = values[j];
                values[j] = values[i];
                values[i] = tempValue;
            }
        }.sort(0, n);
    }

    // Decodes a BytesRef into an array of floats
    static float[] decodeDenseVector(BytesRef vectorBR) {
        int dimCount = vectorBR.length / INT_BYTES;
        float[] vector = new float[dimCount];
        int offset = vectorBR.offset;
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = ((vectorBR.bytes[offset] & 0xFF) << 24)   |
                ((vectorBR.bytes[offset+1] & 0xFF) << 16) |
                ((vectorBR.bytes[offset+2] & 0xFF) <<  8) |
                (vectorBR.bytes[offset+3] & 0xFF);
            vector[dim] = Float.intBitsToFloat(intValue);
            offset = offset + INT_BYTES;
        }
        return vector;
    }
}
