/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.Version;

import java.nio.ByteBuffer;

// static utility functions for encoding and decoding dense_vector and sparse_vector fields
public final class VectorEncoderDecoder {
    static final byte INT_BYTES = 4;
    static final byte SHORT_BYTES = 2;

    private VectorEncoderDecoder() { }

    /**
     * Encodes a sparse array represented by values, dims and dimCount into a bytes array - BytesRef
     * BytesRef: int[] floats encoded as integers values, 2 bytes for each dimension, length of vector
     * @param indexVersion - index version
     * @param dims - dims of the sparse array
     * @param values - values of the sparse array
     * @param dimCount - number of the dimensions, necessary as values and dims are dynamically created arrays,
     *          and may be over-allocated
     * @return BytesRef
     */
    public static BytesRef encodeSparseVector(Version indexVersion, int[] dims, float[] values, int dimCount) {
        // 1. Sort dims and values
        sortSparseDimsValues(dims, values, dimCount);

        // 2. Encode dimensions
        // as each dimension is a positive value that doesn't exceed 65535, 2 bytes is enough for encoding it
        byte[] bytes = indexVersion.onOrAfter(Version.V_7_5_0) ? new byte[dimCount * (INT_BYTES + SHORT_BYTES) + INT_BYTES] :
            new byte[dimCount * (INT_BYTES + SHORT_BYTES)];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        for (int dim = 0; dim < dimCount; dim++) {
            int dimValue = dims[dim];
            byteBuffer.put((byte) (dimValue >> 8));
            byteBuffer.put((byte) dimValue);
        }

        // 3. Encode values
        double dotProduct = 0.0f;
        for (int dim = 0; dim < dimCount; dim++) {
            float value = values[dim];
            byteBuffer.putFloat(value);
            dotProduct += value * value;
        }

        // 4. Encode vector magnitude at the end
        if (indexVersion.onOrAfter(Version.V_7_5_0)) {
            float vectorMagnitude = (float) Math.sqrt(dotProduct);
            byteBuffer.putFloat(vectorMagnitude);
        }

        return new BytesRef(bytes);
    }

    /**
     * Decodes the first part of BytesRef into sparse vector dimensions
     * @param indexVersion - index version
     * @param vectorBR - sparse vector encoded in BytesRef
     */
    public static int[] decodeSparseVectorDims(Version indexVersion, BytesRef vectorBR) {
        int dimCount = indexVersion.onOrAfter(Version.V_7_5_0)
            ? (vectorBR.length - INT_BYTES) / (INT_BYTES + SHORT_BYTES)
            : vectorBR.length / (INT_BYTES + SHORT_BYTES);
        ByteBuffer byteBuffer = ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, dimCount * SHORT_BYTES);

        int[] dims = new int[dimCount];
        for (int dim = 0; dim < dimCount; dim++) {
            dims[dim] = ((byteBuffer.get() & 0xFF) << 8) | (byteBuffer.get() & 0xFF);
        }
        return dims;
    }

    /**
     * Decodes the second part of the BytesRef into sparse vector values
     * @param indexVersion - index version
     * @param vectorBR - sparse vector encoded in BytesRef
     */
    public static float[] decodeSparseVector(Version indexVersion, BytesRef vectorBR) {
        int dimCount = indexVersion.onOrAfter(Version.V_7_5_0)
            ? (vectorBR.length - INT_BYTES) / (INT_BYTES + SHORT_BYTES)
            : vectorBR.length / (INT_BYTES + SHORT_BYTES);
        int offset =  vectorBR.offset + SHORT_BYTES * dimCount;
        float[] vector = new float[dimCount];

        ByteBuffer byteBuffer = ByteBuffer.wrap(vectorBR.bytes, offset, dimCount * INT_BYTES);
        for (int dim = 0; dim < dimCount; dim++) {
            vector[dim] = byteBuffer.getFloat();
        }
        return vector;
    }

    /**
     * Sorts dimensions in the ascending order and
     * sorts values in the same order as their corresponding dimensions
     *
     * @param dims - dimensions of the sparse query vector
     * @param values - values for the sparse query vector
     * @param n - number of dimensions
     */
    public static void sortSparseDimsValues(int[] dims, float[] values, int n) {
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

    /**
     * Sorts dimensions in the ascending order and
     * sorts values in the same order as their corresponding dimensions
     *
     * @param dims - dimensions of the sparse query vector
     * @param values - values for the sparse query vector
     * @param n - number of dimensions
     */
    public static void sortSparseDimsFloatValues(int[] dims, float[] values, int n) {
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

    public static int denseVectorLength(Version indexVersion, BytesRef vectorBR) {
        return indexVersion.onOrAfter(Version.V_7_5_0)
            ? (vectorBR.length - INT_BYTES) / INT_BYTES
            : vectorBR.length / INT_BYTES;
    }

    /**
     * Decodes the last 4 bytes of the encoded vector, which contains the vector magnitude.
     * NOTE: this function can only be called on vectors from an index version greater than or
     * equal to 7.5.0, since vectors created prior to that do not store the magnitude.
     */
    public static float decodeVectorMagnitude(Version indexVersion, BytesRef vectorBR) {
        assert indexVersion.onOrAfter(Version.V_7_5_0);
        ByteBuffer byteBuffer = ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, vectorBR.length);
        return byteBuffer.getFloat(vectorBR.offset + vectorBR.length - 4);
    }
}
