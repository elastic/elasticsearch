/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.Version;

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
        byte[] buf = indexVersion.onOrAfter(Version.V_7_4_0) ? new byte[dimCount * (INT_BYTES + SHORT_BYTES) + INT_BYTES] :
            new byte[dimCount * (INT_BYTES + SHORT_BYTES)];
        int offset = 0;
        for (int dim = 0; dim < dimCount; dim++) {
            buf[offset++] = (byte) (dims[dim] >>  8);
            buf[offset++] = (byte) dims[dim];
        }

        // 3. Encode values
        double dotProduct = 0.0f;
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = Float.floatToIntBits(values[dim]);
            buf[offset++] = (byte) (intValue >> 24);
            buf[offset++] = (byte) (intValue >> 16);
            buf[offset++] = (byte) (intValue >>  8);
            buf[offset++] = (byte) intValue;
            dotProduct += values[dim] * values[dim];
        }

        // 4. Encode vector magnitude at the end
        if (indexVersion.onOrAfter(Version.V_7_4_0)) {
            float vectorMagnitude = (float) Math.sqrt(dotProduct);
            int vectorMagnitudeIntValue = Float.floatToIntBits(vectorMagnitude);
            buf[offset++] = (byte) (vectorMagnitudeIntValue >> 24);
            buf[offset++] = (byte) (vectorMagnitudeIntValue >> 16);
            buf[offset++] = (byte) (vectorMagnitudeIntValue >> 8);
            buf[offset++] = (byte) vectorMagnitudeIntValue;
        }

        return new BytesRef(buf);
    }

    /**
     * Decodes the first part of BytesRef into sparse vector dimensions
     * @param indexVersion - index version
     * @param vectorBR - sparse vector encoded in BytesRef
     */
    public static int[] decodeSparseVectorDims(Version indexVersion, BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int dimCount = indexVersion.onOrAfter(Version.V_7_4_0) ? (vectorBR.length - INT_BYTES) / (INT_BYTES + SHORT_BYTES) :
            vectorBR.length / (INT_BYTES + SHORT_BYTES);
        int offset = vectorBR.offset;
        int[] dims = new int[dimCount];
        for (int dim = 0; dim < dimCount; dim++) {
            dims[dim] = ((vectorBR.bytes[offset++] & 0xFF) << 8) | (vectorBR.bytes[offset++] & 0xFF);
        }
        return dims;
    }

    /**
     * Decodes the second part of the BytesRef into sparse vector values
     * @param indexVersion - index version
     * @param vectorBR - sparse vector encoded in BytesRef
     */
    public static float[] decodeSparseVector(Version indexVersion, BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int dimCount = indexVersion.onOrAfter(Version.V_7_4_0) ? (vectorBR.length - INT_BYTES) / (INT_BYTES + SHORT_BYTES) :
            vectorBR.length / (INT_BYTES + SHORT_BYTES);
        int offset = vectorBR.offset + SHORT_BYTES * dimCount;
        float[] vector = new float[dimCount];
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = ((vectorBR.bytes[offset++] & 0xFF) << 24)   |
                ((vectorBR.bytes[offset++] & 0xFF) << 16) |
                ((vectorBR.bytes[offset++] & 0xFF) <<  8) |
                (vectorBR.bytes[offset++] & 0xFF);
            vector[dim] = Float.intBitsToFloat(intValue);
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
    public static void sortSparseDimsDoubleValues(int[] dims, double[] values, int n) {
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

                double tempValue = values[j];
                values[j] = values[i];
                values[i] = tempValue;
            }
        }.sort(0, n);
    }

    /**
     * Decodes a BytesRef into an array of floats
     * @param indexVersion - index Version
     * @param vectorBR - dense vector encoded in BytesRef
     */
    public static float[] decodeDenseVector(Version indexVersion, BytesRef vectorBR) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        int dimCount = indexVersion.onOrAfter(Version.V_7_4_0) ? (vectorBR.length - INT_BYTES) / INT_BYTES : vectorBR.length/ INT_BYTES;
        int offset = vectorBR.offset;
        float[] vector = new float[dimCount];
        for (int dim = 0; dim < dimCount; dim++) {
            int intValue = ((vectorBR.bytes[offset++] & 0xFF) << 24)   |
                ((vectorBR.bytes[offset++] & 0xFF) << 16) |
                ((vectorBR.bytes[offset++] & 0xFF) <<  8) |
                (vectorBR.bytes[offset++] & 0xFF);
            vector[dim] = Float.intBitsToFloat(intValue);
        }
        return vector;
    }

    /**
     * Calculates vector magnitude either by
     * decoding last 4 bytes of BytesRef into a vector magnitude or calculating it
     * @param indexVersion - index Version
     * @param vectorBR - vector encoded in BytesRef
     * @param vector - float vector
     */
    public static float getVectorMagnitude(Version indexVersion, BytesRef vectorBR, float[] vector) {
        if (vectorBR == null) {
            throw new IllegalArgumentException("A document doesn't have a value for a vector field!");
        }
        if (indexVersion.onOrAfter(Version.V_7_4_0)) { // decode vector magnitude
            int offset = vectorBR.offset + vectorBR.length - 4;
            int vectorMagnitudeIntValue = ((vectorBR.bytes[offset++] & 0xFF) << 24)   |
                ((vectorBR.bytes[offset++] & 0xFF) << 16) |
                ((vectorBR.bytes[offset++] & 0xFF) <<  8) |
                (vectorBR.bytes[offset++] & 0xFF);
            float vectorMagnitude = Float.intBitsToFloat(vectorMagnitudeIntValue);
            return vectorMagnitude;
        } else { // calculate vector magnitude
            double dotProduct = 0f;
            for (int dim = 0; dim < vector.length; dim++) {
                dotProduct += (double) vector[dim] * vector[dim];
            }
            return (float) Math.sqrt(dotProduct);
        }
    }
}
