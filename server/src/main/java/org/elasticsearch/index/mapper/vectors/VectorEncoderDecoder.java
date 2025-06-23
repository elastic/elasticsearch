/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.IndexVersion;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAGNITUDE_STORED_INDEX_VERSION;

public final class VectorEncoderDecoder {
    public static final byte INT_BYTES = 4;

    private VectorEncoderDecoder() {}

    public static int denseVectorLength(IndexVersion indexVersion, BytesRef vectorBR) {
        return indexVersion.onOrAfter(MAGNITUDE_STORED_INDEX_VERSION)
            ? (vectorBR.length - INT_BYTES) / INT_BYTES
            : vectorBR.length / INT_BYTES;
    }

    /**
     * Decodes the last 4 bytes of the encoded vector, which contains the vector magnitude.
     * NOTE: this function can only be called on vectors from an index version greater than or
     * equal to 7.5.0, since vectors created prior to that do not store the magnitude.
     */
    public static float decodeMagnitude(IndexVersion indexVersion, BytesRef vectorBR) {
        assert indexVersion.onOrAfter(MAGNITUDE_STORED_INDEX_VERSION);
        ByteBuffer byteBuffer = indexVersion.onOrAfter(LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION)
            ? ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, vectorBR.length).order(ByteOrder.LITTLE_ENDIAN)
            : ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, vectorBR.length);
        return byteBuffer.getFloat(vectorBR.offset + vectorBR.length - INT_BYTES);
    }

    /**
     * Calculates vector magnitude
     */
    private static float calculateMagnitude(float[] decodedVector) {
        return (float) Math.sqrt(VectorUtil.dotProduct(decodedVector, decodedVector));
    }

    public static float getMagnitude(IndexVersion indexVersion, BytesRef vectorBR, float[] decodedVector) {
        if (vectorBR == null) {
            throw new IllegalArgumentException(DenseVectorScriptDocValues.MISSING_VECTOR_FIELD_MESSAGE);
        }
        if (indexVersion.onOrAfter(MAGNITUDE_STORED_INDEX_VERSION)) {
            return decodeMagnitude(indexVersion, vectorBR);
        } else {
            return calculateMagnitude(decodedVector);
        }
    }

    /**
     * Decodes a BytesRef into the provided array of floats
     * @param vectorBR - dense vector encoded in BytesRef
     * @param vector - array of floats where the decoded vector should be stored
     */
    public static void decodeDenseVector(IndexVersion indexVersion, BytesRef vectorBR, float[] vector) {
        if (vectorBR == null) {
            throw new IllegalArgumentException(DenseVectorScriptDocValues.MISSING_VECTOR_FIELD_MESSAGE);
        }
        if (indexVersion.onOrAfter(LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION)) {
            FloatBuffer fb = ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, vectorBR.length)
                .order(ByteOrder.LITTLE_ENDIAN)
                .asFloatBuffer();
            fb.get(vector);
        } else {
            ByteBuffer byteBuffer = ByteBuffer.wrap(vectorBR.bytes, vectorBR.offset, vectorBR.length);
            for (int dim = 0; dim < vector.length; dim++) {
                vector[dim] = byteBuffer.getFloat((dim * Float.BYTES) + vectorBR.offset);
            }
        }
    }

    public static float[] getMultiMagnitudes(BytesRef magnitudes) {
        assert magnitudes.length % Float.BYTES == 0;
        float[] multiMagnitudes = new float[magnitudes.length / Float.BYTES];
        ByteBuffer byteBuffer = ByteBuffer.wrap(magnitudes.bytes, magnitudes.offset, magnitudes.length).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < magnitudes.length / Float.BYTES; i++) {
            multiMagnitudes[i] = byteBuffer.getFloat();
        }
        return multiMagnitudes;
    }

}
