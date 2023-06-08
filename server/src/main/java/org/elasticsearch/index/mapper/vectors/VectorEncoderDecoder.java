/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.LITTLE_ENDIAN_FLOAT_STORED_INDEX_VERSION;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAGNITUDE_STORED_INDEX_VERSION;

public final class VectorEncoderDecoder {
    public static final byte INT_BYTES = 4;

    private VectorEncoderDecoder() {}

    public static int denseVectorLength(Version indexVersion, BytesRef vectorBR) {
        return indexVersion.onOrAfter(MAGNITUDE_STORED_INDEX_VERSION)
            ? (vectorBR.length - INT_BYTES) / INT_BYTES
            : vectorBR.length / INT_BYTES;
    }

    /**
     * Decodes the last 4 bytes of the encoded vector, which contains the vector magnitude.
     * NOTE: this function can only be called on vectors from an index version greater than or
     * equal to 7.5.0, since vectors created prior to that do not store the magnitude.
     */
    public static float decodeMagnitude(Version indexVersion, BytesRef vectorBR) {
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
        double magnitude = 0.0f;
        for (int i = 0; i < decodedVector.length; i++) {
            magnitude += decodedVector[i] * decodedVector[i];
        }
        magnitude = Math.sqrt(magnitude);
        return (float) magnitude;
    }

    public static float getMagnitude(Version indexVersion, BytesRef vectorBR, float[] decodedVector) {
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
    public static void decodeDenseVector(Version indexVersion, BytesRef vectorBR, float[] vector) {
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

}
