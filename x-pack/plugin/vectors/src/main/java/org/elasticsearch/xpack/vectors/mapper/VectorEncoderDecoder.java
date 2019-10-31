/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;

import java.nio.ByteBuffer;

public final class VectorEncoderDecoder {
    public static final byte INT_BYTES = 4;

    private VectorEncoderDecoder() { }

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
