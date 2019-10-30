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
}
