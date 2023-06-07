/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;

public class VectorEncoderDecoderTests extends ESTestCase {

    public void testVectorDecodingWithOffset() {
        float[] inputFloats = new float[] { 1f, 2f, 3f, 4f };
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        double magnitude = 0.0;
        for (float f : inputFloats) {
            byteBuffer.putFloat(f);
            magnitude += f * f;
        }
        // Binary documents store magnitude in a float at the end of the buffer array
        magnitude /= 4;
        byteBuffer.putFloat((float)magnitude);
        BytesRef floatBytes = new BytesRef(byteBuffer.array());
        // adjust so that we have an offset ignoring the first float
        floatBytes.length = 16;
        floatBytes.offset = 4;
        // since we are ignoring the first float to mock an offset, our dimensions can be assumed to be 3
        float[] outputFloats = new float[3];
        VectorEncoderDecoder.decodeDenseVector(floatBytes, outputFloats);
        assertArrayEquals(outputFloats, new float[] { 2f, 3f, 4f }, 0f);
    }

}
