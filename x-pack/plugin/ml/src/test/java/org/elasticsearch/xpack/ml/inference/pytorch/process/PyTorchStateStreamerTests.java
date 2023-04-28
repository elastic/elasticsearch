/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchStateStreamer.NUM_BYTES_IN_PRELUDE;
import static org.elasticsearch.xpack.ml.inference.pytorch.process.PyTorchStateStreamer.UNSIGNED_INT_MAX;

public class PyTorchStateStreamerTests extends ESTestCase {
    public void testModelSizeExtremes() {
        // Longs are 8 bytes but we should only use 4
        ByteBuffer longByteBuffer = ByteBuffer.allocate(NUM_BYTES_IN_PRELUDE * 2);
        ByteBuffer byteBuffer = ByteBuffer.allocate(NUM_BYTES_IN_PRELUDE);
        for (Long value : List.of(
            0L,
            (long) Integer.MAX_VALUE - 1,
            (long) Integer.MAX_VALUE,
            Integer.MAX_VALUE + 1L,
            Integer.MAX_VALUE + 10L,
            Integer.MAX_VALUE + 13L,
            UNSIGNED_INT_MAX
        )) {
            byteBuffer.putInt(value.intValue());
            longByteBuffer.putLong(value);
            // check the BIG_ENDIAN arrays are exactly the same
            assertArrayEquals(Arrays.copyOfRange(longByteBuffer.array(), 4, 8), byteBuffer.array());
            // Check that the other bytes are all `0`
            assertArrayEquals(Arrays.copyOfRange(longByteBuffer.array(), 0, 4), new byte[] { 0, 0, 0, 0 });
            byteBuffer.clear();
            longByteBuffer.clear();
        }
    }
}
