/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.blobcache;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import java.io.EOFException;
import java.nio.ByteBuffer;

public class BlobCacheUtilsTests extends ESTestCase {

    public void testReadSafeThrows() {
        final ByteBuffer buffer = ByteBuffer.allocate(randomIntBetween(1, 1025));
        final int remaining = randomIntBetween(1, 1025);
        expectThrows(EOFException.class, () -> BlobCacheUtils.readSafe(BytesArray.EMPTY.streamInput(), buffer, 0, remaining, null));
    }
}
