/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.blobcache;

import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.EOFException;
import java.nio.ByteBuffer;

public class BlobCacheUtilsTests extends ESTestCase {

    public void testReadSafeThrows() {
        final ByteBuffer buffer = ByteBuffer.allocate(randomIntBetween(1, 1025));
        final int remaining = randomIntBetween(1, 1025);
        expectThrows(EOFException.class, () -> BlobCacheUtils.readSafe(BytesArray.EMPTY.streamInput(), buffer, 0, remaining));
    }

    public void testToPageAlignedSize() {
        long value = randomLongBetween(0, Long.MAX_VALUE / 2);
        long expected = ((value - 1) / SharedBytes.PAGE_SIZE + 1) * SharedBytes.PAGE_SIZE;
        assertThat(BlobCacheUtils.toPageAlignedSize(value), Matchers.equalTo(expected));
    }
}
