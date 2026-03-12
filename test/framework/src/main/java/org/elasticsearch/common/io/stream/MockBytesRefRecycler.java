/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomByte;
import static org.elasticsearch.test.ESTestCase.randomByteArrayOfLength;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * A {@link Recycler} for {@link BytesRef} pages which returns pages with nontrivial offsets and verifies that the surrounding buffer pool
 * is left untouched.
 */
public class MockBytesRefRecycler implements Recycler<BytesRef>, Releasable {

    private final AtomicInteger activePageCount = new AtomicInteger();

    @Override
    public V<BytesRef> obtain() {
        activePageCount.incrementAndGet();
        final var bufferPool = randomByteArrayOfLength(between(pageSize(), pageSize() * 4));
        final var offset = randomBoolean()
            // align to a multiple of pageSize(), which is the usual case in production
            ? between(0, bufferPool.length / pageSize() - 1) * pageSize()
            // no alignment, to detect cases where alignment is assumed
            : between(0, bufferPool.length - pageSize());
        final var bytesRef = new BytesRef(bufferPool, offset, pageSize());

        final var bufferPoolCopy = ArrayUtil.copyArray(bufferPool); // keep for a later check for out-of-bounds writes
        final var dummyByte = randomByte();
        Arrays.fill(bufferPoolCopy, offset, offset + pageSize(), dummyByte);

        return new V<>() {
            @Override
            public BytesRef v() {
                return bytesRef;
            }

            @Override
            public boolean isRecycled() {
                throw new AssertionError("shouldn't matter");
            }

            @Override
            public void close() {
                onClose(bytesRef);

                // page must not be changed
                assertSame(bufferPool, bytesRef.bytes);
                assertEquals(offset, bytesRef.offset);
                assertEquals(pageSize(), bytesRef.length);

                Arrays.fill(bufferPool, offset, offset + pageSize(), dummyByte); // overwrite buffer contents to detect use-after-free
                assertArrayEquals(bufferPoolCopy, bufferPool); // remainder of pool must be unmodified

                activePageCount.decrementAndGet();
            }
        };
    }

    @Override
    public int pageSize() {
        return PageCacheRecycler.BYTE_PAGE_SIZE;
    }

    @Override
    public void close() {
        assertEquals(0, activePageCount.get());
    }

    protected void onClose(BytesRef bytesRef) {}

    /**
     * @return number of currently-active pages
     */
    public int activePageCount() {
        return activePageCount.get();
    }
}
