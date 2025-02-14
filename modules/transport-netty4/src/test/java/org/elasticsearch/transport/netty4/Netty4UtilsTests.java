/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.AbstractBytesReferenceTestCase;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class Netty4UtilsTests extends ESTestCase {

    private static final int PAGE_SIZE = PageCacheRecycler.BYTE_PAGE_SIZE;
    private final BigArrays bigarrays = new BigArrays(null, new NoneCircuitBreakerService(), CircuitBreaker.REQUEST);

    public void testToChannelBufferWithEmptyRef() throws IOException {
        ByteBuf buffer = Netty4Utils.toByteBuf(getRandomizedBytesReference(0));
        assertSame(Unpooled.EMPTY_BUFFER, buffer);
    }

    public void testToChannelBufferWithSlice() throws IOException {
        BytesReference ref = getRandomizedBytesReference(randomIntBetween(1, 3 * PAGE_SIZE));
        int sliceOffset = randomIntBetween(0, ref.length());
        int sliceLength = randomIntBetween(ref.length() - sliceOffset, ref.length() - sliceOffset);
        BytesReference slice = ref.slice(sliceOffset, sliceLength);
        ByteBuf buffer = Netty4Utils.toByteBuf(slice);
        BytesReference bytesReference = Netty4Utils.toBytesReference(buffer);
        assertArrayEquals(BytesReference.toBytes(slice), BytesReference.toBytes(bytesReference));
    }

    public void testToChannelBufferWithSliceAfter() throws IOException {
        BytesReference ref = getRandomizedBytesReference(randomIntBetween(1, 3 * PAGE_SIZE));
        int sliceOffset = randomIntBetween(0, ref.length());
        int sliceLength = randomIntBetween(ref.length() - sliceOffset, ref.length() - sliceOffset);
        ByteBuf buffer = Netty4Utils.toByteBuf(ref);
        BytesReference bytesReference = Netty4Utils.toBytesReference(buffer);
        assertArrayEquals(
            BytesReference.toBytes(ref.slice(sliceOffset, sliceLength)),
            BytesReference.toBytes(bytesReference.slice(sliceOffset, sliceLength))
        );
    }

    public void testToChannelBuffer() throws IOException {
        BytesReference ref = getRandomizedBytesReference(randomIntBetween(1, 3 * PAGE_SIZE));
        ByteBuf buffer = Netty4Utils.toByteBuf(ref);
        BytesReference bytesReference = Netty4Utils.toBytesReference(buffer);
        if (AbstractBytesReferenceTestCase.getNumPages(ref) > 1) { // we gather the buffers into a channel buffer
            assertTrue(buffer instanceof CompositeByteBuf);
        }
        assertArrayEquals(BytesReference.toBytes(ref), BytesReference.toBytes(bytesReference));
    }

    /**
     * Test that wrapped reference counted object from netty reflects correct counts in ES RefCounted
     */
    public void testToRefCounted() {
        var buf = PooledByteBufAllocator.DEFAULT.buffer(1);
        assertEquals(1, buf.refCnt());

        var refCounted = Netty4Utils.toRefCounted(buf);
        assertEquals(1, refCounted.refCnt());

        buf.retain();
        assertEquals(2, refCounted.refCnt());

        refCounted.incRef();
        assertEquals(3, refCounted.refCnt());
        assertEquals(buf.refCnt(), refCounted.refCnt());

        refCounted.decRef();
        assertEquals(2, refCounted.refCnt());
        assertEquals(buf.refCnt(), refCounted.refCnt());
        assertTrue(refCounted.hasReferences());

        refCounted.decRef();
        refCounted.decRef();
        assertFalse(refCounted.hasReferences());
    }

    /**
     * Ensures that released ByteBuf cannot be accessed from ReleasableBytesReference
     */
    public void testToReleasableBytesReferenceThrowOnByteBufRelease() {
        var buf = PooledByteBufAllocator.DEFAULT.buffer(1);
        var relBytes = Netty4Utils.toReleasableBytesReference(buf);
        buf.release();
        assertThrows(AssertionError.class, () -> relBytes.get(0));
    }

    private BytesReference getRandomizedBytesReference(int length) throws IOException {
        // we know bytes stream output always creates a paged bytes reference, we use it to create randomized content
        ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(length, bigarrays);
        for (int i = 0; i < length; i++) {
            out.writeByte((byte) random().nextInt(1 << 8));
        }
        assertEquals(out.size(), length);
        BytesReference ref = out.bytes();
        assertEquals(ref.length(), length);
        if (randomBoolean()) {
            return new BytesArray(ref.toBytesRef());
        } else if (randomBoolean()) {
            BytesRef bytesRef = ref.toBytesRef();
            return Netty4Utils.toBytesReference(Unpooled.wrappedBuffer(bytesRef.bytes, bytesRef.offset, bytesRef.length));
        } else {
            return ref;
        }
    }

}
