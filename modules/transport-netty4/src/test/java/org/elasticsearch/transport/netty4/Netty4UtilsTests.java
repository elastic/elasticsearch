/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
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
        assertArrayEquals(BytesReference.toBytes(ref.slice(sliceOffset, sliceLength)),
            BytesReference.toBytes(bytesReference.slice(sliceOffset, sliceLength)));
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
            return Netty4Utils.toBytesReference(Unpooled.wrappedBuffer(bytesRef.bytes, bytesRef.offset,
                bytesRef.length));
        } else {
            return ref;
        }
    }

}
