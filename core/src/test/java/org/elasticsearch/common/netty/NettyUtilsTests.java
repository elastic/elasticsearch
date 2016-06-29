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
package org.elasticsearch.common.netty;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.CompositeChannelBuffer;
import org.junit.Before;

import java.io.IOException;

public class NettyUtilsTests extends ESTestCase {

    private static final int PAGE_SIZE = BigArrays.BYTE_PAGE_SIZE;
    private final BigArrays bigarrays = new BigArrays(null, new NoneCircuitBreakerService(), false);

    public void testToChannelBufferWithEmptyRef() throws IOException {
        ChannelBuffer channelBuffer = NettyUtils.toChannelBuffer(getRandomizedBytesReference(0));
        assertSame(ChannelBuffers.EMPTY_BUFFER, channelBuffer);
    }

    public void testToChannelBufferWithSlice() throws IOException {
        BytesReference ref = getRandomizedBytesReference(randomIntBetween(1, 3 * PAGE_SIZE));
        int sliceOffset = randomIntBetween(0, ref.length());
        int sliceLength = randomIntBetween(ref.length() - sliceOffset, ref.length() - sliceOffset);
        BytesReference slice = ref.slice(sliceOffset, sliceLength);
        ChannelBuffer channelBuffer = NettyUtils.toChannelBuffer(slice);
        BytesReference bytesReference = NettyUtils.toBytesReference(channelBuffer);
        assertArrayEquals(slice.toBytes(), bytesReference.toBytes());
    }

    public void testToChannelBufferWithSliceAfter() throws IOException {
        BytesReference ref = getRandomizedBytesReference(randomIntBetween(1, 3 * PAGE_SIZE));
        int sliceOffset = randomIntBetween(0, ref.length());
        int sliceLength = randomIntBetween(ref.length() - sliceOffset, ref.length() - sliceOffset);
        ChannelBuffer channelBuffer = NettyUtils.toChannelBuffer(ref);
        BytesReference bytesReference = NettyUtils.toBytesReference(channelBuffer);
        assertArrayEquals(ref.slice(sliceOffset, sliceLength).toBytes(), bytesReference.slice(sliceOffset, sliceLength).toBytes());
    }

    public void testToChannelBuffer() throws IOException {
        BytesReference ref = getRandomizedBytesReference(randomIntBetween(1, 3 * PAGE_SIZE));
        ChannelBuffer channelBuffer = NettyUtils.toChannelBuffer(ref);
        BytesReference bytesReference = NettyUtils.toBytesReference(channelBuffer);
        if (ref instanceof ChannelBufferBytesReference) {
            assertEquals(channelBuffer, ((ChannelBufferBytesReference) ref).toChannelBuffer());
        } else if (ref.hasArray() == false) { // we gather the buffers into a channel buffer
            assertTrue(channelBuffer instanceof CompositeChannelBuffer);
        }
        assertArrayEquals(ref.toBytes(), bytesReference.toBytes());
    }

    private BytesReference getRandomizedBytesReference(int length) throws IOException {
        // TODO we should factor out a BaseBytesReferenceTestCase
        // we know bytes stream output always creates a paged bytes reference, we use it to create randomized content
        ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(length, bigarrays);
        for (int i = 0; i < length; i++) {
            out.writeByte((byte) random().nextInt(1 << 8));
        }
        assertEquals(out.size(), length);
        BytesReference ref = out.bytes();
        assertEquals(ref.length(), length);
        if (randomBoolean()) {
            return ref.toBytesArray();
        } else if (randomBoolean()) {
            BytesArray bytesArray = ref.toBytesArray();
            return NettyUtils.toBytesReference(ChannelBuffers.wrappedBuffer(bytesArray.array(), bytesArray.arrayOffset(),
                bytesArray.length()));
        } else {
            return ref;
        }
    }
}
