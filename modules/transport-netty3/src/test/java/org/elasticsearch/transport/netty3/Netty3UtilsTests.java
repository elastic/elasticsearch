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
package org.elasticsearch.transport.netty3;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.AbstractBytesReferenceTestCase;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.CompositeChannelBuffer;

import java.io.IOException;

public class Netty3UtilsTests extends ESTestCase {

    private static final int PAGE_SIZE = BigArrays.BYTE_PAGE_SIZE;
    private final BigArrays bigarrays = new BigArrays(null, new NoneCircuitBreakerService(), false);

    public void testToChannelBufferWithEmptyRef() throws IOException {
        ChannelBuffer channelBuffer = Netty3Utils.toChannelBuffer(getRandomizedBytesReference(0));
        assertSame(ChannelBuffers.EMPTY_BUFFER, channelBuffer);
    }

    public void testToChannelBufferWithSlice() throws IOException {
        BytesReference ref = getRandomizedBytesReference(randomIntBetween(1, 3 * PAGE_SIZE));
        int sliceOffset = randomIntBetween(0, ref.length());
        int sliceLength = randomIntBetween(ref.length() - sliceOffset, ref.length() - sliceOffset);
        BytesReference slice = ref.slice(sliceOffset, sliceLength);
        ChannelBuffer channelBuffer = Netty3Utils.toChannelBuffer(slice);
        BytesReference bytesReference = Netty3Utils.toBytesReference(channelBuffer);
        assertArrayEquals(BytesReference.toBytes(slice), BytesReference.toBytes(bytesReference));
    }

    public void testToChannelBufferWithSliceAfter() throws IOException {
        BytesReference ref = getRandomizedBytesReference(randomIntBetween(1, 3 * PAGE_SIZE));
        int sliceOffset = randomIntBetween(0, ref.length());
        int sliceLength = randomIntBetween(ref.length() - sliceOffset, ref.length() - sliceOffset);
        ChannelBuffer channelBuffer = Netty3Utils.toChannelBuffer(ref);
        BytesReference bytesReference = Netty3Utils.toBytesReference(channelBuffer);
        assertArrayEquals(BytesReference.toBytes(ref.slice(sliceOffset, sliceLength)),
            BytesReference.toBytes(bytesReference.slice(sliceOffset, sliceLength)));
    }

    public void testToChannelBuffer() throws IOException {
        BytesReference ref = getRandomizedBytesReference(randomIntBetween(1, 3 * PAGE_SIZE));
        ChannelBuffer channelBuffer = Netty3Utils.toChannelBuffer(ref);
        BytesReference bytesReference = Netty3Utils.toBytesReference(channelBuffer);
        if (ref instanceof ChannelBufferBytesReference) {
            assertEquals(channelBuffer, ((ChannelBufferBytesReference) ref).toChannelBuffer());
        } else if (AbstractBytesReferenceTestCase.getNumPages(ref) > 1) { // we gather the buffers into a channel buffer
            assertTrue(channelBuffer instanceof CompositeChannelBuffer);
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
            return Netty3Utils.toBytesReference(ChannelBuffers.wrappedBuffer(bytesRef.bytes, bytesRef.offset,
                bytesRef.length));
        } else {
            return ref;
        }
    }

}
