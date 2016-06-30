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
package org.elasticsearch.transport.netty;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.AbstractBytesReferenceTestCase;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.transport.netty.NettyUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import java.io.IOException;

public class ChannelBufferBytesReferenceTests extends AbstractBytesReferenceTestCase {
    @Override
    protected BytesReference newBytesReference(int length) throws IOException {
        ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(length, bigarrays);
        for (int i = 0; i < length; i++) {
            out.writeByte((byte) random().nextInt(1 << 8));
        }
        assertEquals(out.size(), length);
        BytesReference ref = out.bytes();
        assertEquals(ref.length(), length);
        BytesRef bytesArray = ref.toBytesRef();
        return NettyUtils.toBytesReference(ChannelBuffers.wrappedBuffer(bytesArray.bytes, bytesArray.offset,
            bytesArray.length));
    }

    public void testSliceOnAdvancedBuffer() throws IOException {
        BytesReference bytesReference = newBytesReference(randomIntBetween(10, 3 * PAGE_SIZE));
        BytesRef bytesArray = bytesReference.toBytesRef();
        ChannelBuffer channelBuffer = ChannelBuffers.wrappedBuffer(bytesArray.bytes, bytesArray.offset,
            bytesArray.length);
        int numBytesToRead = randomIntBetween(1, 5);
        for (int i = 0; i < numBytesToRead; i++) {
            channelBuffer.readByte();
        }
        BytesReference other = NettyUtils.toBytesReference(channelBuffer);
        BytesReference slice = bytesReference.slice(numBytesToRead, bytesReference.length() - numBytesToRead);
        assertEquals(other, slice);

        assertEquals(other.slice(3, 1), slice.slice(3, 1));
    }
}
