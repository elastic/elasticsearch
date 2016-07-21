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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

final class ChannelBufferBytesReference extends BytesReference {

    private final ChannelBuffer buffer;
    private final int length;
    private final int offset;

    ChannelBufferBytesReference(ChannelBuffer buffer, int length) {
        this.buffer = buffer;
        this.length = length;
        this.offset = buffer.readerIndex();
        assert length <= buffer.readableBytes() : "length[" + length +"] > " + buffer.readableBytes();
    }

    @Override
    public byte get(int index) {
        return buffer.getByte(offset + index);
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        return new ChannelBufferBytesReference(buffer.slice(offset + from, length), length);
    }

    @Override
    public StreamInput streamInput() {
        return new ChannelBufferStreamInput(buffer.duplicate(), length);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        buffer.getBytes(offset, os, length);
    }

    ChannelBuffer toChannelBuffer() {
        return buffer.duplicate();
    }

    @Override
    public String utf8ToString() {
        return buffer.toString(offset, length, StandardCharsets.UTF_8);
    }

    @Override
    public BytesRef toBytesRef() {
        if (buffer.hasArray()) {
            return new BytesRef(buffer.array(), buffer.arrayOffset() + offset, length);
        }
        final byte[] copy = new byte[length];
        buffer.getBytes(offset, copy);
        return new BytesRef(copy);
    }

    @Override
    public long ramBytesUsed() {
        return buffer.capacity();
    }
}
