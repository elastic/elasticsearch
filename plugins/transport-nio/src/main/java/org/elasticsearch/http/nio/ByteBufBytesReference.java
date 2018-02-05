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
package org.elasticsearch.http.nio;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class ByteBufBytesReference extends BytesReference {

    private final ByteBuf buffer;
    private final int length;
    private final int offset;

    ByteBufBytesReference(ByteBuf buffer, int length) {
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
        return new ByteBufBytesReference(buffer.slice(offset + from, length), length);
    }

    @Override
    public StreamInput streamInput() {
        return new ByteBufStreamInput(buffer.duplicate(), length);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        buffer.getBytes(offset, os, length);
    }

    public ByteBuf toByteBuf() {
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

    public static BytesReference toBytesReference(final ByteBuf buffer) {
        return new ByteBufBytesReference(buffer, buffer.readableBytes());
    }

    /**
     * Turns the given BytesReference into a ByteBuf. Note: the returned ByteBuf will reference the internal
     * pages of the BytesReference. Don't free the bytes of reference before the ByteBuf goes out of scope.
     */
    public static ByteBuf toByteBuf(final BytesReference reference) {
        if (reference.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        if (reference instanceof ByteBufBytesReference) {
            return ((ByteBufBytesReference) reference).toByteBuf();
        } else {
            final BytesRefIterator iterator = reference.iterator();
            // usually we have one, two, or three components from the header, the message, and a buffer
            final List<ByteBuf> buffers = new ArrayList<>(3);
            try {
                BytesRef slice;
                while ((slice = iterator.next()) != null) {
                    buffers.add(Unpooled.wrappedBuffer(slice.bytes, slice.offset, slice.length));
                }
                final CompositeByteBuf composite = Unpooled.compositeBuffer(buffers.size());
                composite.addComponents(true, buffers);
                return composite;
            } catch (IOException ex) {
                throw new AssertionError("no IO happens here", ex);
            }
        }
    }
}
