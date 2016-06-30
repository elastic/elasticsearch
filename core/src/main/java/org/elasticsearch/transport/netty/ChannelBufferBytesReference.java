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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 */
final class ChannelBufferBytesReference implements BytesReference {

    private final ChannelBuffer buffer;
    private final int size;

    ChannelBufferBytesReference(ChannelBuffer buffer, int size) {
        this.buffer = buffer;
        this.size = size;
        assert size <= buffer.readableBytes() : "size[" + size +"] > " + buffer.readableBytes();
    }

    @Override
    public byte get(int index) {
        return buffer.getByte(buffer.readerIndex() + index);
    }

    @Override
    public int length() {
        return size;
    }

    @Override
    public BytesReference slice(int from, int length) {
        return new ChannelBufferBytesReference(buffer.slice(buffer.readerIndex() + from, length), length);
    }

    @Override
    public StreamInput streamInput() {
        return new ChannelBufferStreamInput(buffer.duplicate(), size);
    }

    @Override
    public void writeTo(OutputStream os) throws IOException {
        buffer.getBytes(buffer.readerIndex(), os, size);
    }

    public byte[] toBytes() {
        return copyBytesArray().toBytes();
    }

    @Override
    public BytesArray toBytesArray() {
        if (buffer.hasArray()) {
            return new BytesArray(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), size);
        }
        return copyBytesArray();
    }

    @Override
    public BytesArray copyBytesArray() {
        byte[] copy = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), copy);
        return new BytesArray(copy);
    }

    public ChannelBuffer toChannelBuffer() {
        return buffer.duplicate();
    }

    @Override
    public boolean hasArray() {
        return buffer.hasArray();
    }

    @Override
    public byte[] array() {
        return buffer.array();
    }

    @Override
    public int arrayOffset() {
        return buffer.arrayOffset() + buffer.readerIndex();
    }

    @Override
    public String toUtf8() {
        return buffer.toString(StandardCharsets.UTF_8);
    }

    @Override
    public BytesRef toBytesRef() {
        if (buffer.hasArray()) {
            return new BytesRef(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), size);
        }
        byte[] copy = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), copy);
        return new BytesRef(copy);
    }

    @Override
    public BytesRef copyBytesRef() {
        byte[] copy = new byte[size];
        buffer.getBytes(buffer.readerIndex(), copy);
        return new BytesRef(copy);
    }

    @Override
    public int hashCode() {
        return Helper.bytesHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return Helper.bytesEqual(this, (BytesReference) obj);
    }
}
