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

package org.elasticsearch.common.bytes;

import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * This is a {@link BytesReference} backed by a {@link ByteBuffer}. The byte buffer can either be a heap or
 * direct byte buffer. The reference is composed of the space between the {@link ByteBuffer#position()} and
 * {@link ByteBuffer#limit()} at construction time. If the position or limit of the underlying byte buffer is
 * changed, those changes will not be reflected in this reference. Any changes to the underlying data in the
 * byte buffer will be reflected in this reference.
 */
public class ByteBufferReference extends AbstractBytesReference {

    private final ByteBuffer buffer;
    private final int length;

    ByteBufferReference(ByteBuffer buffer) {
        this.buffer = buffer.slice();
        this.length = buffer.remaining();
    }

    @Override
    public byte get(int index) {
        return buffer.get(index);
    }

    @Override
    public int getInt(int index) {
        return buffer.getInt(index);
    }

    @Override
    public int indexOf(byte marker, int from) {
        final int remainingBytes = Math.max(length - from, 0);
        Objects.checkFromIndexSize(from, remainingBytes, length);
        if (buffer.hasArray()) {
            int startIndex = from + buffer.arrayOffset();
            int endIndex = startIndex + remainingBytes;
            final byte[] array = buffer.array();
            for (int i = startIndex; i < endIndex; i++) {
                if (array[i] == marker) {
                    return (i - buffer.arrayOffset());
                }
            }
            return -1;
        } else {
            return super.indexOf(marker, from);
        }
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        Objects.checkFromIndexSize(from, length, this.length);
        buffer.position(from);
        buffer.limit(from + length);
        ByteBufferReference newByteBuffer = new ByteBufferReference(buffer);
        buffer.position(0);
        buffer.limit(this.length);
        return newByteBuffer;
    }

    /**
     * This will return a bytes ref composed of the bytes. If this is a direct byte buffer, the bytes will
     * have to be copied.
     *
     * @return the bytes ref
     */
    @Override
    public BytesRef toBytesRef() {
        if (buffer.hasArray()) {
            return new BytesRef(buffer.array(), buffer.arrayOffset(), length);
        }
        final byte[] copy = new byte[length];
        buffer.get(copy, 0, length);
        return new BytesRef(copy);
    }

    @Override
    public long ramBytesUsed() {
        return buffer.capacity();
    }
}
