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
import org.apache.lucene.util.FutureObjects;

import java.nio.ByteBuffer;

/**
 * This is a {@link BytesReference} backed by a {@link ByteBuffer}. The byte buffer can either be a heap or
 * direct byte buffer. The reference is composed of the space between the {@link ByteBuffer#position()} and
 * {@link ByteBuffer#limit()} at construction time. If the position or limit of the underlying byte buffer is
 * changed, those changes will not be reflected in this reference. However, modifying the limit or position
 * of the underlying byte buffer is not recommended as those can be used during {@link ByteBuffer#get()}
 * bounds checks. Use {@link ByteBuffer#duplicate()} at creation time if you plan on modifying the markers of
 * the underlying byte buffer. Any changes to the underlying data in the byte buffer will be reflected.
 */
public class ByteBufferReference extends BytesReference {

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
    public int length() {
        return length;
    }

    @Override
    public BytesReference slice(int from, int length) {
        FutureObjects.checkFromIndexSize(from, length, this.length);
        buffer.position(from);
        buffer.limit(from + length);
        ByteBuffer newByteBuffer = buffer.duplicate();
        buffer.position(0);
        buffer.limit(length);
        return new ByteBufferReference(newByteBuffer);
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
