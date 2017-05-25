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

package org.elasticsearch.transport.nio;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.nio.ByteBuffer;

public class ByteBufferReference extends BytesReference {

    private final int offset;
    private final int length;
    private final ByteBuffer writeBuffer;
    private final ByteBuffer readBuffer;
    private int writeIndex;
    private int readIndex;

    public ByteBufferReference(ByteBuffer writeBuffer, ByteBuffer readBuffer, int offset, int length, int writeIndex, int readIndex) {
        this.offset = offset;
        this.length = length;
        this.writeIndex = writeIndex;
        this.readIndex = readIndex;
        this.writeBuffer = writeBuffer;
        this.readBuffer = readBuffer;
    }

    public static ByteBufferReference heap(BytesArray bytesArray) {
        return heap(bytesArray, 0, 0);
    }

    public static ByteBufferReference heap(BytesArray bytesArray, int writeIndex, int readIndex) {
        int offset = bytesArray.offset();
        ByteBuffer writeBuffer = ByteBuffer.wrap(bytesArray.array(), offset, bytesArray.length());
        ByteBuffer readBuffer = ByteBuffer.wrap(bytesArray.array(), offset, bytesArray.length());
        initializePositions(offset, writeIndex, readIndex, writeBuffer, readBuffer);
        return new ByteBufferReference(writeBuffer, readBuffer, offset, bytesArray.length(), writeIndex, readIndex);
    }

    @Override
    public byte get(int index) {
        return readBuffer.get(index + offset);
    }

    @Override
    public int length() {
        return length;
    }

    @Override
    public ByteBufferReference slice(int from, int length) {
        if (from < 0 || (from + length) > this.length) {
            throw new IllegalArgumentException("can't slice a buffer with length [" + this.length +
                "], with slice parameters from [" + from + "], length [" + length + "]");
        }
        int newReadIndex = Math.min(Math.max(readIndex - from, 0), length);
        int newWriteIndex = Math.min(Math.max(writeIndex - from, 0), length);

        ByteBuffer newWriteBuffer = this.writeBuffer.duplicate();
        ByteBuffer newReadBuffer = this.readBuffer.duplicate();
        initializePositions(offset, newReadIndex, newWriteIndex, newWriteBuffer, newReadBuffer);
        return new ByteBufferReference(newWriteBuffer, newReadBuffer, offset + from, length, newWriteIndex, newReadIndex);
    }

    @Override
    public BytesRef toBytesRef() {
        if (readBuffer.hasArray()) {
            return new BytesRef(readBuffer.array(), readBuffer.arrayOffset() + offset, length);
        }
        final byte[] copy = new byte[length];
        readBuffer.get(copy, offset, length);
        return new BytesRef(copy);
    }

    @Override
    public long ramBytesUsed() {
        return readBuffer.capacity();
    }

    public int getWriteIndex() {
        return writeIndex;
    }

    public void incrementWrite(int delta) {
        writeIndex += delta;
    }

    public int getWriteRemaining() {
        return length - writeIndex;
    }

    public int getReadIndex() {
        return readIndex;
    }

    public void incrementRead(int delta) {
        readIndex += delta;
    }

    public int getReadRemaining() {
        return length - readIndex;
    }

    public ByteBuffer getWriteByteBuffer() {
        writeBuffer.position(offset + writeIndex);
        writeBuffer.limit(offset + length);
        return writeBuffer;
    }

    public ByteBuffer getReadByteBuffer() {
        readBuffer.position(offset + readIndex);
        readBuffer.limit(offset + length);
        return readBuffer;
    }

    private static void initializePositions(int offset, int writeIndex, int readIndex, ByteBuffer writeBuffer, ByteBuffer readBuffer) {
        writeBuffer.position(offset + writeIndex);
        readBuffer.position(offset + readIndex);
    }
}
