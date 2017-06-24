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

import java.nio.ByteBuffer;

public class ByteBufferReference extends NetworkBytesReference {

    private final int offset;
    private BytesArray bytesArray;
    private final ByteBuffer writeBuffer;
    private final ByteBuffer readBuffer;

    public ByteBufferReference(BytesArray bytesArray, int writeIndex, int readIndex) {
        this.bytesArray = bytesArray;
        this.offset = bytesArray.offset();
        this.length = bytesArray.length();
        this.writeIndex = writeIndex;
        this.readIndex = readIndex;
        this.writeBuffer = ByteBuffer.wrap(bytesArray.array(), offset, bytesArray.length());
        this.readBuffer = ByteBuffer.wrap(bytesArray.array(), offset, bytesArray.length());
        initializePositions(offset, writeIndex, readIndex, writeBuffer, readBuffer);
    }

    public static ByteBufferReference heapBuffer(BytesArray bytesArray) {
        return heapBuffer(bytesArray, 0, 0);
    }

    public static ByteBufferReference heapBuffer(BytesArray bytesArray, int writeIndex, int readIndex) {
        if (readIndex > writeIndex) {
            throw new IndexOutOfBoundsException("Read index [" + readIndex + "] was greater than write index [" + writeIndex + "]");
        }
        return new ByteBufferReference(bytesArray, writeIndex, readIndex);
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
        BytesArray newBytesArray = (BytesArray) bytesArray.slice(from, length);

        int newReadIndex = Math.min(Math.max(readIndex - from, 0), length);
        int newWriteIndex = Math.min(Math.max(writeIndex - from, 0), length);

        return heapBuffer(newBytesArray, newWriteIndex, newReadIndex);
    }

    @Override
    public BytesRef toBytesRef() {
        return bytesArray.toBytesRef();
    }

    @Override
    public long ramBytesUsed() {
        return readBuffer.capacity();
    }

    @Override
    public boolean hasMultipleBuffers() {
        return false;
    }

    @Override
    public ByteBuffer getWriteByteBuffer() {
        writeBuffer.position(offset + writeIndex);
        writeBuffer.limit(offset + length);
        return writeBuffer;
    }

    @Override
    public ByteBuffer getReadByteBuffer() {
        readBuffer.position(offset + readIndex);
        readBuffer.limit(offset + length);
        return readBuffer;
    }

    @Override
    public ByteBuffer[] getWriteByteBuffers() {
        ByteBuffer[] byteBuffers = new ByteBuffer[1];
        byteBuffers[0] = writeBuffer;
        return byteBuffers;
    }

    @Override
    public ByteBuffer[] getReadByteBuffers() {
        ByteBuffer[] byteBuffers = new ByteBuffer[1];
        byteBuffers[0] = readBuffer;
        return byteBuffers;
    }

    private static void initializePositions(int offset, int writeIndex, int readIndex, ByteBuffer writeBuffer, ByteBuffer readBuffer) {
        writeBuffer.position(offset + writeIndex);
        readBuffer.position(offset + readIndex);
    }
}
