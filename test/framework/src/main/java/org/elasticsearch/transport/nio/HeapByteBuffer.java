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

public class HeapByteBuffer extends BytesReference {

    private final BytesArray bytesArray;
    private int writeIndex;
    private int readIndex;
    private final ByteBuffer writeBuffer;
    private final ByteBuffer readBuffer;

    public HeapByteBuffer(BytesArray bytesArray) {
        this(bytesArray, 0, 0);
    }

    public HeapByteBuffer(BytesArray bytesArray, int writeIndex, int readIndex) {
        this.bytesArray = bytesArray;
        this.writeIndex = writeIndex;
        this.readIndex = readIndex;
        this.writeBuffer = ByteBuffer.wrap(bytesArray.array(), bytesArray.offset() + writeIndex, bytesArray.length());
        this.readBuffer = ByteBuffer.wrap(bytesArray.array(), bytesArray.offset() + writeIndex, bytesArray.length());
    }

    @Override
    public byte get(int index) {
        return bytesArray.get(index);
    }

    @Override
    public int length() {
        return bytesArray.length();
    }

    @Override
    public HeapByteBuffer slice(int from, int length) {
        if (from < 0 || (from + length) > bytesArray.length()) {
            throw new IllegalArgumentException("can't slice a buffer with length [" + bytesArray.length() +
                "], with slice parameters from [" + from + "], length [" + length + "]");
        }
        BytesArray slice = new BytesArray(bytesArray.array(), bytesArray.offset() + from, length);
        int newReadIndex = Math.min(Math.max(readIndex - from, 0), length);
        int newWriteIndex = Math.min(Math.max(writeIndex - from, 0), length);
        return new HeapByteBuffer(slice, newReadIndex, newWriteIndex);
    }

    @Override
    public BytesRef toBytesRef() {
        return bytesArray.toBytesRef();
    }

    @Override
    public long ramBytesUsed() {
        return bytesArray.ramBytesUsed();
    }

    public int getWriteIndex() {
        return writeIndex;
    }

    public void incrementWrite(int delta) {
        writeIndex += delta;
    }

    public int getReadIndex() {
        return readIndex;
    }

    public void incrementRead(int delta) {
        readIndex += delta;
    }

    public ByteBuffer getWriteByteBuffer() {
        writeBuffer.position(bytesArray.offset() + writeIndex);
        return writeBuffer;
    }

    public ByteBuffer getReadByteBuffer() {
        readBuffer.position(bytesArray.offset() + readIndex);
        return readBuffer;
    }
}
