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
import java.util.Iterator;

public class NetworkBytesReference extends BytesReference {

    private final BytesArray bytesArray;
    private final ByteBuffer writeBuffer;
    private final ByteBuffer readBuffer;

    private int writeIndex;
    private int readIndex;

    public NetworkBytesReference(BytesArray bytesArray, int writeIndex, int readIndex) {
        this.bytesArray = bytesArray;
        this.writeIndex = writeIndex;
        this.readIndex = readIndex;
        this.writeBuffer = ByteBuffer.wrap(bytesArray.array());
        this.readBuffer = ByteBuffer.wrap(bytesArray.array());
    }

    public static NetworkBytesReference wrap(BytesArray bytesArray) {
        return wrap(bytesArray, 0, 0);
    }

    public static NetworkBytesReference wrap(BytesArray bytesArray, int writeIndex, int readIndex) {
        if (readIndex > writeIndex) {
            throw new IndexOutOfBoundsException("Read index [" + readIndex + "] was greater than write index [" + writeIndex + "]");
        }
        return new NetworkBytesReference(bytesArray, writeIndex, readIndex);
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
    public NetworkBytesReference slice(int from, int length) {
        BytesReference ref = bytesArray.slice(from, length);
        BytesArray newBytesArray;
        if (ref instanceof BytesArray) {
            newBytesArray = (BytesArray) ref;
        } else {
            newBytesArray = new BytesArray(ref.toBytesRef());
        }

        int newReadIndex = Math.min(Math.max(readIndex - from, 0), length);
        int newWriteIndex = Math.min(Math.max(writeIndex - from, 0), length);

        return wrap(newBytesArray, newWriteIndex, newReadIndex);
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
        int newWriteIndex = writeIndex + delta;
        if (newWriteIndex > bytesArray.length()) {
            throw new IndexOutOfBoundsException("New write index [" + newWriteIndex + "] would be greater than length" +
                " [" + bytesArray.length() + "]");
        }

        writeIndex = newWriteIndex;
    }

    public int getWriteRemaining() {
        return bytesArray.length() - writeIndex;
    }

    public boolean hasWriteRemaining() {
        return getWriteRemaining() > 0;
    }

    public int getReadIndex() {
        return readIndex;
    }

    public void incrementRead(int delta) {
        int newReadIndex = readIndex + delta;
        if (newReadIndex > writeIndex) {
            throw new IndexOutOfBoundsException("New read index [" + newReadIndex + "] would be greater than write" +
                " index [" + writeIndex + "]");
        }
        readIndex = newReadIndex;
    }

    public int getReadRemaining() {
        return writeIndex - readIndex;
    }

    public boolean hasReadRemaining() {
        return getReadRemaining() > 0;
    }

    public ByteBuffer getWriteByteBuffer() {
        writeBuffer.position(bytesArray.offset() + writeIndex);
        writeBuffer.limit(bytesArray.offset() + bytesArray.length());
        return writeBuffer;
    }

    public ByteBuffer getReadByteBuffer() {
        readBuffer.position(bytesArray.offset() + readIndex);
        readBuffer.limit(bytesArray.offset() + writeIndex);
        return readBuffer;
    }

    public static void vectorizedIncrementReadIndexes(Iterable<NetworkBytesReference> references, int delta) {
        Iterator<NetworkBytesReference> refs = references.iterator();
        while (delta != 0) {
            NetworkBytesReference ref = refs.next();
            int amountToInc = Math.min(ref.getReadRemaining(), delta);
            ref.incrementRead(amountToInc);
            delta -= amountToInc;
        }
    }
}
