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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;

public class ByteBufferReferenceTests extends ESTestCase {

    private NetworkBytesReference buffer;

    public void testBasicGetByte() {
        byte[] bytes = new byte[10];
        initializeBytes(bytes);
        buffer = NetworkBytesReference.wrap(new BytesArray(bytes));

        assertEquals(10, buffer.length());
        for (int i = 0 ; i < bytes.length; ++i) {
            assertEquals(i, buffer.get(i));
        }
    }

    public void testBasicGetByteWithOffset() {
        byte[] bytes = new byte[10];
        initializeBytes(bytes);
        buffer = NetworkBytesReference.wrap(new BytesArray(bytes, 2, 8));

        assertEquals(8, buffer.length());
        for (int i = 2 ; i < bytes.length; ++i) {
            assertEquals(i, buffer.get(i - 2));
        }
    }

    public void testBasicGetByteWithOffsetAndLimit() {
        byte[] bytes = new byte[10];
        initializeBytes(bytes);
        buffer = NetworkBytesReference.wrap(new BytesArray(bytes, 2, 6));

        assertEquals(6, buffer.length());
        for (int i = 2 ; i < bytes.length - 2; ++i) {
            assertEquals(i, buffer.get(i - 2));
        }
    }

    public void testGetWriteBufferRespectsWriteIndex() {
        byte[] bytes = new byte[10];

        buffer = NetworkBytesReference.wrap(new BytesArray(bytes, 2, 8));

        ByteBuffer writeByteBuffer = buffer.getWriteByteBuffer();

        assertEquals(2, writeByteBuffer.position());
        assertEquals(10, writeByteBuffer.limit());

        buffer.incrementWrite(2);

        writeByteBuffer = buffer.getWriteByteBuffer();
        assertEquals(4, writeByteBuffer.position());
        assertEquals(10, writeByteBuffer.limit());
    }

    public void testGetReadBufferRespectsReadIndex() {
        byte[] bytes = new byte[10];

        buffer = NetworkBytesReference.wrap(new BytesArray(bytes, 3, 6), 6, 0);

        ByteBuffer readByteBuffer = buffer.getReadByteBuffer();

        assertEquals(3, readByteBuffer.position());
        assertEquals(9, readByteBuffer.limit());

        buffer.incrementRead(2);

        readByteBuffer = buffer.getReadByteBuffer();
        assertEquals(5, readByteBuffer.position());
        assertEquals(9, readByteBuffer.limit());
    }

    public void testWriteAndReadRemaining() {
        byte[] bytes = new byte[10];

        buffer = NetworkBytesReference.wrap(new BytesArray(bytes, 2, 8));

        assertEquals(0, buffer.getReadRemaining());
        assertEquals(8, buffer.getWriteRemaining());

        buffer.incrementWrite(3);
        buffer.incrementRead(2);

        assertEquals(1, buffer.getReadRemaining());
        assertEquals(5, buffer.getWriteRemaining());
    }

    public void testBasicSlice() {
        byte[] bytes = new byte[20];
        initializeBytes(bytes);

        buffer = NetworkBytesReference.wrap(new BytesArray(bytes, 2, 18));

        NetworkBytesReference slice = buffer.slice(4, 14);

        assertEquals(14, slice.length());
        assertEquals(0, slice.getReadIndex());
        assertEquals(0, slice.getWriteIndex());

        for (int i = 6; i < 20; ++i) {
            assertEquals(i, slice.get(i - 6));
        }
    }

    public void testSliceWithReadAndWriteIndexes() {
        byte[] bytes = new byte[20];
        initializeBytes(bytes);

        buffer = NetworkBytesReference.wrap(new BytesArray(bytes, 2, 18));

        buffer.incrementWrite(9);
        buffer.incrementRead(5);

        NetworkBytesReference slice = buffer.slice(6, 12);

        assertEquals(12, slice.length());
        assertEquals(0, slice.getReadIndex());
        assertEquals(3, slice.getWriteIndex());

        for (int i = 8; i < 20; ++i) {
            assertEquals(i, slice.get(i - 8));
        }
    }

    private void initializeBytes(byte[] bytes) {
        for (int i = 0 ; i < bytes.length; ++i) {
            bytes[i] = (byte) i;
        }
    }
}
