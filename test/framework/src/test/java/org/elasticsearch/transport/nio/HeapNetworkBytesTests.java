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

import org.elasticsearch.common.bytes.AbstractBytesReferenceTestCase;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.BytesPage;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HeapNetworkBytesTests extends AbstractBytesReferenceTestCase {

    private HeapNetworkBytes buffer;

    public void testBasicGetByte() {
        byte[] bytes = new byte[10];
        initializeBytes(bytes);
        buffer = HeapNetworkBytes.wrap(new BytesArray(bytes));

        assertEquals(10, buffer.length());
        for (int i = 0 ; i < bytes.length; ++i) {
            assertEquals(i, buffer.get(i));
        }
    }

    public void testBasicGetByteWithOffset() {
        byte[] bytes = new byte[10];
        initializeBytes(bytes);
        buffer = HeapNetworkBytes.wrap(new BytesArray(bytes, 2, 8));

        assertEquals(8, buffer.length());
        for (int i = 2 ; i < bytes.length; ++i) {
            assertEquals(i, buffer.get(i - 2));
        }
    }

    public void testBasicGetByteWithOffsetAndLimit() {
        byte[] bytes = new byte[10];
        initializeBytes(bytes);
        buffer = HeapNetworkBytes.wrap(new BytesArray(bytes, 2, 6));

        assertEquals(6, buffer.length());
        for (int i = 2 ; i < bytes.length - 2; ++i) {
            assertEquals(i, buffer.get(i - 2));
        }
    }

    public void testGetWriteBufferRespectsIndex() {
        byte[] bytes = new byte[10];

        buffer = HeapNetworkBytes.wrap(new BytesArray(bytes, 2, 8));

        ByteBuffer writeByteBuffer = buffer.postIndexByteBuffer();

        assertEquals(2, writeByteBuffer.position());
        assertEquals(10, writeByteBuffer.limit());

        buffer.incrementIndex(2);

        writeByteBuffer = buffer.postIndexByteBuffer();
        assertEquals(4, writeByteBuffer.position());
        assertEquals(10, writeByteBuffer.limit());
    }

    public void testGetReadBufferRespectsReadIndex() {
        byte[] bytes = new byte[10];

        buffer = HeapNetworkBytes.wrap(new BytesArray(bytes, 3, 6));
        buffer.incrementIndex(3);

        ByteBuffer readByteBuffer = buffer.preIndexByteBuffer();

        assertEquals(3, readByteBuffer.position());
        assertEquals(6, readByteBuffer.limit());

        buffer.incrementIndex(2);

        readByteBuffer = buffer.preIndexByteBuffer();
        assertEquals(3, readByteBuffer.position());
        assertEquals(8, readByteBuffer.limit());
    }

    public void testWriteAndReadRemaining() {
        byte[] bytes = new byte[10];

        buffer = HeapNetworkBytes.wrap(new BytesArray(bytes, 2, 8));

        assertEquals(8, buffer.getRemaining());

        buffer.incrementIndex(3);

        assertEquals(5, buffer.getRemaining());
    }

    public void testBasicSlice() {
        byte[] bytes = new byte[20];
        initializeBytes(bytes);

        buffer = HeapNetworkBytes.wrap(new BytesArray(bytes, 2, 18));

        BytesReference slice = buffer.slice(4, 14);

        assertEquals(14, slice.length());

        for (int i = 6; i < 20; ++i) {
            assertEquals(i, slice.get(i - 6));
        }
    }

    public void testSliceAndRetainRespectsReadAndWriteIndexes() {
        byte[] bytes = new byte[20];
        initializeBytes(bytes);

        buffer = HeapNetworkBytes.wrap(new BytesArray(bytes, 2, 18));

        buffer.incrementIndex(9);

        NetworkBytesReference slice = buffer.sliceAndRetain(6, 12);

        assertEquals(12, slice.length());
        assertEquals(3, slice.getIndex());

        for (int i = 8; i < 20; ++i) {
            assertEquals(i, slice.get(i - 8));
        }
    }

    public void testClose() {
        byte[] bytes = new byte[20];
        initializeBytes(bytes);
        Releasable closer = mock(Releasable.class);
        BytesPage bytesPage = new BytesPage(bytes, closer);

        HeapNetworkBytes heapNetworkBytes = HeapNetworkBytes.fromBytesPage(bytesPage);

        heapNetworkBytes.close();
        verify(closer).close();
    }

    public void testCannotCloseTwice() {
        byte[] bytes = new byte[20];
        initializeBytes(bytes);
        Releasable closer = mock(Releasable.class);
        BytesPage bytesPage = new BytesPage(bytes, closer);

        HeapNetworkBytes heapNetworkBytes = HeapNetworkBytes.fromBytesPage(bytesPage);

        heapNetworkBytes.close();
        IllegalStateException exception = expectThrows(IllegalStateException.class, heapNetworkBytes::close);
        assertEquals("Attempting to close NetworkBytesReference that is already closed.",exception.getMessage());

        verify(closer, times(1)).close();
    }

    public void testSliceAndRetainRetainsReleasable() {
        byte[] bytes = new byte[20];
        initializeBytes(bytes);
        Releasable closer = mock(Releasable.class);
        BytesPage bytesPage = new BytesPage(bytes, closer);

        HeapNetworkBytes heapNetworkBytes = HeapNetworkBytes.fromBytesPage(bytesPage);

        NetworkBytesReference child1 = heapNetworkBytes.sliceAndRetain(0, 8);

        child1.close();
        verify(closer).close();
    }

    private void initializeBytes(byte[] bytes) {
        for (int i = 0 ; i < bytes.length; ++i) {
            bytes[i] = (byte) i;
        }
    }

    @Override
    protected BytesReference newBytesReference(int length) throws IOException {
        return HeapNetworkBytes.wrap(newBytesArrayReference(length, randomInt(length)));
    }

    @Override
    protected BytesReference newBytesReferenceWithOffsetOfZero(int length) throws IOException {
        return HeapNetworkBytes.wrap(newBytesArrayReference(length, 0));
    }

    @Override
    public void testArrayOffset() throws IOException {
        int length = randomInt(PAGE_SIZE * randomIntBetween(2, 5));
        HeapNetworkBytes pbr = (HeapNetworkBytes) newBytesReferenceWithOffsetOfZero(length);
        assertEquals(0, pbr.iterator().next().offset);
    }

    public static BytesArray newBytesArrayReference(int length, int offset) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput(length + offset);
        for (int i = 0; i < length + offset; i++) {
            out.writeByte((byte) random().nextInt(1 << 8));
        }
        return new BytesArray(out.bytes().toBytesRef().bytes, offset, length);
    }
}
