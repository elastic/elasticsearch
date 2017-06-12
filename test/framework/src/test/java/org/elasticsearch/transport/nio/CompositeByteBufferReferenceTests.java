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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

public class CompositeByteBufferReferenceTests extends ESTestCase {

    private CompositeByteBufferReference buffer;

    @Before
    public void setBuffer() {
        buffer = new CompositeByteBufferReference();
    }

    public void testGetValuesAtIndexes() {
        buffer.addBuffer(newBuffer(0, 10));
        buffer.addBuffer(newBuffer(10, 10));

        assertEquals(20, buffer.length());
        for (int i = 0; i < 20; ++i) {
            assertEquals(i, buffer.get(i));
        }
    }

    public void testSlice() {
        buffer.addBuffer(newBuffer(0, 10));
        buffer.addBuffer(newBuffer(10, 10));

        BytesReference slice = buffer.slice(7, 20 - 7);

        assertEquals(13, slice.length());
        int j = 7;
        for (int i = 0; i < 13; ++i) {
            assertEquals(j++, slice.get(i));
        }
    }

    public void testDropUpToWithSplitBuffer() {
        buffer.addBuffer(newBuffer(0, 10));
        buffer.addBuffer(newBuffer(10, 10));

        buffer.dropUpTo(8);

        assertEquals(12, buffer.length());
        int j = 8;
        for (int i = 0; i < 12; ++i) {
            assertEquals(j++, buffer.get(i));
        }
    }

    public void testDropUpToAligned() {
        buffer.addBuffer(newBuffer(0, 10));
        buffer.addBuffer(newBuffer(10, 10));

        buffer.dropUpTo(10);

        assertEquals(10, buffer.length());
        int j = 10;
        for (int i = 0; i < 10; ++i) {
            assertEquals(j++, buffer.get(i));
        }
    }

    public void testDropUpToThatDropsBufferAndSplitLastBuffer() {
        buffer.addBuffer(newBuffer(0, 10));
        buffer.addBuffer(newBuffer(10, 10));

        buffer.dropUpTo(12);

        assertEquals(8, buffer.length());
        int j = 12;
        for (int i = 0; i < 8; ++i) {
            assertEquals(j++, buffer.get(i));
        }
    }

    public void testDropUpToUpdatesReadAndWriteIndexes() {
        buffer.addBuffer(newBuffer(0, 10));
        buffer.addBuffer(newBuffer(10, 10));

        buffer.incrementWrite(15);
        buffer.incrementRead(5);

        buffer.dropUpTo(12);

        assertEquals(8, buffer.length());
        assertEquals(0, buffer.getReadIndex());
        assertEquals(3, buffer.getWriteIndex());
    }

    public void testReadAndWriteIndexesFromAddingBuffers() {
        ByteBufferReference ref1 = newBuffer(0, 10);
        ref1.incrementWrite(10);
        ref1.incrementRead(5);
        buffer.addBuffer(ref1);

        ByteBufferReference ref2 = newBuffer(10, 10);
        ref2.incrementWrite(3);
        buffer.addBuffer(ref2);

        assertEquals(13, buffer.getWriteIndex());
        assertEquals(5, buffer.getReadIndex());
    }

    public void testIncrementReadAndWriteIndexesChangesUnderlyingBuffers() {
        ByteBufferReference ref1 = newBuffer(0, 10);
        buffer.addBuffer(ref1);

        ByteBufferReference ref2 = newBuffer(10, 10);
        buffer.addBuffer(ref2);

        assertEquals(0, ref1.getWriteIndex());
        assertEquals(0, ref1.getReadIndex());
        assertEquals(0, ref2.getWriteIndex());
        assertEquals(0, ref2.getReadIndex());

        buffer.incrementWrite(18);
        buffer.incrementRead(12);

        assertEquals(10, ref1.getWriteIndex());
        assertEquals(10, ref1.getReadIndex());
        assertEquals(8, ref2.getWriteIndex());
        assertEquals(2, ref2.getReadIndex());
    }

    public void testWriteSpaceMustBeContiguous() {
        ByteBufferReference ref1 = newBuffer(0, 10);
        ref1.incrementWrite(10);
        buffer.addBuffer(ref1);

        ByteBufferReference ref2 = newBuffer(10, 10);
        ref2.incrementWrite(5);
        ByteBufferReference ref3 = newBuffer(20, 10);
        ref3.incrementWrite(3);

        try {
            buffer.addBuffers(ref2, ref3);
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            assertEquals("The writable spaces must be contiguous across buffers.", e.getMessage());
        }

        assertEquals(10, buffer.length());
        assertEquals(10, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadIndex());
        assertEquals(0, buffer.getWriteByteBuffers().length);
        assertEquals(1, buffer.getReadByteBuffers().length);
    }

    public void testReadSpaceMustBeContiguous() {
        ByteBufferReference ref1 = newBuffer(0, 10);
        ref1.incrementWrite(10);
        ref1.incrementRead(10);
        buffer.addBuffer(ref1);

        ByteBufferReference ref2 = newBuffer(10, 10);
        ref2.incrementWrite(10);
        ref2.incrementRead(5);
        ByteBufferReference ref3 = newBuffer(20, 10);
        ref3.incrementWrite(3);
        ref3.incrementRead(3);

        try {
            buffer.addBuffers(ref2, ref3);
            fail("Should have thrown exception");
        } catch (IllegalArgumentException e) {
            assertEquals("The readable spaces must be contiguous across buffers.", e.getMessage());
        }

        assertEquals(10, buffer.length());
        assertEquals(10, buffer.getReadIndex());
        assertEquals(10, buffer.getWriteIndex());
        assertEquals(0, buffer.getReadByteBuffers().length);
        assertEquals(0, buffer.getWriteByteBuffers().length);
    }

    public void testGetReadBuffersEdgeCases() {
        assertEquals(0, buffer.getReadIndex());
        assertEquals(0, buffer.getReadByteBuffers().length);

        ByteBufferReference ref1 = newBuffer(0, 10);
        ref1.incrementWrite(10);
        ref1.incrementRead(10);

        buffer.addBuffer(ref1);

        assertEquals(10, buffer.getReadIndex());
        assertEquals(0, buffer.getReadByteBuffers().length);
    }

    public void testGetReadBuffersOnlyReturnsBuffersWithSpace() {
        ByteBufferReference ref1 = newBuffer(0, 10);
        ref1.incrementWrite(10);
        ref1.incrementRead(10);
        ByteBufferReference ref2 = newBuffer(10, 10);
        ref2.incrementWrite(10);
        ref2.incrementRead(5);
        ByteBufferReference ref3 = newBuffer(20, 10);
        ref3.incrementWrite(10);

        buffer.addBuffers(ref1, ref2, ref3);

        assertEquals(15, buffer.getReadIndex());
        assertEquals(2, buffer.getReadByteBuffers().length);
    }

    public void testGetWriteBuffersEdgeCases() {
        assertEquals(0, buffer.getWriteIndex());
        assertEquals(0, buffer.getWriteByteBuffers().length);

        ByteBufferReference ref1 = newBuffer(0, 10);
        ref1.incrementWrite(10);

        buffer.addBuffer(ref1);

        assertEquals(10, buffer.getWriteIndex());
        assertEquals(0, buffer.getWriteByteBuffers().length);
    }

    private ByteBufferReference newBuffer(int startInt, int size) {
        byte[] rawBytes = new byte[size];
        ByteBufferReference heapBuffer = ByteBufferReference.heapBuffer(new BytesArray(rawBytes));
        for (int i = 0; i < size; ++i) {
            rawBytes[i] = (byte) startInt++;
        }

        return heapBuffer;
    }


}
