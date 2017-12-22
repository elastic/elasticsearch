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

package org.elasticsearch.nio;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiConsumer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteOperationTests extends ESTestCase {

    private NioSocketChannel channel;
    private BiConsumer<Void, Throwable> listener;

    @Before
    @SuppressWarnings("unchecked")
    public void setFields() {
        channel = mock(NioSocketChannel.class);
        listener = mock(BiConsumer.class);

    }

    public void testFlush() throws IOException {
        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        WriteOperation writeOp = new WriteOperation(channel, buffers, listener);


        when(channel.write(any(ByteBuffer[].class))).thenReturn(10);

        writeOp.flush();

        assertTrue(writeOp.isFullyFlushed());
    }

    public void testPartialFlush() throws IOException {
        ByteBuffer[] buffers = {ByteBuffer.allocate(10)};
        WriteOperation writeOp = new WriteOperation(channel, buffers, listener);

        when(channel.write(any(ByteBuffer[].class))).thenReturn(5);

        writeOp.flush();

        assertFalse(writeOp.isFullyFlushed());
    }

    public void testMultipleFlushesWithCompositeBuffer() throws IOException {
        ByteBuffer[] buffers = {ByteBuffer.allocate(10), ByteBuffer.allocate(15), ByteBuffer.allocate(3)};
        WriteOperation writeOp = new WriteOperation(channel, buffers, listener);

        ArgumentCaptor<ByteBuffer[]> buffersCaptor = ArgumentCaptor.forClass(ByteBuffer[].class);

        when(channel.write(buffersCaptor.capture())).thenReturn(5)
            .thenReturn(5)
            .thenReturn(2)
            .thenReturn(15)
            .thenReturn(1);

        writeOp.flush();
        assertFalse(writeOp.isFullyFlushed());
        writeOp.flush();
        assertFalse(writeOp.isFullyFlushed());
        writeOp.flush();
        assertFalse(writeOp.isFullyFlushed());
        writeOp.flush();
        assertFalse(writeOp.isFullyFlushed());
        writeOp.flush();
        assertTrue(writeOp.isFullyFlushed());

        List<ByteBuffer[]> values = buffersCaptor.getAllValues();
        ByteBuffer[] byteBuffers = values.get(0);
        assertEquals(3, byteBuffers.length);
        assertEquals(10, byteBuffers[0].remaining());

        byteBuffers = values.get(1);
        assertEquals(3, byteBuffers.length);
        assertEquals(5, byteBuffers[0].remaining());

        byteBuffers = values.get(2);
        assertEquals(2, byteBuffers.length);
        assertEquals(15, byteBuffers[0].remaining());

        byteBuffers = values.get(3);
        assertEquals(2, byteBuffers.length);
        assertEquals(13, byteBuffers[0].remaining());

        byteBuffers = values.get(4);
        assertEquals(1, byteBuffers.length);
        assertEquals(1, byteBuffers[0].remaining());
    }
}
