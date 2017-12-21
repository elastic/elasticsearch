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
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.junit.Before;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TcpTransportReadConsumerTests extends ESTestCase {

    private TcpTransportReadConsumer readConsumer;
    private TcpNioSocketChannel channel;
    private TcpReadHandler handler;
    private InboundChannelBuffer buffer;

    @Before
    public void init() {
        handler = mock(TcpReadHandler.class);
        channel = mock(TcpNioSocketChannel.class);
        readConsumer = new TcpTransportReadConsumer(handler, channel);
        buffer = new InboundChannelBuffer(() -> new InboundChannelBuffer.Page(ByteBuffer.allocate(1 << 14), ()-> {}));
    }

    public void testConsumeWithIncompleteHeader() throws IOException {
        ByteBuffer byteBuffer = buffer.sliceBuffersTo(6)[0];
        byteBuffer.put((byte) 'E');
        byteBuffer.put((byte) 'S');
        byteBuffer.put((byte) 1);
        byteBuffer.put((byte) 1);
        buffer.incrementIndex(4);

        assertEquals(0, readConsumer.consumeReads(buffer));

        verifyZeroInteractions(handler);
    }

    public void testDecodePing() throws IOException {
        ByteBuffer byteBuffer = buffer.sliceBuffersTo(6)[0];
        byteBuffer.put((byte) 'E');
        byteBuffer.put((byte) 'S');
        byteBuffer.putInt(-1);
        buffer.incrementIndex(6);

        assertEquals(6, readConsumer.consumeReads(buffer));

        verifyZeroInteractions(handler);
    }

    public void testDecodePingWithStartOfSecondMessage() throws IOException {
        ByteBuffer byteBuffer = buffer.sliceBuffersTo(8)[0];
        byteBuffer.put((byte) 'E');
        byteBuffer.put((byte) 'S');
        byteBuffer.putInt(-1);
        byteBuffer.put((byte) 'E');
        byteBuffer.put((byte) 'S');
        buffer.incrementIndex(8);

        assertEquals(6, readConsumer.consumeReads(buffer));

        verifyZeroInteractions(handler);
    }

    public void testDecodeMessage() throws IOException {
        byte[] message = {'M', 'A'};

        ByteBuffer byteBuffer = buffer.sliceBuffersTo(8)[0];
        byteBuffer.put((byte) 'E');
        byteBuffer.put((byte) 'S');
        byteBuffer.putInt(2);
        byteBuffer.put(message);
        buffer.incrementIndex(8);

        assertEquals(8, readConsumer.consumeReads(buffer));

        verify(handler).handleMessage(new BytesArray(message), channel, 2);
    }

    public void testDecodeIncompleteMessage() throws IOException {
        byte[] message = {'M', 'A'};

        ByteBuffer byteBuffer = buffer.sliceBuffersTo(8)[0];
        byteBuffer.put((byte) 'E');
        byteBuffer.put((byte) 'S');
        byteBuffer.putInt(3);
        byteBuffer.put(message);
        buffer.incrementIndex(8);

        assertEquals(0, readConsumer.consumeReads(buffer));

        verifyZeroInteractions(handler);
    }

    public void testInvalidLength() throws IOException {
        byte[] message = {'M', 'A'};

        ByteBuffer byteBuffer = buffer.sliceBuffersTo(8)[0];
        byteBuffer.put((byte) 'E');
        byteBuffer.put((byte) 'S');
        byteBuffer.putInt(-2);
        byteBuffer.put(message);
        buffer.incrementIndex(8);

        StreamCorruptedException e = expectThrows(StreamCorruptedException.class, () -> readConsumer.consumeReads(buffer));
        assertEquals("invalid data length: -2", e.getMessage());

        verifyZeroInteractions(handler);
    }

    public void testInvalidHeader() throws IOException {
        ByteBuffer byteBuffer = buffer.sliceBuffersTo(7)[0];
        byteBuffer.put((byte) 'E');
        byteBuffer.put((byte) 'C');
        byte byte1 = randomByte();
        byte byte2 = randomByte();
        byteBuffer.put(byte1);
        byteBuffer.put(byte2);
        byteBuffer.put(randomByte());
        byteBuffer.put(randomByte());
        byteBuffer.put(randomByte());
        buffer.incrementIndex(7);

        StreamCorruptedException ex = expectThrows(StreamCorruptedException.class, () -> readConsumer.consumeReads(buffer));
        String expected = "invalid internal transport message format, got (45,43,"
            + Integer.toHexString(byte1 & 0xFF) + ","
            + Integer.toHexString(byte2 & 0xFF) + ")";
        assertEquals(expected, ex.getMessage());

        verifyZeroInteractions(handler);
    }

    public void testHTTPHeader() throws IOException {
        String[] httpHeaders = {"GET", "POST", "PUT", "HEAD", "DELETE", "OPTIONS", "PATCH", "TRACE"};

        for (String httpHeader : httpHeaders) {
            ByteBuffer byteBuffer = buffer.sliceBuffersFrom(buffer.getIndex())[0];
            for (char c : httpHeader.toCharArray()) {
                byteBuffer.put((byte) c);
                buffer.incrementIndex(1);
            }
            buffer.incrementIndex(5);

            TcpTransport.HttpOnTransportException ex = expectThrows(TcpTransport.HttpOnTransportException.class,
                () -> readConsumer.consumeReads(buffer));

            assertEquals("This is not a HTTP port", ex.getMessage());
        }
    }
}
