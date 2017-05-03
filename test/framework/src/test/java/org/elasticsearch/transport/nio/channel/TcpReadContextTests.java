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

package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.nio.TcpReadHandler;
import org.junit.Before;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TcpReadContextTests extends ESTestCase {

    private static String PROFILE = "profile";
    private TcpReadHandler handler;
    private int messageLength;
    private ReadChannelImpl channel;
    private NewTcpReadContext readContext;

    @Before
    public void init() throws IOException {
        handler = mock(TcpReadHandler.class);

        messageLength = randomInt(96) + 4;
        channel = new ReadChannelImpl();
        readContext = new NewTcpReadContext(channel, handler);
    }

    public void testSuccessfulRead() throws IOException {
        byte[] bytes = createMessage(messageLength);
        byte[] fullMessage = combineMessageAndHeader(bytes);

        channel.setBytes(fullMessage);

        readContext.read();

        verify(handler).handleMessage(new BytesArray(bytes), channel, PROFILE, messageLength);
    }

//    public void testPartialHeaderRead() throws IOException {
//        byte[] fullMessage = createMessage(messageLength);
//        byte[] fullMessageWithHeader = combineMessageAndHeader(fullMessage);
//        ByteBuffer buffer = ByteBuffer.wrap(fullMessageWithHeader);
//        byte[] part1 = new byte[5];
//        byte[] part2 = new byte[fullMessageWithHeader.length - 5];
//        buffer.get(part1);
//        buffer.get(part2);
//
//        channel.setBytes(part1);
//
//        readContext.read();
//
//        verifyZeroInteractions(handler);
//
//        channel.setBytes(part2);
//
//        readContext.read();
//
//        verify(handler).handleMessage(new BytesArray(fullMessage), channel, PROFILE, messageLength);
//    }
//
//    public void testPartialMessageRead() throws IOException {
//        byte[] part1 = createMessage(messageLength);
//        byte[] fullPart1 = combineMessageAndHeader(part1, messageLength + messageLength);
//        byte[] part2 = createMessage(messageLength);
//
//        channel.setBytes(fullPart1);
//
//        readContext.read();
//
//        verifyZeroInteractions(handler);
//
//        channel.setBytes(part2);
//
//        readContext.read();
//
//        CompositeBytesReference reference = new CompositeBytesReference(new BytesArray(part1), new BytesArray(part2));
//        verify(handler).handleMessage(reference, channel, PROFILE, messageLength + messageLength);
//    }

    public void testReadThrowsIOException() throws IOException {
        byte[] fullMessage = combineMessageAndHeader(createMessage(messageLength));

        channel.setBytes(fullMessage);
        channel.setThrowException(true);

        try {
            readContext.read();
            fail("Expected exception");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(IOException.class));
            assertEquals("Boom!", ex.getMessage());
        }
    }

    private static byte[] combineMessageAndHeader(byte[] bytes) {
        return combineMessageAndHeader(bytes, bytes.length);
    }

    private static byte[] combineMessageAndHeader(byte[] bytes, int messageLength) {
        byte[] fullMessage = new byte[bytes.length + 6];
        ByteBuffer wrapped = ByteBuffer.wrap(fullMessage);
        wrapped.put((byte) 'E');
        wrapped.put((byte) 'S');
        wrapped.putInt(messageLength);
        wrapped.put(bytes);
        return fullMessage;
    }

    private static byte[] createMessage(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = randomByte();
        }
        return bytes;
    }

    private static byte[] createPing() {
        byte[] fullMessage = new byte[6];
        ByteBuffer wrapped = ByteBuffer.wrap(fullMessage);
        wrapped.put((byte) 'E');
        wrapped.put((byte) 'S');
        wrapped.putInt(-1);
        return fullMessage;
    }

    private static class ReadChannelImpl extends NioSocketChannel {

        private byte[] bytes;
        private int currentPosition = 0;
        private boolean shouldThrow;

        ReadChannelImpl() throws IOException {
            this(mock(SocketChannel.class));
        }

        ReadChannelImpl(SocketChannel socketChannel) throws IOException {
            super(PROFILE, socketChannel);
        }

        @Override
        public int read(ByteBuffer buffer) throws IOException {
            if (shouldThrow) {
                throw new IOException("Boom!");
            }

            int startPosition = currentPosition;
            while (buffer.hasRemaining() && currentPosition != bytes.length) {
                buffer.put(bytes[currentPosition++]);
            }
            return currentPosition - startPosition;
        }

        @Override
        public long vectorizedRead(ByteBuffer[] buffers) throws IOException {
            if (shouldThrow) {
                throw new IOException("Boom!");
            }

            int startPosition = currentPosition;
            while (buffers[0].hasRemaining() && currentPosition != bytes.length) {
                buffers[0].put(bytes[currentPosition++]);
            }
            return currentPosition - startPosition;
        }

        void setBytes(byte[] bytes) {
            this.currentPosition = 0;
            this.bytes = bytes;
        }

        void setThrowException(boolean shouldThrow) {
            this.shouldThrow = shouldThrow;
        }
    }
}
