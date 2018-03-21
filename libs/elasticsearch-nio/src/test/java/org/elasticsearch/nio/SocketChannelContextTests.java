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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SocketChannelContextTests extends ESTestCase {

    private SocketChannel rawChannel;
    private TestSocketChannelContext context;
    private Consumer<Exception> exceptionHandler;
    private NioSocketChannel channel;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        super.setUp();

        rawChannel = mock(SocketChannel.class);
        channel = mock(NioSocketChannel.class);
        when(channel.getRawChannel()).thenReturn(rawChannel);
        exceptionHandler = mock(Consumer.class);
        context = new TestSocketChannelContext(channel, mock(SocketSelector.class), exceptionHandler);
    }

    public void testIOExceptionSetIfEncountered() throws IOException {
        when(rawChannel.write(any(ByteBuffer[].class), anyInt(), anyInt())).thenThrow(new IOException());
        when(rawChannel.write(any(ByteBuffer.class))).thenThrow(new IOException());
        when(rawChannel.read(any(ByteBuffer[].class), anyInt(), anyInt())).thenThrow(new IOException());
        when(rawChannel.read(any(ByteBuffer.class))).thenThrow(new IOException());
        assertFalse(context.hasIOException());
        expectThrows(IOException.class, () -> {
            if (randomBoolean()) {
                context.read();
            } else {
                context.flushChannel();
            }
        });
        assertTrue(context.hasIOException());
    }

    public void testSignalWhenPeerClosed() throws IOException {
        when(rawChannel.read(any(ByteBuffer[].class), anyInt(), anyInt())).thenReturn(-1L);
        when(rawChannel.read(any(ByteBuffer.class))).thenReturn(-1);
        assertFalse(context.isPeerClosed());
        context.read();
        assertTrue(context.isPeerClosed());
    }

    public void testConnectSucceeds() throws IOException {
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        when(rawChannel.finishConnect()).thenReturn(false, true);

        context.addConnectListener((v, t) -> {
            if (t == null) {
                listenerCalled.compareAndSet(false, true);
            } else {
                throw new AssertionError("Connection should not fail");
            }
        });

        assertFalse(context.connect());
        assertFalse(context.isConnectComplete());
        assertFalse(listenerCalled.get());
        assertTrue(context.connect());
        assertTrue(context.isConnectComplete());
        assertTrue(listenerCalled.get());
    }

    public void testConnectFails() throws IOException {
        AtomicReference<Exception> exception = new AtomicReference<>();
        IOException ioException = new IOException("boom");
        when(rawChannel.finishConnect()).thenReturn(false).thenThrow(ioException);

        context.addConnectListener((v, t) -> {
            if (t == null) {
                throw new AssertionError("Connection should not succeed");
            } else {
                exception.set((Exception) t);
            }
        });

        assertFalse(context.connect());
        assertFalse(context.isConnectComplete());
        assertNull(exception.get());
        expectThrows(IOException.class, context::connect);
        assertFalse(context.isConnectComplete());
        assertSame(ioException, exception.get());
    }

    private static class TestSocketChannelContext extends SocketChannelContext {

        private TestSocketChannelContext(NioSocketChannel channel, SocketSelector selector, Consumer<Exception> exceptionHandler) {
            super(channel, selector, exceptionHandler);
        }

        @Override
        public int read() throws IOException {
            if (randomBoolean()) {
                ByteBuffer[] byteBuffers = {ByteBuffer.allocate(10)};
                return readFromChannel(byteBuffers);
            } else {
                return readFromChannel(ByteBuffer.allocate(10));
            }
        }

        @Override
        public void sendMessage(ByteBuffer[] buffers, BiConsumer<Void, Throwable> listener) {

        }

        @Override
        public void queueWriteOperation(WriteOperation writeOperation) {

        }

        @Override
        public void flushChannel() throws IOException {
            if (randomBoolean()) {
                ByteBuffer[] byteBuffers = {ByteBuffer.allocate(10)};
                flushToChannel(byteBuffers);
            } else {
                flushToChannel(ByteBuffer.allocate(10));
            }
        }

        @Override
        public boolean hasQueuedWriteOps() {
            return false;
        }

        @Override
        public boolean selectorShouldClose() {
            return false;
        }

        @Override
        public void closeChannel() {

        }
    }
}
