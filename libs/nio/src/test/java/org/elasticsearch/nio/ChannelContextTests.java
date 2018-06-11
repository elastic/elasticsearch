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
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ChannelContextTests extends ESTestCase {

    private TestChannelContext context;
    private Consumer<Exception> exceptionHandler;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() throws Exception {
        super.setUp();
        exceptionHandler = mock(Consumer.class);
    }

    public void testCloseSuccess() throws IOException {
        FakeRawChannel rawChannel = new FakeRawChannel(null);
        context = new TestChannelContext(rawChannel, exceptionHandler);

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        context.addCloseListener((v, t) -> {
            if (t == null) {
                listenerCalled.compareAndSet(false, true);
            } else {
                throw new AssertionError("Close should not fail");
            }
        });

        assertFalse(rawChannel.hasCloseBeenCalled());
        assertTrue(context.isOpen());
        assertFalse(listenerCalled.get());
        context.closeFromSelector();
        assertTrue(rawChannel.hasCloseBeenCalled());
        assertFalse(context.isOpen());
        assertTrue(listenerCalled.get());
    }

    public void testCloseException() throws IOException {
        IOException ioException = new IOException("boom");
        FakeRawChannel rawChannel = new FakeRawChannel(ioException);
        context = new TestChannelContext(rawChannel, exceptionHandler);

        AtomicReference<Exception> exception = new AtomicReference<>();
        context.addCloseListener((v, t) -> {
            if (t == null) {
                throw new AssertionError("Close should not fail");
            } else {
                exception.set(t);
            }
        });

        assertFalse(rawChannel.hasCloseBeenCalled());
        assertTrue(context.isOpen());
        assertNull(exception.get());
        expectThrows(IOException.class, context::closeFromSelector);
        assertTrue(rawChannel.hasCloseBeenCalled());
        assertFalse(context.isOpen());
        assertSame(ioException, exception.get());
    }

    public void testExceptionsAreDelegatedToHandler() {
        context = new TestChannelContext(new FakeRawChannel(null), exceptionHandler);
        IOException exception = new IOException();
        context.handleException(exception);
        verify(exceptionHandler).accept(exception);
    }

    private static class TestChannelContext extends ChannelContext<FakeRawChannel> {

        private TestChannelContext(FakeRawChannel channel, Consumer<Exception> exceptionHandler) {
            super(channel, exceptionHandler);
        }

        @Override
        public void closeChannel() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public NioSelector getSelector() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public NioChannel getChannel() {
            throw new UnsupportedOperationException("not implemented");
        }
    }

    private class FakeRawChannel extends SelectableChannel implements NetworkChannel {

        private final IOException exceptionOnClose;
        private AtomicBoolean hasCloseBeenCalled = new AtomicBoolean(false);

        private FakeRawChannel(IOException exceptionOnClose) {
            this.exceptionOnClose = exceptionOnClose;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            hasCloseBeenCalled.compareAndSet(false, true);
            if (exceptionOnClose != null) {
                throw exceptionOnClose;
            }
        }

        private boolean hasCloseBeenCalled() {
            return hasCloseBeenCalled.get();
        }

        @Override
        public NetworkChannel bind(SocketAddress local) throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public SocketAddress getLocalAddress() throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public <T> NetworkChannel setOption(SocketOption<T> name, T value) throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public <T> T getOption(SocketOption<T> name) throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public Set<SocketOption<?>> supportedOptions() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public SelectorProvider provider() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public int validOps() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public boolean isRegistered() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public SelectionKey keyFor(Selector sel) {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public SelectionKey register(Selector sel, int ops, Object att) throws ClosedChannelException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public SelectableChannel configureBlocking(boolean block) throws IOException {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public boolean isBlocking() {
            throw new UnsupportedOperationException("not implemented");
        }

        @Override
        public Object blockingLock() {
            throw new UnsupportedOperationException("not implemented");
        }
    }
}
