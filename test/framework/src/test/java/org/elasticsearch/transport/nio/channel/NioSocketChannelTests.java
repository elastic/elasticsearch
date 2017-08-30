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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.SocketEventHandler;
import org.elasticsearch.transport.nio.SocketSelector;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NioSocketChannelTests extends ESTestCase {

    private SocketSelector selector;
    private AtomicBoolean closedRawChannel;
    private Thread thread;

    @Before
    @SuppressWarnings("unchecked")
    public void startSelector() throws IOException {
        selector = new SocketSelector(new SocketEventHandler(logger, mock(BiConsumer.class)));
        thread = new Thread(selector::runLoop);
        closedRawChannel = new AtomicBoolean(false);
        thread.start();
        selector.isRunningFuture().actionGet();
    }

    @After
    public void stopSelector() throws IOException, InterruptedException {
        selector.close();
        thread.join();
    }

    public void testClose() throws IOException, TimeoutException, InterruptedException {
        AtomicReference<NioChannel> ref = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        NioSocketChannel socketChannel = new DoNotCloseChannel(NioChannel.CLIENT, mock(SocketChannel.class), selector);
        socketChannel.setContexts(mock(ReadContext.class), mock(WriteContext.class));
        Consumer<NioChannel> listener = (c) -> {
            ref.set(c);
            latch.countDown();
        };
        socketChannel.getCloseFuture().addListener(ActionListener.wrap(listener::accept, (e) -> listener.accept(socketChannel)));
        CloseFuture closeFuture = socketChannel.getCloseFuture();

        assertFalse(closeFuture.isClosed());
        assertFalse(closedRawChannel.get());

        socketChannel.closeAsync();

        closeFuture.awaitClose(100, TimeUnit.SECONDS);

        assertTrue(closedRawChannel.get());
        assertTrue(closeFuture.isClosed());
        latch.await();
        assertSame(socketChannel, ref.get());
    }

    public void testConnectSucceeds() throws IOException, InterruptedException {
        SocketChannel rawChannel = mock(SocketChannel.class);
        when(rawChannel.finishConnect()).thenReturn(true);
        NioSocketChannel socketChannel = new DoNotCloseChannel(NioChannel.CLIENT, rawChannel, selector);
        socketChannel.setContexts(mock(ReadContext.class), mock(WriteContext.class));
        selector.scheduleForRegistration(socketChannel);

        ConnectFuture connectFuture = socketChannel.getConnectFuture();
        assertTrue(connectFuture.awaitConnectionComplete(100, TimeUnit.SECONDS));

        assertTrue(socketChannel.isConnectComplete());
        assertTrue(socketChannel.isOpen());
        assertFalse(closedRawChannel.get());
        assertFalse(connectFuture.connectFailed());
        assertNull(connectFuture.getException());
    }

    public void testConnectFails() throws IOException, InterruptedException {
        SocketChannel rawChannel = mock(SocketChannel.class);
        when(rawChannel.finishConnect()).thenThrow(new ConnectException());
        NioSocketChannel socketChannel = new DoNotCloseChannel(NioChannel.CLIENT, rawChannel, selector);
        socketChannel.setContexts(mock(ReadContext.class), mock(WriteContext.class));
        selector.scheduleForRegistration(socketChannel);

        ConnectFuture connectFuture = socketChannel.getConnectFuture();
        assertFalse(connectFuture.awaitConnectionComplete(100, TimeUnit.SECONDS));

        assertFalse(socketChannel.isConnectComplete());
        // Even if connection fails the channel is 'open' until close() is called
        assertTrue(socketChannel.isOpen());
        assertTrue(connectFuture.connectFailed());
        assertThat(connectFuture.getException(), instanceOf(ConnectException.class));
    }

    private class DoNotCloseChannel extends DoNotRegisterChannel {

        private DoNotCloseChannel(String profile, SocketChannel channel, SocketSelector selector) throws IOException {
            super(profile, channel, selector);
        }

        @Override
        void closeRawChannel() throws IOException {
            closedRawChannel.set(true);
        }
    }
}
