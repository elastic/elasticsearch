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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.nio.OpenChannels;
import org.elasticsearch.transport.nio.SocketEventHandler;
import org.elasticsearch.transport.nio.SocketSelector;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NioSocketChannelTests extends ESTestCase {

    private SocketSelector selector;
    private AtomicBoolean closedRawChannel;
    private Thread thread;
    private OpenChannels openChannels;

    @Before
    @SuppressWarnings("unchecked")
    public void startSelector() throws IOException {
        openChannels = new OpenChannels(logger);
        selector = new SocketSelector(new SocketEventHandler(logger, mock(BiConsumer.class), openChannels));
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

    public void testClose() throws Exception {
        AtomicBoolean isClosed = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        NioSocketChannel socketChannel = new DoNotCloseChannel(mock(SocketChannel.class), selector);
        openChannels.clientChannelOpened(socketChannel);
        socketChannel.setContexts(mock(ReadContext.class), mock(WriteContext.class));
        socketChannel.addCloseListener(new ActionListener<Void>() {
            @Override
            public void onResponse(Void o) {
                isClosed.set(true);
                latch.countDown();
            }
            @Override
            public void onFailure(Exception e) {
                isClosed.set(true);
                latch.countDown();
            }
        });

        assertTrue(socketChannel.isOpen());
        assertFalse(closedRawChannel.get());
        assertFalse(isClosed.get());
        assertTrue(openChannels.getClientChannels().containsKey(socketChannel));

        PlainActionFuture<Void> closeFuture = PlainActionFuture.newFuture();
        socketChannel.addCloseListener(closeFuture);
        socketChannel.close();
        closeFuture.actionGet();

        assertTrue(closedRawChannel.get());
        assertFalse(socketChannel.isOpen());
        assertFalse(openChannels.getClientChannels().containsKey(socketChannel));
        latch.await();
        assertTrue(isClosed.get());
    }

    public void testConnectSucceeds() throws Exception {
        SocketChannel rawChannel = mock(SocketChannel.class);
        when(rawChannel.finishConnect()).thenReturn(true);
        NioSocketChannel socketChannel = new DoNotCloseChannel(rawChannel, selector);
        socketChannel.setContexts(mock(ReadContext.class), mock(WriteContext.class));
        selector.scheduleForRegistration(socketChannel);

        PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
        socketChannel.addConnectListener(connectFuture);
        connectFuture.get(100, TimeUnit.SECONDS);

        assertTrue(socketChannel.isConnectComplete());
        assertTrue(socketChannel.isOpen());
        assertFalse(closedRawChannel.get());
    }

    public void testConnectFails() throws Exception {
        SocketChannel rawChannel = mock(SocketChannel.class);
        when(rawChannel.finishConnect()).thenThrow(new ConnectException());
        NioSocketChannel socketChannel = new DoNotCloseChannel(rawChannel, selector);
        socketChannel.setContexts(mock(ReadContext.class), mock(WriteContext.class));
        selector.scheduleForRegistration(socketChannel);

        PlainActionFuture<Void> connectFuture = PlainActionFuture.newFuture();
        socketChannel.addConnectListener(connectFuture);
        ExecutionException e = expectThrows(ExecutionException.class, () -> connectFuture.get(100, TimeUnit.SECONDS));
        assertTrue(e.getCause() instanceof IOException);

        assertFalse(socketChannel.isConnectComplete());
        // Even if connection fails the channel is 'open' until close() is called
        assertTrue(socketChannel.isOpen());
    }

    private class DoNotCloseChannel extends DoNotRegisterChannel {

        private DoNotCloseChannel(SocketChannel channel, SocketSelector selector) throws IOException {
            super(channel, selector);
        }

        @Override
        void closeRawChannel() throws IOException {
            closedRawChannel.set(true);
        }
    }
}
