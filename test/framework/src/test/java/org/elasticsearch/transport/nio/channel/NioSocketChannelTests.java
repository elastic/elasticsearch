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

import org.elasticsearch.transport.nio.ESSelector;
import org.elasticsearch.transport.nio.SocketEventHandler;
import org.elasticsearch.transport.nio.SocketSelector;
import org.junit.Before;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.Selector;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class NioSocketChannelTests extends AbstractNioChannelTestCase {

    private InetAddress loopbackAddress = InetAddress.getLoopbackAddress();
    private SocketSelector selector;

    @Before
    public void setSelector() throws IOException {
        selector = new SocketSelector(new SocketEventHandler(logger, mock(BiConsumer.class)), mock(Selector.class));
    }

    @Override
    public NioChannel channelToClose() throws IOException {
        return channelFactory.openNioChannel(new InetSocketAddress(loopbackAddress, mockServerSocket.getLocalPort()), selector);
    }

    @Override
    public ESSelector channelSelector() throws IOException {
        return selector;
    }

    public void testConnectSucceeds() throws IOException, InterruptedException {
        InetSocketAddress remoteAddress = new InetSocketAddress(loopbackAddress, mockServerSocket.getLocalPort());
        NioSocketChannel socketChannel = channelFactory.openNioChannel(remoteAddress, selector);
        Thread thread = new Thread(wrappedRunnable(() -> ensureConnect(socketChannel)));
        thread.start();
        ConnectFuture connectFuture = socketChannel.getConnectFuture();
        connectFuture.awaitConnectionComplete(100, TimeUnit.SECONDS);

        assertTrue(socketChannel.isConnectComplete());
        assertTrue(socketChannel.isOpen());
        assertFalse(connectFuture.connectFailed());
        assertNull(connectFuture.getException());

        thread.join();
    }

    public void testConnectFails() throws IOException, InterruptedException {
        mockServerSocket.close();
        InetSocketAddress remoteAddress = new InetSocketAddress(loopbackAddress, mockServerSocket.getLocalPort());
        NioSocketChannel socketChannel = channelFactory.openNioChannel(remoteAddress, selector);
        Thread thread = new Thread(wrappedRunnable(() -> ensureConnect(socketChannel)));
        thread.start();
        ConnectFuture connectFuture = socketChannel.getConnectFuture();
        connectFuture.awaitConnectionComplete(100, TimeUnit.SECONDS);

        assertFalse(socketChannel.isConnectComplete());
        // Even if connection fails the channel is 'open' until close() is called
        assertTrue(socketChannel.isOpen());
        assertTrue(connectFuture.connectFailed());
        assertThat(connectFuture.getException(), instanceOf(ConnectException.class));

        thread.join();
    }

    private void ensureConnect(NioSocketChannel nioSocketChannel) throws IOException {
        for (;;) {
            boolean isConnected = nioSocketChannel.finishConnect();
            if (isConnected) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
    }
}
