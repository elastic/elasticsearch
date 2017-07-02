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

import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.TcpReadHandler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;

public abstract class AbstractNioChannelTestCase extends ESTestCase {

    ChannelFactory channelFactory = new ChannelFactory(Settings.EMPTY, mock(TcpReadHandler.class));
    MockServerSocket mockServerSocket;

    private Thread serverThread;

    @Before
    @SuppressForbidden(reason = "Allow getLocalHost")
    public void serverSocketSetup() throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        mockServerSocket = new MockServerSocket();
        mockServerSocket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 1);
        mockServerSocket.setReuseAddress(true);
        serverThread = new Thread(() -> {
            latch.countDown();
            try (Socket socket = mockServerSocket.accept()) {
                int inputStream = socket.getInputStream().read();
            } catch (IOException e) {
            }
        });
        serverThread.start();
        latch.await();
    }

    @After
    public void serverSocketTearDown() throws IOException, InterruptedException {
        mockServerSocket.close();
        serverThread.join();
    }

    public abstract NioChannel channelToClose(Consumer<NioChannel> closeListener) throws IOException;

    public void testClose() throws IOException, TimeoutException, InterruptedException {
        AtomicReference<NioChannel> ref = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        NioChannel socketChannel = channelToClose((c) -> {
            ref.set(c);
            latch.countDown();
        });
        CloseFuture closeFuture = socketChannel.getCloseFuture();

        assertFalse(closeFuture.isClosed());
        assertTrue(socketChannel.getRawChannel().isOpen());

        socketChannel.closeAsync();

        closeFuture.awaitClose(100, TimeUnit.SECONDS);

        assertFalse(socketChannel.getRawChannel().isOpen());
        assertTrue(closeFuture.isClosed());
        latch.await();
        assertSame(socketChannel, ref.get());
    }

    protected Runnable wrappedRunnable(CheckedRunnable<Exception> runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (Exception e) {
            }
        };
    }
}
