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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.ESSelector;
import org.elasticsearch.transport.nio.TcpReadHandler;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public abstract class AbstractNioChannelTestCase extends ESTestCase {

    ChannelFactory channelFactory = new ChannelFactory(Settings.EMPTY, mock(TcpReadHandler.class));
    MockServerSocket mockServerSocket;
    private Thread serverThread;

    @Before
    public void serverSocketSetup() throws IOException {
        mockServerSocket = new MockServerSocket(0);
        serverThread = new Thread(() -> {
            while (!mockServerSocket.isClosed()) {
                try {
                    Socket socket = mockServerSocket.accept();
                    InputStream inputStream = socket.getInputStream();
                    socket.close();
                } catch (IOException e) {
                }
            }
        });
        serverThread.start();
    }

    @After
    public void serverSocketTearDown() throws IOException {
        serverThread.interrupt();
        mockServerSocket.close();
    }

    public abstract NioChannel channelToClose() throws IOException;

    public abstract ESSelector channelSelector() throws IOException;

    public void testClose() throws IOException, TimeoutException, InterruptedException {
        AtomicReference<NioChannel> ref = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        NioChannel socketChannel = channelToClose();
        CloseFuture closeFuture = socketChannel.getCloseFuture();
        closeFuture.setListener((c) -> {ref.set(c); latch.countDown();});

        assertFalse(closeFuture.isClosed());
        assertTrue(socketChannel.getRawChannel().isOpen());

        channelSelector().singleLoop();

        socketChannel.closeAsync();

        Thread thread = new Thread(channelSelector()::singleLoop);
        thread.start();

        closeFuture.awaitClose(100, TimeUnit.SECONDS);

        assertFalse(socketChannel.getRawChannel().isOpen());
        assertTrue(closeFuture.isClosed());
        latch.await();
        assertSame(socketChannel, ref.get());

        thread.join();
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
