/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.gcs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.mocksocket.MockServerSocket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A mock HTTP Proxy server for testing of support of HTTP proxies in various SDKs
 */
class MockHttpProxyServer implements Closeable {

    private static final Logger log = LogManager.getLogger(MockHttpProxyServer.class);

    private final MockServerSocket serverSocket;
    private final Thread serverThread;
    private final CountDownLatch latch;
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    MockHttpProxyServer(SocketRequestHandler handler) throws IOException {
        // Emulate a proxy HTTP server with plain sockets because MockHttpServer doesn't work as a proxy
        serverSocket = new MockServerSocket(0);
        latch = new CountDownLatch(1);
        serverThread = new Thread(() -> {
            latch.countDown();
            while (Thread.currentThread().isInterrupted() == false) {
                Socket socket;
                try {
                    socket = serverSocket.accept();
                } catch (SocketException e) {
                    // Server socket is closed
                    break;
                } catch (IOException e) {
                    log.error("Unable to accept socket request", e);
                    break;
                }
                executorService.submit(() -> {
                    try (
                        socket;
                        var is = new BufferedInputStream(socket.getInputStream());
                        var os = new BufferedOutputStream(socket.getOutputStream())
                    ) {
                        // Don't handle keep-alive connections to keep things simple
                        handler.handle(is, os);
                    } catch (IOException e) {
                        log.error("Unable to handle socket request", e);
                    }
                });
            }
        });
        serverThread.start();
    }

    MockHttpProxyServer await() throws InterruptedException {
        latch.await();
        return this;
    }

    int getPort() {
        return serverSocket.getLocalPort();
    }

    String getHost() {
        return NetworkAddress.format(serverSocket.getInetAddress());
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
        serverThread.interrupt();
        serverSocket.close();
    }

    @FunctionalInterface
    interface SocketRequestHandler {
        void handle(InputStream is, OutputStream os) throws IOException;
    }
}
