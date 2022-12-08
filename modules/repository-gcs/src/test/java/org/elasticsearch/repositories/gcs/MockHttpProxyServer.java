package org.elasticsearch.repositories.gcs;

import org.elasticsearch.mocksocket.MockServerSocket;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

/**
 * A Mock single-threaded HTTP Proxy server for testing of the support of HTTP proxies in various SDKs
 */
public class MockHttpProxyServer implements Closeable {

    private final MockServerSocket serverSocket;
    private final Thread serverThread;
    private final CountDownLatch latch;

    public MockHttpProxyServer(SocketRequestHandler handler) throws IOException {
        // Emulate a proxy HTTP server with plain sockets because MockHttpServer doesn't work as a proxy
        serverSocket = new MockServerSocket(0);
        latch = new CountDownLatch(1);
        serverThread = new Thread(() -> {
            latch.countDown();
            while (Thread.currentThread().isInterrupted() == false) {
                try (
                    var socket = serverSocket.accept();
                    var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    var writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8)
                ) {
                    handler.handle(reader, writer);
                } catch (IOException ignored) {}
            }
        });
        serverThread.start();
    }

    public MockHttpProxyServer await() throws InterruptedException {
        latch.await();
        return this;
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    @Override
    public void close() throws IOException {
        serverThread.interrupt();
        serverSocket.close();
    }

    @FunctionalInterface
    interface SocketRequestHandler {
        void handle(BufferedReader reader, Writer writer) throws IOException;
    }
}
