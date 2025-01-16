/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.common;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutionException;

@SuppressForbidden(reason = "Testing entitlement check on forbidden action")
class NetworkAccessCheckActions {

    static void serverSocketAccept() throws IOException {
        try (ServerSocket socket = new DummyImplementations.DummyBoundServerSocket()) {
            try {
                socket.accept();
            } catch (IOException e) {
                // Our dummy socket cannot accept connections unless we tell the JDK how to create a socket for it.
                // But Socket.setSocketImplFactory(); is one of the methods we always forbid, so we cannot use it.
                // Still, we can check accept is called (allowed/denied), we don't care if it fails later for this
                // known reason.
                assert e.getMessage().contains("client socket implementation factory not set");
            }
        }
    }

    static void serverSocketBind() throws IOException {
        try (ServerSocket socket = new DummyImplementations.DummyServerSocket()) {
            socket.bind(null);
        }
    }

    static void createSocketWithProxy() throws IOException {
        try (Socket socket = new Socket(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(0)))) {
            assert socket.isBound() == false;
        }
    }

    static void socketBind() throws IOException {
        try (Socket socket = new DummyImplementations.DummySocket()) {
            socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    static void socketConnect() throws IOException {
        try (Socket socket = new DummyImplementations.DummySocket()) {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    static void serverSocketChannelBind() throws IOException {
        try (var serverSocketChannel = ServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    static void serverSocketChannelBindWithBacklog() throws IOException {
        try (var serverSocketChannel = ServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 50);
        }
    }

    static void serverSocketChannelAccept() throws IOException {
        try (var serverSocketChannel = ServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.accept();
        }
    }

    static void asynchronousServerSocketChannelBind() throws IOException {
        try (var serverSocketChannel = AsynchronousServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    static void asynchronousServerSocketChannelBindWithBacklog() throws IOException {
        try (var serverSocketChannel = AsynchronousServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 50);
        }
    }

    static void asynchronousServerSocketChannelAccept() throws IOException {
        try (var serverSocketChannel = AsynchronousServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            var future = serverSocketChannel.accept();
            future.cancel(true);
        }
    }

    static void asynchronousServerSocketChannelAcceptWithHandler() throws IOException {
        try (var serverSocketChannel = AsynchronousServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            serverSocketChannel.accept(null, new CompletionHandler<>() {
                @Override
                public void completed(AsynchronousSocketChannel result, Object attachment) {}

                @Override
                public void failed(Throwable exc, Object attachment) {
                    assert exc.getClass().getSimpleName().equals("NotEntitledException") == false;
                }
            });
        }
    }

    static void socketChannelBind() throws IOException {
        try (var socketChannel = SocketChannel.open()) {
            socketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    static void socketChannelConnect() throws IOException {
        try (var socketChannel = SocketChannel.open()) {
            try {
                socketChannel.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            } catch (BindException e) {
                // We expect to fail, not a valid address to connect to
            }
        }
    }

    static void asynchronousSocketChannelBind() throws IOException {
        try (var socketChannel = AsynchronousSocketChannel.open()) {
            socketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    static void asynchronousSocketChannelConnect() throws IOException, InterruptedException {
        try (var socketChannel = AsynchronousSocketChannel.open()) {
            var future = socketChannel.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            try {
                future.get();
            } catch (ExecutionException e) {
                assert e.getCause().getClass().getSimpleName().equals("NotEntitledException") == false;
            } finally {
                future.cancel(true);
            }
        }
    }

    static void asynchronousSocketChannelConnectWithCompletion() throws IOException {
        try (var socketChannel = AsynchronousSocketChannel.open()) {
            socketChannel.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), null, new CompletionHandler<>() {
                @Override
                public void completed(Void result, Object attachment) {}

                @Override
                public void failed(Throwable exc, Object attachment) {
                    assert exc.getClass().getSimpleName().equals("NotEntitledException") == false;
                }
            });
        }
    }

    static void datagramChannelBind() throws IOException {
        try (var channel = DatagramChannel.open()) {
            channel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    static void datagramChannelConnect() throws IOException {
        try (var channel = DatagramChannel.open()) {
            channel.configureBlocking(false);
            try {
                channel.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            } catch (SocketException e) {
                // We expect to fail, not a valid address to connect to
            }
        }
    }

    static void datagramChannelSend() throws IOException {
        try (var channel = DatagramChannel.open()) {
            channel.configureBlocking(false);
            channel.send(ByteBuffer.wrap(new byte[] {0}), new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234));
        }
    }

    static void datagramChannelReceive() throws IOException {
        try (var channel = DatagramChannel.open()) {
            channel.configureBlocking(false);
            var buffer = new byte[1];
            channel.receive(ByteBuffer.wrap(buffer));
        }
    }
}
