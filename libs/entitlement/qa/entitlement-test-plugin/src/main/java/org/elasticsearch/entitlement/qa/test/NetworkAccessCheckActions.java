/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.ResponseCache;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.StandardProtocolFamily;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.spi.URLStreamHandlerProvider;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.DatagramChannel;
import java.nio.channels.NotYetBoundException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.security.InvalidAlgorithmParameterException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertStore;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressForbidden(reason = "Testing entitlement check on forbidden action")
@SuppressWarnings({ "unused" /* called via reflection */, "deprecation" })
class NetworkAccessCheckActions {

    @EntitlementTest(expectedAccess = PLUGINS)
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

    @EntitlementTest(expectedAccess = PLUGINS)
    static void serverSocketBind() throws IOException {
        try (ServerSocket socket = new DummyImplementations.DummyServerSocket()) {
            socket.bind(null);
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createSocketWithProxy() throws IOException {
        try (Socket socket = new Socket(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(0)))) {
            assert socket.isBound() == false;
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void socketBind() throws IOException {
        try (Socket socket = new DummyImplementations.DummySocket()) {
            socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void socketConnect() throws IOException {
        try (Socket socket = new DummyImplementations.DummySocket()) {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void createLDAPCertStore() {
        try {
            // We pass down null params to provoke a InvalidAlgorithmParameterException
            CertStore.getInstance("LDAP", null);
        } catch (InvalidAlgorithmParameterException ex) {
            // Assert we actually hit the class we care about, LDAPCertStore (or its impl)
            assert Arrays.stream(ex.getStackTrace()).anyMatch(e -> e.getClassName().endsWith("LDAPCertStore"));
        } catch (NoSuchAlgorithmException e) {
            // In some environments (e.g. with FIPS enabled) the LDAPCertStore is not present, so this will fail.
            // This is OK, as this means the class we care about (LDAPCertStore) is not even present
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void serverSocketChannelBind() throws IOException {
        try (var serverSocketChannel = ServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void serverSocketChannelBindWithBacklog() throws IOException {
        try (var serverSocketChannel = ServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 50);
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void serverSocketChannelAccept() throws IOException {
        try (var serverSocketChannel = ServerSocketChannel.open()) {
            serverSocketChannel.configureBlocking(false);
            try {
                serverSocketChannel.accept();
            } catch (NotYetBoundException e) {
                // It's OK, we did not call bind on the socket on purpose so we can just test "accept"
                // "accept" will be called and exercise the Entitlement check, we don't care if it fails afterward for this known reason.
            }
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void asynchronousServerSocketChannelBind() throws IOException {
        try (var serverSocketChannel = AsynchronousServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void asynchronousServerSocketChannelBindWithBacklog() throws IOException {
        try (var serverSocketChannel = AsynchronousServerSocketChannel.open()) {
            serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 50);
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void asynchronousServerSocketChannelAccept() throws IOException {
        try (var serverSocketChannel = AsynchronousServerSocketChannel.open()) {
            try {
                var future = serverSocketChannel.accept();
                future.cancel(true);
            } catch (NotYetBoundException e) {
                // It's OK, we did not call bind on the socket on purpose so we can just test "accept"
                // "accept" will be called and exercise the Entitlement check, we don't care if it fails afterward for this known reason.
            }
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void asynchronousServerSocketChannelAcceptWithHandler() throws IOException {
        try (var serverSocketChannel = AsynchronousServerSocketChannel.open()) {
            try {
                serverSocketChannel.accept(null, new CompletionHandler<>() {
                    @Override
                    public void completed(AsynchronousSocketChannel result, Object attachment) {}

                    @Override
                    public void failed(Throwable exc, Object attachment) {
                        assert exc.getClass().getSimpleName().equals("NotEntitledException") == false;
                    }
                });
            } catch (NotYetBoundException e) {
                // It's OK, we did not call bind on the socket on purpose so we can just test "accept"
                // "accept" will be called and exercise the Entitlement check, we don't care if it fails afterward for this known reason.
            }
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void socketChannelBind() throws IOException {
        try (var socketChannel = SocketChannel.open()) {
            socketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void socketChannelConnect() throws IOException {
        try (var socketChannel = SocketChannel.open()) {
            try {
                socketChannel.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            } catch (SocketException e) {
                // We expect to fail, not a valid address to connect to.
                // "connect" will be called and exercise the Entitlement check, we don't care if it fails afterward for this known reason.
            }
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void socketChannelOpenProtocol() throws IOException {
        SocketChannel.open(StandardProtocolFamily.INET).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void socketChannelOpenAddress() throws IOException {
        SocketChannel.open(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0)).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void asynchronousSocketChannelBind() throws IOException {
        try (var socketChannel = AsynchronousSocketChannel.open()) {
            socketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
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

    @EntitlementTest(expectedAccess = PLUGINS)
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

    @EntitlementTest(expectedAccess = PLUGINS)
    static void datagramChannelBind() throws IOException {
        try (var channel = DatagramChannel.open()) {
            channel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void datagramChannelConnect() throws IOException {
        try (var channel = DatagramChannel.open()) {
            channel.configureBlocking(false);
            try {
                channel.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            } catch (SocketException e) {
                // We expect to fail, not a valid address to connect to.
                // "connect" will be called and exercise the Entitlement check, we don't care if it fails afterward for this known reason.
            }
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void datagramChannelSend() throws IOException {
        try (var channel = DatagramChannel.open()) {
            channel.configureBlocking(false);
            channel.send(ByteBuffer.wrap(new byte[] { 0 }), new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234));
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void datagramChannelReceive() throws IOException {
        try (var channel = DatagramChannel.open()) {
            channel.configureBlocking(false);
            var buffer = new byte[1];
            channel.receive(ByteBuffer.wrap(buffer));
        }
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createURLStreamHandlerProvider() {
        var x = new URLStreamHandlerProvider() {
            @Override
            public URLStreamHandler createURLStreamHandler(String protocol) {
                return null;
            }
        };
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createURLWithURLStreamHandler() throws MalformedURLException {
        var x = new URL("http", "host", 1234, "file", new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) {
                return null;
            }
        });
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createURLWithURLStreamHandler2() throws MalformedURLException {
        var x = new URL(null, "spec", new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL u) {
                return null;
            }
        });
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void setDefaultResponseCache() {
        ResponseCache.setDefault(null);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void setDefaultProxySelector() {
        ProxySelector.setDefault(null);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void setDefaultSSLContext() throws NoSuchAlgorithmException {
        SSLContext.setDefault(SSLContext.getDefault());
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void setDefaultHostnameVerifier() {
        HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> false);
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void setDefaultSSLSocketFactory() {
        HttpsURLConnection.setDefaultSSLSocketFactory(new DummyImplementations.DummySSLSocketFactory());
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void setHttpsConnectionProperties() {
        new DummyImplementations.DummyHttpsURLConnection().setSSLSocketFactory(new DummyImplementations.DummySSLSocketFactory());
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void datagramSocket$$setDatagramSocketImplFactory() throws IOException {
        DatagramSocket.setDatagramSocketImplFactory(() -> { throw new IllegalStateException(); });
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void httpURLConnection$$setFollowRedirects() {
        HttpURLConnection.setFollowRedirects(HttpURLConnection.getFollowRedirects());
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void serverSocket$$setSocketFactory() throws IOException {
        ServerSocket.setSocketFactory(() -> { throw new IllegalStateException(); });
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void socket$$setSocketImplFactory() throws IOException {
        Socket.setSocketImplFactory(() -> { throw new IllegalStateException(); });
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void url$$setURLStreamHandlerFactory() {
        URL.setURLStreamHandlerFactory(__ -> { throw new IllegalStateException(); });
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void urlConnection$$setFileNameMap() {
        URLConnection.setFileNameMap(__ -> { throw new IllegalStateException(); });
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void urlConnection$$setContentHandlerFactory() {
        URLConnection.setContentHandlerFactory(__ -> { throw new IllegalStateException(); });
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void bindDatagramSocket() throws SocketException {
        try (var socket = new DatagramSocket(null)) {
            socket.bind(null);
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void connectDatagramSocket() throws SocketException {
        try (var socket = new DummyImplementations.DummyDatagramSocket()) {
            socket.connect(new InetSocketAddress(1234));
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void joinGroupDatagramSocket() throws IOException {
        try (var socket = new DummyImplementations.DummyDatagramSocket()) {
            socket.joinGroup(
                new InetSocketAddress(InetAddress.getByAddress(new byte[] { (byte) 230, 0, 0, 1 }), 1234),
                NetworkInterface.getByIndex(0)
            );
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void leaveGroupDatagramSocket() throws IOException {
        try (var socket = new DummyImplementations.DummyDatagramSocket()) {
            socket.leaveGroup(
                new InetSocketAddress(InetAddress.getByAddress(new byte[] { (byte) 230, 0, 0, 1 }), 1234),
                NetworkInterface.getByIndex(0)
            );
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void sendDatagramSocket() throws IOException {
        try (var socket = new DummyImplementations.DummyDatagramSocket()) {
            socket.send(new DatagramPacket(new byte[] { 0 }, 1, InetAddress.getLocalHost(), 1234));
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void receiveDatagramSocket() throws IOException {
        try (var socket = new DummyImplementations.DummyDatagramSocket()) {
            socket.receive(new DatagramPacket(new byte[1], 1, InetAddress.getLocalHost(), 1234));
        }
    }

    private NetworkAccessCheckActions() {}
}
