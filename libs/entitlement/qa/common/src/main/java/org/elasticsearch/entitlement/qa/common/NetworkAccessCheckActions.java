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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

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

    @SuppressForbidden(reason = "Testing entitlement check on forbidden action")
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

    @SuppressForbidden(reason = "Testing entitlement check on forbidden action")
    static void socketConnect() throws IOException {
        try (Socket socket = new DummyImplementations.DummySocket()) {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        }
    }

    static void urlOpenConnectionWithProxy() throws URISyntaxException, IOException {
        var url = new URI("http://localhost").toURL();
        var urlConnection = url.openConnection(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(0)));
        assert urlConnection != null;
    }

    static void httpClientBuilderBuild() {
        try (HttpClient httpClient = HttpClient.newBuilder().build()) {
            assert httpClient != null;
        }
    }

    static void httpClientSend() throws InterruptedException {
        try (HttpClient httpClient = HttpClient.newBuilder().build()) {
            // Shutdown the client, so the send action will shortcut before actually executing any network operation
            // (but after it run our check in the prologue)
            httpClient.shutdown();
            try {
                httpClient.send(HttpRequest.newBuilder(URI.create("http://localhost")).build(), HttpResponse.BodyHandlers.discarding());
            } catch (IOException e) {
                // Expected, since we shut down the client
            }
        }
    }

    static void httpClientSendAsync() {
        try (HttpClient httpClient = HttpClient.newBuilder().build()) {
            // Shutdown the client, so the send action will return before actually executing any network operation
            // (but after it run our check in the prologue)
            httpClient.shutdown();
            var future = httpClient.sendAsync(
                HttpRequest.newBuilder(URI.create("http://localhost")).build(),
                HttpResponse.BodyHandlers.discarding()
            );
            assert future.isCompletedExceptionally();
            future.exceptionally(ex -> {
                assert ex instanceof IOException;
                return null;
            });
        }
    }
}
