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
}
