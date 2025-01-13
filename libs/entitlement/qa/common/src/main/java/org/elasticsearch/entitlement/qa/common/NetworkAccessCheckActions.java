/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.common;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ServerSocket;
import java.net.Socket;

class NetworkAccessCheckActions {

    static void serverSocketAccept() {
        try (ServerSocket socket = new DummyImplementations.DummyServerSocket()) {
            socket.accept();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void serverSocketBind() {
        try (ServerSocket socket = new DummyImplementations.DummyServerSocket()) {
            socket.bind(null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void createSocketWithProxy() {
        try (Socket socket = new Socket(new Proxy(Proxy.Type.HTTP, new InetSocketAddress(0)))) {
            assert socket.isBound() == false;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void socketBind() {
        try (Socket socket = new DummyImplementations.DummySocket()) {
            socket.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void socketConnect() {
        try (Socket socket = new DummyImplementations.DummySocket()) {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
