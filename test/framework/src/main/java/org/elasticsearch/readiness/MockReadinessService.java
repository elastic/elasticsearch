/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.readiness;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.plugins.Plugin;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketOption;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MockReadinessService extends ReadinessService {
    /**
     * Marker plugin used by {@link MockNode} to enable {@link MockReadinessService}.
     */
    public static class TestPlugin extends Plugin {}

    private static class MockServerSocketChannel extends ServerSocketChannel {
        private static final Map<String, MockServerSocketChannel> mockedSockets = new HashMap<>();

        static ServerSocketChannel open(String nodeName) {
            var mockSocket = new MockServerSocketChannel();
            mockedSockets.put(nodeName, mockSocket);
            return mockSocket;
        }

        private MockServerSocketChannel() {
            super(SelectorProvider.provider());
        }

        @Override
        public ServerSocketChannel bind(SocketAddress local, int backlog) {
            assert isOpen();
            return this;
        }

        @Override
        public <T> ServerSocketChannel setOption(SocketOption<T> name, T value) {
            throw new UnsupportedOperationException("Calling not mocked method");
        }

        @Override
        public <T> T getOption(SocketOption<T> name) {
            throw new UnsupportedOperationException("Calling not mocked method");
        }

        @Override
        public Set<SocketOption<?>> supportedOptions() {
            throw new UnsupportedOperationException("Calling not mocked method");
        }

        @Override
        public ServerSocket socket() {
            throw new UnsupportedOperationException("Calling not mocked method");
        }

        @Override
        public SocketChannel accept() {
            return null;
        }

        @Override
        public SocketAddress getLocalAddress() {
            return new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        }

        @Override
        protected void implCloseSelectableChannel() {}

        @Override
        protected void implConfigureBlocking(boolean block) {
            throw new UnsupportedOperationException("Calling not mocked method");
        }
    }

    public MockReadinessService(ClusterService clusterService, Environment environment) {
        super(clusterService, environment, MockServerSocketChannel::open);
    }

    static void tcpReadinessProbeTrue(String nodeName) {
        MockServerSocketChannel mockedSocket = MockServerSocketChannel.mockedSockets.get(nodeName);
        if (mockedSocket == null) {
            throw new AssertionError("Mocked socket not created for this node");
        }
        if (mockedSocket.isOpen() == false) {
            throw new AssertionError("Readiness socket should be open");
        }
    }

    static void tcpReadinessProbeFalse(String nodeName) {
        MockServerSocketChannel mockedSocket = MockServerSocketChannel.mockedSockets.get(nodeName);
        if (mockedSocket == null) {
            throw new AssertionError("Mocked socket not created for this node");
        }
        if (mockedSocket.isOpen()) {
            throw new AssertionError("Readiness socket should be closed");
        }
    }
}
