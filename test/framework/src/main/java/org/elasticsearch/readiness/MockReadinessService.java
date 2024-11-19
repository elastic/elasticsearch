/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.Set;

public class MockReadinessService extends ReadinessService {
    /**
     * Marker plugin used by {@link MockNode} to enable {@link MockReadinessService}.
     */
    public static class TestPlugin extends Plugin {}

    private static final int RETRIES = 30;

    private static final int RETRY_DELAY_IN_MILLIS = 100;

    private static final String METHOD_NOT_MOCKED = "This method has not been mocked";

    private static class MockServerSocketChannel extends ServerSocketChannel {

        static ServerSocketChannel openMock() {
            return new MockServerSocketChannel();
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
            throw new UnsupportedOperationException(METHOD_NOT_MOCKED);
        }

        @Override
        public <T> T getOption(SocketOption<T> name) {
            throw new UnsupportedOperationException(METHOD_NOT_MOCKED);
        }

        @Override
        public Set<SocketOption<?>> supportedOptions() {
            throw new UnsupportedOperationException(METHOD_NOT_MOCKED);
        }

        @Override
        public ServerSocket socket() {
            throw new UnsupportedOperationException(METHOD_NOT_MOCKED);
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
            throw new UnsupportedOperationException(METHOD_NOT_MOCKED);
        }
    }

    public MockReadinessService(ClusterService clusterService, Environment environment) {
        super(clusterService, environment, MockServerSocketChannel::openMock);
    }

    private static boolean socketIsOpen(ReadinessService readinessService) {
        ServerSocketChannel mockedSocket = readinessService.serverChannel();
        return mockedSocket != null && mockedSocket.isOpen();
    }

    public static void tcpReadinessProbeTrue(ReadinessService readinessService) throws InterruptedException {
        for (int i = 1; i <= RETRIES; ++i) {
            if (socketIsOpen(readinessService)) {
                return;
            }
            Thread.sleep(RETRY_DELAY_IN_MILLIS * i);
        }

        throw new AssertionError("Readiness socket should be open");
    }

    public static void tcpReadinessProbeFalse(ReadinessService readinessService) throws InterruptedException {
        for (int i = 0; i < RETRIES; ++i) {
            if (socketIsOpen(readinessService) == false) {
                return;
            }
            Thread.sleep(RETRY_DELAY_IN_MILLIS * i);
        }

        throw new AssertionError("Readiness socket should be closed");
    }
}
