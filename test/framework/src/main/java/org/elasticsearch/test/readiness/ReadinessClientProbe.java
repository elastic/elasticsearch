/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.readiness;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.readiness.ReadinessService;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.SocketChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

import static org.apache.lucene.tests.util.LuceneTestCase.expectThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

/**
 * Helper test interface that provides basic socket connect functionality to
 * the readiness service for testing purposes
 */
public interface ReadinessClientProbe {
    default void tcpReadinessProbeTrue(ReadinessService readinessService) throws Exception {
        tcpReadinessProbeTrue(readinessService.boundAddress().publishAddress().getPort());
    }

    // extracted because suppress forbidden checks have issues with lambdas
    @SuppressForbidden(reason = "Intentional socket open")
    default void channelConnect(SocketChannel channel, InetSocketAddress socketAddress) throws IOException {
        channel.connect(socketAddress);
    }

    @SuppressForbidden(reason = "Intentional socket open")
    default void tcpReadinessProbeTrue(Integer port) throws Exception {
        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);

        try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.INET)) {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                try {
                    channelConnect(channel, socketAddress);
                    // if we succeeded to connect the server is ready
                } catch (IOException e) {
                    fail("Shouldn't reach here");
                }
                return null;
            });
        }
    }

    default void tcpReadinessProbeFalse(ReadinessService readinessService) throws Exception {
        tcpReadinessProbeFalse(readinessService.boundAddress().publishAddress().getPort());
    }

    @SuppressForbidden(reason = "Intentional socket open")
    default void tcpReadinessProbeFalse(Integer port) throws Exception {
        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);

        try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.INET)) {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                String message = expectThrows(IOException.class, () -> channelConnect(channel, socketAddress)).getMessage();
                assertThat(message, containsString("Connection refused"));
                return null;
            });
        }
    }
}
