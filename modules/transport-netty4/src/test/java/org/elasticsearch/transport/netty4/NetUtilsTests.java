/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;

import static org.hamcrest.Matchers.hasItem;

public class NetUtilsTests extends ESTestCase {

    public void testExtendedSocketOptions() throws IOException {
        assertTrue(
            "jdk.net module not resolved",
            ModuleLayer.boot().modules().stream().map(Module::getName).anyMatch(nm -> nm.equals("jdk.net"))
        );

        assumeTrue("Platform possibly not supported", IOUtils.LINUX || IOUtils.MAC_OS_X);
        try (var channel = networkChannel()) {
            var options = channel.supportedOptions();
            assertThat(options, hasItem(NetUtils.getTcpKeepIdleSocketOption()));
            assertThat(options, hasItem(NetUtils.getTcpKeepIntervalSocketOption()));
            assertThat(options, hasItem(NetUtils.getTcpKeepCountSocketOption()));
        }
    }

    private static NetworkChannel networkChannel() {
        try {
            return SocketChannel.open();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
