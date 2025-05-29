/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.entitlement.qa.entitled.EntitledActions;

import java.io.FileDescriptor;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.spi.SelectorProvider;
import java.nio.file.StandardOpenOption;
import java.util.Set;

import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.ALWAYS_DENIED;
import static org.elasticsearch.entitlement.qa.test.EntitlementTest.ExpectedAccess.PLUGINS;

@SuppressWarnings({ "unused" /* called via reflection */ })
class NioChannelsActions {

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createFileChannel() throws IOException {
        new DummyImplementations.DummyFileChannel().close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileChannelOpenForWrite() throws IOException {
        FileChannel.open(FileCheckActions.readWriteFile(), StandardOpenOption.WRITE).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileChannelOpenForRead() throws IOException {
        FileChannel.open(FileCheckActions.readFile()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileChannelOpenForWriteWithOptions() throws IOException {
        FileChannel.open(FileCheckActions.readWriteFile(), Set.of(StandardOpenOption.WRITE)).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void fileChannelOpenForReadWithOptions() throws IOException {
        FileChannel.open(FileCheckActions.readFile(), Set.of(StandardOpenOption.READ)).close();
    }

    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void createAsynchronousFileChannel() {
        new DummyImplementations.DummyAsynchronousFileChannel().close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void asynchronousFileChannelOpenForWrite() throws IOException {
        var file = EntitledActions.createTempFileForWrite();
        AsynchronousFileChannel.open(file, StandardOpenOption.WRITE).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void asynchronousFileChannelOpenForRead() throws IOException {
        var file = EntitledActions.createTempFileForRead();
        AsynchronousFileChannel.open(file).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void asynchronousFileChannelOpenForWriteWithOptions() throws IOException {
        var file = EntitledActions.createTempFileForWrite();
        AsynchronousFileChannel.open(file, Set.of(StandardOpenOption.WRITE), EsExecutors.DIRECT_EXECUTOR_SERVICE).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void asynchronousFileChannelOpenForReadWithOptions() throws IOException {
        var file = EntitledActions.createTempFileForRead();
        AsynchronousFileChannel.open(file, Set.of(StandardOpenOption.READ), EsExecutors.DIRECT_EXECUTOR_SERVICE).close();
    }

    @SuppressForbidden(reason = "specifically testing jdk.nio.Channels")
    @EntitlementTest(expectedAccess = ALWAYS_DENIED)
    static void channelsReadWriteSelectableChannel() throws IOException {
        jdk.nio.Channels.readWriteSelectableChannel(new FileDescriptor(), new DummyImplementations.DummySelectableChannelCloser()).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void selectableChannelRegisterConnect() throws IOException {
        try (var selectableChannel = new DummyImplementations.DummySelectableChannel(new DummyImplementations.DummySelectorProvider())) {
            selectableChannel.register(new DummyImplementations.DummySelector(SelectorProvider.provider()), SelectionKey.OP_CONNECT);
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void selectableChannelRegisterAccept() throws IOException {
        try (var selectableChannel = new DummyImplementations.DummySelectableChannel(new DummyImplementations.DummySelectorProvider())) {
            selectableChannel.register(new DummyImplementations.DummySelector(SelectorProvider.provider()), SelectionKey.OP_ACCEPT);
        }
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void selectoProviderOpenSocketChannel() throws IOException {
        SelectorProvider.provider().openSocketChannel().close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void selectoProviderOpenDatagramChannel() throws IOException {
        SelectorProvider.provider().openDatagramChannel().close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void selectoProviderOpenServerSocketChannel() throws IOException {
        SelectorProvider.provider().openServerSocketChannel().close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void selectoProviderOpenSocketChannelWithProtocol() throws IOException {
        SelectorProvider.provider().openSocketChannel(StandardProtocolFamily.INET).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void selectoProviderOpenDatagramChannelWithProtocol() throws IOException {
        SelectorProvider.provider().openDatagramChannel(StandardProtocolFamily.INET).close();
    }

    @EntitlementTest(expectedAccess = PLUGINS)
    static void selectoProviderOpenServerSocketChannelWithProtocol() throws IOException {
        SelectorProvider.provider().openServerSocketChannel(StandardProtocolFamily.INET).close();
    }
}
