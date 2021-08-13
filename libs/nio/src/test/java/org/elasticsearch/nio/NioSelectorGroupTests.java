/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.mockito.Mockito.mock;

public class NioSelectorGroupTests extends ESTestCase {

    private NioSelectorGroup nioGroup;

    @Override
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        super.setUp();
        nioGroup = new NioSelectorGroup(daemonThreadFactory(Settings.EMPTY, "acceptor"), 1,
            daemonThreadFactory(Settings.EMPTY, "selector"), 1, (s) -> new EventHandler(mock(Consumer.class), s));
    }

    @Override
    public void tearDown() throws Exception {
        nioGroup.close();
        super.tearDown();
    }

    public void testStartAndClose() throws IOException {
        // ctor starts threads. So we are testing that close() stops the threads. Our thread linger checks
        // will throw an exception is stop fails
        nioGroup.close();
    }

    @SuppressWarnings("unchecked")
    public void testCannotOperateAfterClose() throws IOException {
        nioGroup.close();

        IllegalStateException ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.bindServerChannel(mock(InetSocketAddress.class), mock(ChannelFactory.class)));
        assertEquals("NioGroup is closed.", ise.getMessage());
        ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.openChannel(mock(InetSocketAddress.class), mock(ChannelFactory.class)));
        assertEquals("NioGroup is closed.", ise.getMessage());
    }

    public void testCanCloseTwice() throws IOException {
        nioGroup.close();
        nioGroup.close();
    }

    @SuppressWarnings("unchecked")
    public void testExceptionAtStartIsHandled() throws IOException {
        RuntimeException ex = new RuntimeException();
        CheckedRunnable<IOException> ctor = () -> new NioSelectorGroup(r -> {throw ex;}, 1,
            daemonThreadFactory(Settings.EMPTY, "selector"),
            1, (s) -> new EventHandler(mock(Consumer.class), s));
        RuntimeException runtimeException = expectThrows(RuntimeException.class, ctor::run);
        assertSame(ex, runtimeException);
        // ctor starts threads. So we are testing that a failure to construct will stop threads. Our thread
        // linger checks will throw an exception is stop fails
    }
}
