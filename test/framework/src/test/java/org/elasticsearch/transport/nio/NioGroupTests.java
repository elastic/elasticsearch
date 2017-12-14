/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.channel.ChannelFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.mockito.Mockito.mock;

public class NioGroupTests extends ESTestCase {

    private NioGroup nioGroup;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        nioGroup = new NioGroup(logger, daemonThreadFactory(Settings.EMPTY, "acceptor"), 1, AcceptorEventHandler::new,
            daemonThreadFactory(Settings.EMPTY, "selector"), 1, SocketEventHandler::new);
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

    public void testExceptionAtStartIsHandled() throws IOException {
        RuntimeException ex = new RuntimeException();
        CheckedRunnable<IOException> ctor = () -> new NioGroup(logger, r -> {throw ex;}, 1,
            AcceptorEventHandler::new, daemonThreadFactory(Settings.EMPTY, "selector"), 1, SocketEventHandler::new);
        RuntimeException runtimeException = expectThrows(RuntimeException.class, ctor::run);
        assertSame(ex, runtimeException);
        // ctor starts threads. So we are testing that a failure to construct will stop threads. Our thread
        // linger checks will throw an exception is stop fails
    }
}
