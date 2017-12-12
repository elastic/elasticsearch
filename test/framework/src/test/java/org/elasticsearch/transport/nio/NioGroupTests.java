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

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.channel.ChannelFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public void testStartAndClose() throws IOException {
        // start() starts threads. So we are testing that stop() stops the threads. Our thread linger checks
        // will throw an exception is stop fails
        nioGroup.start();
        nioGroup.close();
    }

    @SuppressWarnings("unchecked")
    public void testCannotOperateBeforeStartOrAfterClose() throws IOException {
        IllegalStateException ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.bindServerChannel(mock(InetSocketAddress.class), mock(ChannelFactory.class)));
        assertEquals("NioGroup is not running.", ise.getMessage());
        ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.openChannel(mock(InetSocketAddress.class), mock(ChannelFactory.class)));
        assertEquals("NioGroup is not running.", ise.getMessage());

        nioGroup.start();
        nioGroup.close();

        ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.bindServerChannel(mock(InetSocketAddress.class), mock(ChannelFactory.class)));
        assertEquals("NioGroup is not running.", ise.getMessage());
        ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.openChannel(mock(InetSocketAddress.class), mock(ChannelFactory.class)));
        assertEquals("NioGroup is not running.", ise.getMessage());
    }

    public void testCanCloseBeforeStartAndThenCannotStart() throws IOException {
        nioGroup.close();

        IllegalStateException ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.start());
        assertEquals("NioGroup is closed.", ise.getMessage());
    }

    public void testCannotStartTwiceOrAfterClose() throws IOException {
        nioGroup.start();
        IllegalStateException ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.start());
        assertEquals("NioGroup already started.", ise.getMessage());

        nioGroup.close();

        ise = expectThrows(IllegalStateException.class,
            () -> nioGroup.start());
        assertEquals("NioGroup is closed.", ise.getMessage());
    }

    public void testCanCloseTwice() throws IOException {
        nioGroup.start();
        nioGroup.close();
        nioGroup.close();
    }

    public void testExceptionAtStartIsHandled() throws IOException {
        RuntimeException ex = new RuntimeException();
        AtomicBoolean firstStartAttempt = new AtomicBoolean(true);
        nioGroup = new NioGroup(logger, r -> {
            if (firstStartAttempt.compareAndSet(true, false)) {
                throw ex;
            } else {
                throw new AssertionError("Should not be allowed a second start attempt");
            }
        }, 1, AcceptorEventHandler::new, daemonThreadFactory(Settings.EMPTY, "selector"), 1, SocketEventHandler::new);
        RuntimeException runtimeException = expectThrows(RuntimeException.class, () -> nioGroup.start());
        assertSame(ex, runtimeException);
        IllegalStateException ise = expectThrows(IllegalStateException.class, () -> nioGroup.start());
        assertEquals("NioGroup is closed.", ise.getMessage());
        // start() starts threads. So we are testing that stop() stops the threads. Our thread linger checks
        // will throw an exception is stop fails
    }
}
