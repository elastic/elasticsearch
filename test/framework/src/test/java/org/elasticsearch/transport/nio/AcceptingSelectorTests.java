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

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;
import org.elasticsearch.transport.nio.utils.TestSelectionKey;
import org.junit.Before;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.PrivilegedActionException;
import java.util.Collections;
import java.util.HashSet;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AcceptingSelectorTests extends ESTestCase {

    private AcceptingSelector selector;
    private NioServerSocketChannel serverChannel;
    private AcceptorEventHandler eventHandler;
    private TestSelectionKey selectionKey;
    private Selector rawSelector;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        eventHandler = mock(AcceptorEventHandler.class);
        serverChannel = mock(NioServerSocketChannel.class);

        rawSelector = mock(Selector.class);
        selector = new AcceptingSelector(eventHandler, rawSelector);
        this.selector.setThread();

        selectionKey = new TestSelectionKey(0);
        selectionKey.attach(serverChannel);
        when(serverChannel.getSelectionKey()).thenReturn(selectionKey);
        when(serverChannel.getSelector()).thenReturn(selector);
        when(serverChannel.isOpen()).thenReturn(true);
    }

    public void testRegisteredChannel() throws IOException, PrivilegedActionException {
        selector.scheduleForRegistration(serverChannel);

        selector.preSelect();

        verify(eventHandler).serverChannelRegistered(serverChannel);
    }

    public void testClosedChannelWillNotBeRegistered() throws Exception {
        when(serverChannel.isOpen()).thenReturn(false);
        selector.scheduleForRegistration(serverChannel);

        selector.preSelect();

        verify(eventHandler).registrationException(same(serverChannel), any(ClosedChannelException.class));
    }

    public void testRegisterChannelFailsDueToException() throws Exception {
        selector.scheduleForRegistration(serverChannel);

        ClosedChannelException closedChannelException = new ClosedChannelException();
        doThrow(closedChannelException).when(serverChannel).register();

        selector.preSelect();

        verify(eventHandler).registrationException(serverChannel, closedChannelException);
    }

    public void testAcceptEvent() throws IOException {
        selectionKey.setReadyOps(SelectionKey.OP_ACCEPT);

        selector.processKey(selectionKey);

        verify(eventHandler).acceptChannel(serverChannel);
    }

    public void testAcceptException() throws IOException {
        selectionKey.setReadyOps(SelectionKey.OP_ACCEPT);
        IOException ioException = new IOException();

        doThrow(ioException).when(eventHandler).acceptChannel(serverChannel);

        selector.processKey(selectionKey);

        verify(eventHandler).acceptException(serverChannel, ioException);
    }

    public void testCleanup() throws IOException {
        selector.scheduleForRegistration(serverChannel);

        selector.preSelect();

        TestSelectionKey key = new TestSelectionKey(0);
        key.attach(serverChannel);
        when(rawSelector.keys()).thenReturn(new HashSet<>(Collections.singletonList(key)));

        selector.cleanupAndCloseChannels();

        verify(eventHandler).handleClose(serverChannel);
    }
}
