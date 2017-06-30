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
import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.DoNotRegisterServerChannel;
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.ReadContext;
import org.elasticsearch.transport.nio.channel.WriteContext;
import org.junit.Before;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AcceptorEventHandlerTests extends ESTestCase {

    private AcceptorEventHandler handler;
    private SocketSelector socketSelector;
    private ChannelFactory channelFactory;
    private OpenChannels openChannels;
    private NioServerSocketChannel channel;

    @Before
    public void setUpHandler() throws IOException {
        channelFactory = mock(ChannelFactory.class);
        socketSelector = mock(SocketSelector.class);
        openChannels = new OpenChannels(logger);
        ArrayList<SocketSelector> selectors = new ArrayList<>();
        selectors.add(socketSelector);
        handler = new AcceptorEventHandler(logger, openChannels, new RoundRobinSelectorSupplier(selectors));

        AcceptingSelector selector = mock(AcceptingSelector.class);
        channel = new DoNotRegisterServerChannel("", mock(ServerSocketChannel.class), channelFactory, selector);
        channel.register();
    }

    public void testHandleRegisterAdjustsOpenChannels() {
        assertEquals(0, openChannels.serverChannelsCount());

        handler.serverChannelRegistered(channel);

        assertEquals(1, openChannels.serverChannelsCount());
    }

    public void testHandleRegisterSetsOP_ACCEPTInterest() {
        assertEquals(0, channel.getSelectionKey().interestOps());

        handler.serverChannelRegistered(channel);

        assertEquals(SelectionKey.OP_ACCEPT, channel.getSelectionKey().interestOps());
    }

    public void testHandleAcceptRegistersWithSelector() throws IOException {
        NioSocketChannel childChannel = new NioSocketChannel("", mock(SocketChannel.class), socketSelector);
        when(channelFactory.acceptNioChannel(channel, socketSelector)).thenReturn(childChannel);

        handler.acceptChannel(channel);

        verify(socketSelector).scheduleForRegistration(childChannel);
    }

    public void testHandleAcceptAddsToOpenChannelsAndAddsCloseListenerToRemove() throws IOException {
        NioSocketChannel childChannel = new NioSocketChannel("", SocketChannel.open(), socketSelector);
        childChannel.setContexts(mock(ReadContext.class), mock(WriteContext.class));
        when(channelFactory.acceptNioChannel(channel, socketSelector)).thenReturn(childChannel);

        handler.acceptChannel(channel);

        assertEquals(new HashSet<>(Collections.singletonList(childChannel)), openChannels.getAcceptedChannels());

        when(socketSelector.isOnCurrentThread()).thenReturn(true);
        childChannel.closeFromSelector();

        assertEquals(new HashSet<>(), openChannels.getAcceptedChannels());
    }
}
