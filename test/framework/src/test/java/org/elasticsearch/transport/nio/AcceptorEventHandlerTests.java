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
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AcceptorEventHandlerTests extends ESTestCase {

    private AcceptorEventHandler handler;
    private SocketSelector socketSelector;
    private ChannelFactory channelFactory;
    private NioServerSocketChannel channel;
    private Consumer acceptedChannelCallback;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpHandler() throws IOException {
        channelFactory = mock(ChannelFactory.class);
        socketSelector = mock(SocketSelector.class);
        acceptedChannelCallback = mock(Consumer.class);
        ArrayList<SocketSelector> selectors = new ArrayList<>();
        selectors.add(socketSelector);
        handler = new AcceptorEventHandler(logger, new RoundRobinSupplier<>(selectors.toArray(new SocketSelector[selectors.size()])));

        AcceptingSelector selector = mock(AcceptingSelector.class);
        channel = new DoNotRegisterServerChannel(mock(ServerSocketChannel.class), channelFactory, selector);
        channel.setAcceptContext(acceptedChannelCallback);
        channel.register();
    }

    public void testHandleRegisterSetsOP_ACCEPTInterest() {
        assertEquals(0, channel.getSelectionKey().interestOps());

        handler.serverChannelRegistered(channel);

        assertEquals(SelectionKey.OP_ACCEPT, channel.getSelectionKey().interestOps());
    }

    public void testHandleAcceptCallsChannelFactory() throws IOException {
        NioSocketChannel childChannel = new NioSocketChannel(mock(SocketChannel.class), socketSelector);
        when(channelFactory.acceptNioChannel(same(channel), same(socketSelector))).thenReturn(childChannel);

        handler.acceptChannel(channel);

        verify(channelFactory).acceptNioChannel(same(channel), same(socketSelector));

    }

    @SuppressWarnings("unchecked")
    public void testHandleAcceptCallsServerAcceptCallback() throws IOException {
        NioSocketChannel childChannel = new NioSocketChannel(mock(SocketChannel.class), socketSelector);
        childChannel.setContexts(mock(ReadContext.class), mock(WriteContext.class), mock(BiConsumer.class));
        when(channelFactory.acceptNioChannel(same(channel), same(socketSelector))).thenReturn(childChannel);

        handler.acceptChannel(channel);

        verify(acceptedChannelCallback).accept(childChannel);
    }
}
