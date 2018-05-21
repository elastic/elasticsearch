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

package org.elasticsearch.nio;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.function.Consumer;

import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AcceptorEventHandlerTests extends ESTestCase {

    private AcceptorEventHandler handler;
    private ChannelFactory<NioServerSocketChannel, NioSocketChannel> channelFactory;
    private NioServerSocketChannel channel;
    private DoNotRegisterContext context;
    private RoundRobinSupplier<SocketSelector> selectorSupplier;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpHandler() throws IOException {
        channelFactory = mock(ChannelFactory.class);
        ArrayList<SocketSelector> selectors = new ArrayList<>();
        selectors.add(mock(SocketSelector.class));
        selectorSupplier = new RoundRobinSupplier<>(selectors.toArray(new SocketSelector[selectors.size()]));
        handler = new AcceptorEventHandler(selectorSupplier, mock(Consumer.class));

        channel = new NioServerSocketChannel(mock(ServerSocketChannel.class));
        context = new DoNotRegisterContext(channel, mock(AcceptingSelector.class), mock(Consumer.class));
        channel.setContext(context);
    }

    public void testHandleRegisterSetsOP_ACCEPTInterest() throws IOException {
        assertNull(context.getSelectionKey());

        handler.handleRegistration(context);

        assertEquals(SelectionKey.OP_ACCEPT, channel.getContext().getSelectionKey().interestOps());
    }

    public void testRegisterAddsAttachment() throws IOException {
        assertNull(context.getSelectionKey());

        handler.handleRegistration(context);

        assertEquals(context, context.getSelectionKey().attachment());
    }

    public void testHandleAcceptCallsChannelFactory() throws IOException {
        NioSocketChannel childChannel = new NioSocketChannel(mock(SocketChannel.class));
        NioSocketChannel nullChannel = null;
        when(channelFactory.acceptNioChannel(same(context), same(selectorSupplier))).thenReturn(childChannel, nullChannel);

        handler.acceptChannel(context);

        verify(channelFactory, times(2)).acceptNioChannel(same(context), same(selectorSupplier));
    }

    @SuppressWarnings("unchecked")
    public void testHandleAcceptCallsServerAcceptCallback() throws IOException {
        NioSocketChannel childChannel = new NioSocketChannel(mock(SocketChannel.class));
        SocketChannelContext childContext = mock(SocketChannelContext.class);
        childChannel.setContext(childContext);
        ServerChannelContext serverChannelContext = mock(ServerChannelContext.class);
        channel = new NioServerSocketChannel(mock(ServerSocketChannel.class));
        channel.setContext(serverChannelContext);
        when(serverChannelContext.getChannel()).thenReturn(channel);
        when(channelFactory.acceptNioChannel(same(context), same(selectorSupplier))).thenReturn(childChannel);

        handler.acceptChannel(serverChannelContext);

        verify(serverChannelContext).acceptChannels(selectorSupplier);
    }

    public void testAcceptExceptionCallsExceptionHandler() throws IOException {
        ServerChannelContext serverChannelContext = mock(ServerChannelContext.class);
        IOException exception = new IOException();
        handler.acceptException(serverChannelContext, exception);

        verify(serverChannelContext).handleException(exception);
    }

    private class DoNotRegisterContext extends ServerChannelContext {


        @SuppressWarnings("unchecked")
        DoNotRegisterContext(NioServerSocketChannel channel, AcceptingSelector selector, Consumer<NioSocketChannel> acceptor) {
            super(channel, channelFactory, selector, acceptor, mock(Consumer.class));
        }

        @Override
        public void register() {
            setSelectionKey(new TestSelectionKey(0));
        }
    }
}
