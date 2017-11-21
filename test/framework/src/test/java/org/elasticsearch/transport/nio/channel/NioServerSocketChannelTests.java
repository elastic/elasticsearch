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

package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.nio.AcceptingSelector;
import org.elasticsearch.transport.nio.AcceptorEventHandler;
import org.elasticsearch.transport.nio.OpenChannels;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;

public class NioServerSocketChannelTests extends ESTestCase {

    private AcceptingSelector selector;
    private AtomicBoolean closedRawChannel;
    private Thread thread;

    @Before
    @SuppressWarnings("unchecked")
    public void setSelector() throws IOException {
        selector = new AcceptingSelector(new AcceptorEventHandler(logger, mock(OpenChannels.class), mock(Supplier.class), (c) -> {}));
        thread = new Thread(selector::runLoop);
        closedRawChannel = new AtomicBoolean(false);
        thread.start();
        selector.isRunningFuture().actionGet();
    }

    @After
    public void stopSelector() throws IOException, InterruptedException {
        selector.close();
        thread.join();
    }

    public void testClose() throws Exception {
        AtomicBoolean isClosed = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        NioChannel channel = new DoNotCloseServerChannel("nio", mock(ServerSocketChannel.class), mock(ChannelFactory.class), selector);

        channel.addCloseListener(new ActionListener<Void>() {
            @Override
            public void onResponse(Void o) {
                isClosed.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                isClosed.set(true);
                latch.countDown();
            }
        });

        assertTrue(channel.isOpen());
        assertFalse(closedRawChannel.get());
        assertFalse(isClosed.get());

        PlainActionFuture<Void> closeFuture = PlainActionFuture.newFuture();
        channel.addCloseListener(closeFuture);
        channel.close();
        closeFuture.actionGet();


        assertTrue(closedRawChannel.get());
        assertFalse(channel.isOpen());
        latch.await();
        assertTrue(isClosed.get());
    }

    private class DoNotCloseServerChannel extends DoNotRegisterServerChannel {

        private DoNotCloseServerChannel(String profile, ServerSocketChannel channel, ChannelFactory channelFactory,
                                        AcceptingSelector selector) throws IOException {
            super(channel, channelFactory, selector);
        }

        @Override
        void closeRawChannel() throws IOException {
            closedRawChannel.set(true);
        }
    }
}
