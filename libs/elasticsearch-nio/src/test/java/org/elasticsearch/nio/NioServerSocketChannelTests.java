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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
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
        selector = new AcceptingSelector(new AcceptorEventHandler(logger, mock(Supplier.class)));
        thread = new Thread(selector::runLoop);
        closedRawChannel = new AtomicBoolean(false);
        thread.start();
        FutureUtils.get(selector.isRunningFuture());
    }

    @After
    public void stopSelector() throws IOException, InterruptedException {
        selector.close();
        thread.join();
    }

    @SuppressWarnings("unchecked")
    public void testClose() throws Exception {
        AtomicBoolean isClosed = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);

        try (ServerSocketChannel rawChannel = ServerSocketChannel.open()) {
            NioServerSocketChannel channel = new NioServerSocketChannel(rawChannel, mock(ChannelFactory.class), selector);
            channel.setContext(new ServerChannelContext(channel, mock(Consumer.class), mock(BiConsumer.class)));
            channel.addCloseListener(ActionListener.toBiConsumer(new ActionListener<Void>() {
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
            }));

            assertTrue(channel.isOpen());
            assertTrue(rawChannel.isOpen());
            assertFalse(isClosed.get());

            PlainActionFuture<Void> closeFuture = PlainActionFuture.newFuture();
            channel.addCloseListener(ActionListener.toBiConsumer(closeFuture));
            selector.queueChannelClose(channel);
            closeFuture.actionGet();


            assertFalse(rawChannel.isOpen());
            assertFalse(channel.isOpen());
            latch.await();
            assertTrue(isClosed.get());
        }
    }
}
