/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.transport.Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX;

public class AcceptChannelHeaderThreadContextTests extends ESTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(TEST_MOCK_TRANSPORT_THREAD_PREFIX);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testSuccessfulSyncValidationUntamperedThreadContext() throws Exception {
        AtomicBoolean isValidationSuccessful = new AtomicBoolean(true);
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup(1, threadPool.generic());
        NioSocketChannel channel = new NioSocketChannel();
        channel.pipeline()
            .addLast(
                new AcceptChannelHandler((profile, remoteAddr) -> isValidationSuccessful.get(), "test", threadPool.getThreadContext())
            );
        var registerFuture = eventLoopGroup.submit(() -> { eventLoopGroup.register(channel); });
        registerFuture.get();
        registerFuture.get().toString();
        //        EmbeddedChannel channel = new EmbeddedChannel(
//            new AcceptChannelHandler((profile, remoteAddr) -> isValidationSuccessful.get(), "mockProfile", threadPool.getThreadContext())
//        );
        //        channel.pipeline().addLast(new AcceptChannelHandler((profile, remoteAddr) -> isValidationSuccessful.get(), "mockProfile", threadPool.getThreadContext()));
//        channel.pipeline().addLast(defaultContextAssertingChannelHandler(threadPool.getThreadContext()));
        // send first request through
        sendRequestThrough(channel, isValidationSuccessful.get());
        // send second request through, to check in case the context got stained by the first one through
        if (randomBoolean()) {
            isValidationSuccessful.set(false);
        }
        sendRequestThrough(channel, isValidationSuccessful.get());
    }

    private void sendRequestThrough(Channel channel, boolean success) throws Exception {
//        channel.bind(new InetSocketAddress("8.8.8.8", 0));
//        channel.register();
//        threadPool.generic().submit(() -> {
//            DefaultHttpRequest request1 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
//            channel.writeInbound(request1);
//            DefaultHttpContent content1 = randomBoolean() ? new DefaultHttpContent(Unpooled.buffer(4)) : null;
//            if (content1 != null) {
//                channel.writeInbound(content1);
//            }
//            DefaultLastHttpContent lastContent1 = new DefaultLastHttpContent(Unpooled.buffer(4));
//            channel.writeInbound(lastContent1);
//            channel.runPendingTasks();
//            assertThat(channel.readInbound(), sameInstance(request1));
//            if (content1 != null && success) {
//                assertThat(channel.readInbound(), sameInstance(content1));
//            }
//            if (success) {
//                assertThat(channel.readInbound(), sameInstance(lastContent1));
//            }
//            assertThat(channel.readInbound(), nullValue());
//        }).get(20, TimeUnit.SECONDS);
    }
}
