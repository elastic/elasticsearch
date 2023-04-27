/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transports;
import org.junit.After;

import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.transport.Transports.TEST_MOCK_TRANSPORT_THREAD_PREFIX;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Tests that a misbehaving http header validator, from {@link Netty4HttpHeaderValidator}, cannot change the thread context
 * outside the validation scope.
 * This also tests that a threading validator cannot fork the following netty pipeline handlers on a different thread.
 */
public class Netty4HttpHeaderThreadContextTests extends ESTestCase {
    private EmbeddedChannel channel;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        channel = new EmbeddedChannel();
        threadPool = new TestThreadPool(TEST_MOCK_TRANSPORT_THREAD_PREFIX);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testSuccessfulSyncValidationUntamperedThreadContext() throws Exception {
        // first validation is successful (and sync)
        final AtomicBoolean isValidationSuccessful = new AtomicBoolean(true);
        channel.pipeline()
            .addLast(
                new Netty4HttpHeaderValidator(
                    getValidator(threadPool.executor(ThreadPool.Names.SAME), isValidationSuccessful, null),
                    threadPool.getThreadContext()
                )
            );
        channel.pipeline().addLast(defaultContextAssertingChannelHandler(threadPool.getThreadContext()));
        // send first request through
        sendRequestThrough(isValidationSuccessful.get(), null);
        // send second request through, to check in case the context got stained by the first one through
        if (randomBoolean()) {
            isValidationSuccessful.set(false);
        }
        sendRequestThrough(isValidationSuccessful.get(), null);
    }

    public void testFailedSyncValidationUntamperedThreadContext() throws Exception {
        // first validation is failed (and sync)
        final AtomicBoolean isValidationSuccessful = new AtomicBoolean(false);
        channel.pipeline()
            .addLast(
                new Netty4HttpHeaderValidator(
                    getValidator(threadPool.executor(ThreadPool.Names.SAME), isValidationSuccessful, null),
                    threadPool.getThreadContext()
                )
            );
        channel.pipeline().addLast(defaultContextAssertingChannelHandler(threadPool.getThreadContext()));
        // send first request through
        sendRequestThrough(isValidationSuccessful.get(), null);
        // send second request through, to check in case the context got stained by the first one through
        if (randomBoolean()) {
            isValidationSuccessful.set(true);
        }
        sendRequestThrough(isValidationSuccessful.get(), null);
    }

    public void testSuccessfulAsyncValidationUntamperedThreadContext() throws Exception {
        // first validation is successful (and sync)
        final AtomicBoolean isValidationSuccessful = new AtomicBoolean(true);
        final Semaphore validationDone = new Semaphore(0);
        channel.pipeline()
            .addLast(
                new Netty4HttpHeaderValidator(
                    // use a different executor/thread for the validator
                    getValidator(threadPool.executor(ThreadPool.Names.MANAGEMENT), isValidationSuccessful, validationDone),
                    threadPool.getThreadContext()
                )
            );
        channel.pipeline().addLast(defaultContextAssertingChannelHandler(threadPool.getThreadContext()));
        // send first request through
        sendRequestThrough(isValidationSuccessful.get(), validationDone);
        // send second request through, to check in case the context got stained by the first one through
        if (randomBoolean()) {
            isValidationSuccessful.set(false);
        }
        sendRequestThrough(isValidationSuccessful.get(), validationDone);
    }

    public void testUnsuccessfulAsyncValidationUntamperedThreadContext() throws Exception {
        // first validation is failed (and sync)
        final AtomicBoolean isValidationSuccessful = new AtomicBoolean(false);
        final Semaphore validationDone = new Semaphore(0);
        channel.pipeline()
            .addLast(
                new Netty4HttpHeaderValidator(
                    // use a different executor/thread for the validator
                    getValidator(threadPool.executor(ThreadPool.Names.MANAGEMENT), isValidationSuccessful, validationDone),
                    threadPool.getThreadContext()
                )
            );
        channel.pipeline().addLast(defaultContextAssertingChannelHandler(threadPool.getThreadContext()));
        // send first request through
        sendRequestThrough(isValidationSuccessful.get(), validationDone);
        // send second request through, to check in case the context got stained by the first one through
        if (randomBoolean()) {
            isValidationSuccessful.set(true);
        }
        sendRequestThrough(isValidationSuccessful.get(), validationDone);
    }

    private TriConsumer<HttpRequest, Channel, ActionListener<Void>> getValidator(
        ExecutorService executorService,
        AtomicBoolean success,
        Semaphore validationDone
    ) {
        return (httpRequest, channel, listener) -> {
            executorService.submit(() -> {
                if (randomBoolean()) {
                    threadPool.getThreadContext().putHeader(randomAlphaOfLength(16), "tampered thread context");
                } else {
                    threadPool.getThreadContext().putTransient(randomAlphaOfLength(16), "tampered thread context");
                }
                if (success.get()) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(new Exception("Validation failure"));
                }
                if (validationDone != null) {
                    validationDone.release();
                }
            });
        };
    };

    private void sendRequestThrough(boolean success, Semaphore validationDone) throws Exception {
        threadPool.generic().submit(() -> {
            DefaultHttpRequest request1 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
            channel.writeInbound(request1);
            DefaultHttpContent content1 = randomBoolean() ? new DefaultHttpContent(Unpooled.buffer(4)) : null;
            if (content1 != null) {
                channel.writeInbound(content1);
            }
            DefaultLastHttpContent lastContent1 = new DefaultLastHttpContent(Unpooled.buffer(4));
            channel.writeInbound(lastContent1);
            if (validationDone != null) {
                try {
                    validationDone.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            channel.runPendingTasks();
            assertThat(channel.readInbound(), sameInstance(request1));
            if (content1 != null && success) {
                assertThat(channel.readInbound(), sameInstance(content1));
            }
            if (success) {
                assertThat(channel.readInbound(), sameInstance(lastContent1));
            }
            assertThat(channel.readInbound(), nullValue());
        }).get(20, TimeUnit.SECONDS);
    }

    private static ChannelDuplexHandler defaultContextAssertingChannelHandler(ThreadContext threadContext) {
        return new ChannelDuplexHandler() {
            @Override
            public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception {
                if (false == threadContext.isDefaultContext()) {
                    throw new AssertionError("tampered thread context");
                }
                Transports.assertTransportThread();
                super.bind(ctx, localAddress, promise);
            }

            @Override
            public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise)
                throws Exception {
                if (false == threadContext.isDefaultContext()) {
                    throw new AssertionError("tampered thread context");
                }
                Transports.assertTransportThread();
                super.connect(ctx, remoteAddress, localAddress, promise);
            }

            @Override
            public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
                if (false == threadContext.isDefaultContext()) {
                    throw new AssertionError("tampered thread context");
                }
                Transports.assertTransportThread();
                super.disconnect(ctx, promise);
            }

            @Override
            public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
                if (false == threadContext.isDefaultContext()) {
                    throw new AssertionError("tampered thread context");
                }
                Transports.assertTransportThread();
                super.close(ctx, promise);
            }

            @Override
            public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
                if (false == threadContext.isDefaultContext()) {
                    throw new AssertionError("tampered thread context");
                }
                Transports.assertTransportThread();
                super.deregister(ctx, promise);
            }

            @Override
            public void read(ChannelHandlerContext ctx) throws Exception {
                if (false == threadContext.isDefaultContext()) {
                    throw new AssertionError("tampered thread context");
                }
                Transports.assertTransportThread();
                super.read(ctx);
            }

            @Override
            public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
                if (false == threadContext.isDefaultContext()) {
                    throw new AssertionError("tampered thread context");
                }
                Transports.assertTransportThread();
                super.write(ctx, msg, promise);
            }

            @Override
            public void flush(ChannelHandlerContext ctx) throws Exception {
                if (false == threadContext.isDefaultContext()) {
                    throw new AssertionError("tampered thread context");
                }
                Transports.assertTransportThread();
                super.flush(ctx);
            }
        };
    }
}
