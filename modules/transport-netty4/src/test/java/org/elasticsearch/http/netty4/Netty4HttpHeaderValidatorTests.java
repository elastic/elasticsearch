/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class Netty4HttpHeaderValidatorTests extends ESTestCase {

    private final AtomicReference<Object> header = new AtomicReference<>();
    private final AtomicReference<ActionListener<Void>> listener = new AtomicReference<>();
    private EmbeddedChannel channel;
    private Netty4HttpHeaderValidator netty4HttpHeaderValidator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        reset();
    }

    private void reset() {
        channel = new EmbeddedChannel();
        header.set(null);
        listener.set(null);
        BiConsumer<Object, ActionListener<Void>> validator = (o, validationCompleteListener) -> {
            header.set(o);
            listener.set(validationCompleteListener);
        };
        netty4HttpHeaderValidator = new Netty4HttpHeaderValidator(validator);
        channel.pipeline().addLast(netty4HttpHeaderValidator);
    }

    public void testValidationPausesAndResumesData() {
        assertTrue(channel.config().isAutoRead());

        final DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        DefaultHttpContent content = new DefaultHttpContent(Unpooled.buffer(4));
        channel.writeInbound(request);
        channel.writeInbound(content);

        assertThat(header.get(), sameInstance(request));
        assertThat(channel.readInbound(), nullValue());
        assertFalse(channel.config().isAutoRead());

        listener.get().onResponse(null);
        channel.runPendingTasks();
        assertTrue(channel.config().isAutoRead());
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.FORWARDING_DATA));
        assertThat(channel.readInbound(), sameInstance(request));
        assertThat(channel.readInbound(), sameInstance(content));
        assertThat(channel.readInbound(), nullValue());
        assertThat(content.refCnt(), equalTo(1));

        DefaultLastHttpContent lastContent = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(lastContent);
        assertTrue(channel.config().isAutoRead());
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.WAITING_TO_START));
        assertThat(channel.readInbound(), sameInstance(lastContent));
        assertThat(lastContent.refCnt(), equalTo(1));

        channel.writeInbound(request);
        assertFalse(channel.config().isAutoRead());
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.QUEUEING_DATA));
    }

    public void testValidationErrorForwardsAsDecoderErrorMessage() {
        final DefaultHttpRequest request1 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        DefaultHttpContent content = new DefaultHttpContent(Unpooled.buffer(4));
        channel.writeInbound(request1);
        channel.writeInbound(content);

        assertThat(header.get(), sameInstance(request1));
        assertThat(channel.readInbound(), nullValue());
        assertFalse(channel.config().isAutoRead());

        Exception failure = new Exception("Failure");
        listener.get().onFailure(failure);
        channel.runPendingTasks();
        assertFalse(channel.config().isAutoRead());
        DefaultHttpRequest failed = channel.readInbound();
        assertThat(failed, sameInstance(request1));
        assertThat(failed.headers().get(HttpHeaderNames.CONNECTION), equalTo(HttpHeaderValues.CLOSE.toString()));
        assertTrue(failed.decoderResult().isFailure());
        HeaderValidationException cause = (HeaderValidationException) failed.decoderResult().cause();
        assertThat(cause.getCause().getCause(), equalTo(failure));
        assertTrue(cause.shouldCloseChannel());
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.DROPPING_DATA_PERMANENTLY));

        reset();
        final DefaultHttpRequest request2 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        channel.writeInbound(request2);
        channel.writeInbound(content);

        ElasticsearchException elasticsearchFailure = new ElasticsearchException("Failure");
        listener.get().onFailure(elasticsearchFailure);
        channel.runPendingTasks();
        assertFalse(channel.config().isAutoRead());
        failed = channel.readInbound();
        assertThat(failed, sameInstance(request2));
        assertThat(failed.headers().get(HttpHeaderNames.CONNECTION), equalTo(HttpHeaderValues.CLOSE.toString()));
        assertTrue(failed.decoderResult().isFailure());
        cause = (HeaderValidationException) failed.decoderResult().cause();
        assertThat(failed.decoderResult().cause().getCause(), equalTo(elasticsearchFailure));
        assertTrue(cause.shouldCloseChannel());
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.DROPPING_DATA_PERMANENTLY));

        reset();
        final DefaultHttpRequest request3 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        channel.writeInbound(request3);
        channel.writeInbound(content);

        listener.get().onFailure(new HeaderValidationException(elasticsearchFailure, false));
        channel.runPendingTasks();
        assertTrue(channel.config().isAutoRead());
        failed = channel.readInbound();
        assertThat(failed, sameInstance(request3));
        assertThat(failed.headers().get(HttpHeaderNames.CONNECTION), nullValue());
        assertTrue(failed.decoderResult().isFailure());
        cause = (HeaderValidationException) failed.decoderResult().cause();
        assertThat(failed.decoderResult().cause().getCause(), equalTo(elasticsearchFailure));
        assertFalse(cause.shouldCloseChannel());
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.DROPPING_DATA_UNTIL_NEXT_REQUEST));
    }

    public void testValidationHandlesMultipleQueuedUpMessages() {
        assertTrue(channel.config().isAutoRead());

        final DefaultHttpRequest request1 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        DefaultHttpContent content1 = new DefaultHttpContent(Unpooled.buffer(4));
        DefaultLastHttpContent lastContent1 = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(request1);
        channel.writeInbound(content1);
        channel.writeInbound(lastContent1);
        final DefaultHttpRequest request2 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        DefaultHttpContent content2 = new DefaultHttpContent(Unpooled.buffer(4));
        DefaultLastHttpContent lastContent2 = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(request2);
        channel.writeInbound(content2);
        channel.writeInbound(lastContent2);

        assertThat(header.get(), sameInstance(request1));
        assertThat(channel.readInbound(), nullValue());
        assertFalse(channel.config().isAutoRead());

        listener.get().onResponse(null);
        channel.runPendingTasks();
        assertThat(channel.readInbound(), sameInstance(request1));
        assertThat(channel.readInbound(), sameInstance(content1));
        assertThat(channel.readInbound(), sameInstance(lastContent1));
        assertThat(content1.refCnt(), equalTo(1));

        assertThat(header.get(), sameInstance(request2));

        assertFalse(channel.config().isAutoRead());
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.QUEUEING_DATA));
        assertThat(channel.readInbound(), nullValue());

        listener.get().onResponse(null);
        channel.runPendingTasks();
        assertThat(channel.readInbound(), sameInstance(request2));
        assertThat(channel.readInbound(), sameInstance(content2));
        assertThat(channel.readInbound(), sameInstance(lastContent2));
        assertThat(content2.refCnt(), equalTo(1));

        assertTrue(channel.config().isAutoRead());
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.WAITING_TO_START));
        assertThat(channel.readInbound(), nullValue());
    }

    public void testValidationFailureWithNoRecovery() {
        assertTrue(channel.config().isAutoRead());

        final DefaultHttpRequest request1 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        DefaultHttpContent content1 = new DefaultHttpContent(Unpooled.buffer(4));
        DefaultLastHttpContent lastContent1 = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(request1);
        channel.writeInbound(content1);
        channel.writeInbound(lastContent1);
        final DefaultHttpRequest request2 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        DefaultHttpContent content2 = new DefaultHttpContent(Unpooled.buffer(4));
        DefaultLastHttpContent lastContent2 = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(request2);
        channel.writeInbound(content2);

        assertThat(header.get(), sameInstance(request1));
        assertThat(channel.readInbound(), nullValue());
        assertFalse(channel.config().isAutoRead());

        assertThat(content1.refCnt(), equalTo(2));
        assertThat(content2.refCnt(), equalTo(2));
        listener.get().onFailure(new HeaderValidationException(new ElasticsearchException("Boom"), true));
        channel.runPendingTasks();
        assertThat(channel.readInbound(), sameInstance(request1));
        assertThat(content1.refCnt(), equalTo(1));
        assertThat(content2.refCnt(), equalTo(1));
        assertFalse(channel.config().isAutoRead());

        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.DROPPING_DATA_PERMANENTLY));
        assertThat(channel.readInbound(), nullValue());

        channel.writeInbound(lastContent2);
        assertThat(lastContent2.refCnt(), equalTo(1));
        assertThat(channel.readInbound(), nullValue());
    }

    public void testValidationFailureCanRecover() {
        assertTrue(channel.config().isAutoRead());

        final DefaultHttpRequest request1 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        DefaultHttpContent content1 = new DefaultHttpContent(Unpooled.buffer(4));
        DefaultLastHttpContent lastContent1 = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(request1);
        channel.writeInbound(content1);
        channel.writeInbound(lastContent1);
        final DefaultHttpRequest request2 = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        DefaultHttpContent content2 = new DefaultHttpContent(Unpooled.buffer(4));
        DefaultLastHttpContent lastContent2 = new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER);
        channel.writeInbound(request2);
        channel.writeInbound(content2);

        boolean finishSecondRequest = randomBoolean();
        if (finishSecondRequest) {
            channel.writeInbound(lastContent2);
        }

        assertThat(header.get(), sameInstance(request1));
        assertThat(channel.readInbound(), nullValue());
        assertFalse(channel.config().isAutoRead());

        assertThat(content1.refCnt(), equalTo(2));
        assertThat(content2.refCnt(), equalTo(2));
        listener.get().onFailure(new HeaderValidationException(new ElasticsearchException("Boom"), false));
        channel.runPendingTasks();
        assertThat(channel.readInbound(), sameInstance(request1));
        assertThat(content1.refCnt(), equalTo(1));

        assertThat(header.get(), sameInstance(request2));

        assertFalse(channel.config().isAutoRead());
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.QUEUEING_DATA));
        assertThat(channel.readInbound(), nullValue());

        listener.get().onResponse(null);
        channel.runPendingTasks();
        assertThat(channel.readInbound(), sameInstance(request2));
        assertThat(channel.readInbound(), sameInstance(content2));
        assertThat(content2.refCnt(), equalTo(1));

        if (finishSecondRequest == false) {
            assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.FORWARDING_DATA));
            assertTrue(channel.config().isAutoRead());
            assertThat(channel.readInbound(), nullValue());

            channel.writeInbound(lastContent2);
        }

        assertThat(channel.readInbound(), sameInstance(lastContent2));
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.WAITING_TO_START));
        assertTrue(channel.config().isAutoRead());
    }

    public void testValidatorWithLargeMessage() {
        assertTrue(channel.config().isAutoRead());

        final DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/uri");
        channel.writeInbound(request);

        for (int i = 0; i < 64; ++i) {
            channel.writeInbound(new DefaultHttpContent(Unpooled.buffer(4)));
        }
        channel.writeInbound(new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER));
        channel.writeInbound(request);

        assertThat(header.get(), sameInstance(request));
        assertThat(channel.readInbound(), nullValue());
        assertFalse(channel.config().isAutoRead());

        listener.get().onResponse(null);
        channel.runPendingTasks();
        assertFalse(channel.config().isAutoRead());
        // We have already initiated the next data queuing
        assertThat(netty4HttpHeaderValidator.getState(), equalTo(Netty4HttpHeaderValidator.STATE.QUEUEING_DATA));
        assertThat(channel.readInbound(), sameInstance(request));
        for (int i = 0; i < 64; ++i) {
            assertThat(channel.readInbound(), instanceOf(DefaultHttpContent.class));
        }
        assertThat(channel.readInbound(), instanceOf(LastHttpContent.class));
        assertThat(channel.readInbound(), nullValue());
    }
}
