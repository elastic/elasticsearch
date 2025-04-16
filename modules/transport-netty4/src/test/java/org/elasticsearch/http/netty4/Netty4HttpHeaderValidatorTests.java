/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.flow.FlowControlHandler;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Netty4HttpHeaderValidatorTests extends ESTestCase {
    private EmbeddedChannel channel;
    private BlockingQueue<ValidationRequest> validatorRequestQueue;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        validatorRequestQueue = new LinkedBlockingQueue<>();
        channel = new EmbeddedChannel(
            new Netty4HttpHeaderValidator(
                (httpRequest, channel, listener) -> validatorRequestQueue.add(new ValidationRequest(httpRequest, channel, listener)),
                new ThreadContext(Settings.EMPTY)
            )
        );
        channel.config().setAutoRead(false);
    }

    HttpRequest newHttpRequest() {
        return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "");
    }

    HttpContent newHttpContent() {
        return new DefaultHttpContent(Unpooled.buffer());
    }

    LastHttpContent newLastHttpContent() {
        return new DefaultLastHttpContent();
    }

    public void testValidatorReceiveHttpRequest() {
        channel.writeInbound(newHttpRequest());
        assertEquals(1, validatorRequestQueue.size());
        assertNull(channel.readInbound());
    }

    public void testDecoderFailurePassThrough() {
        for (var i = 0; i < 1000; i++) {
            var httpRequest = newHttpRequest();
            httpRequest.setDecoderResult(DecoderResult.failure(new Exception("bad")));
            channel.writeInbound(httpRequest);
            assertEquals(httpRequest, channel.readInbound());
        }
    }

    /**
     * Sends back-to-back http requests and randomly fail validation.
     * Ensures that invalid requests drop content and valid pass through.
     */
    public void testMixedValidationResults() {
        for (var i = 0; i < 1000; i++) {
            var shouldPassValidation = randomBoolean();
            var request = newHttpRequest();
            var content = newHttpContent();
            var last = newLastHttpContent();

            channel.writeInbound(request);
            var validationRequest = validatorRequestQueue.poll();
            assertNotNull(validationRequest);
            if (shouldPassValidation) {
                validationRequest.listener.onResponse(null);
            } else {
                validationRequest.listener.onFailure(new ValidationException());
            }
            channel.runPendingTasks();

            var gotRequest = channel.readInbound();
            assertEquals(
                "should set decoder result failure for invalid request",
                shouldPassValidation,
                ((HttpRequest) gotRequest).decoderResult().isSuccess()
            );
            assertEquals(request, gotRequest);

            channel.writeInbound(content);
            channel.writeInbound(last);
            if (shouldPassValidation) {
                assertEquals("should pass content for valid request", content, channel.readInbound());
                content.release();
                assertEquals(last, channel.readInbound());
                last.release();
            } else {
                assertNull("should drop content for invalid request", channel.readInbound());
            }
        }
    }

    public void testIgnoreReadWhenValidating() {
        channel.pipeline().addFirst(new FlowControlHandler()); // catch all inbound messages

        channel.writeInbound(newHttpRequest());
        channel.writeInbound(newLastHttpContent()); // should hold by flow-control-handler
        assertNull("nothing should pass yet", channel.readInbound());

        channel.read();
        var validationRequest = validatorRequestQueue.poll();
        assertNotNull(validationRequest);

        channel.read();
        assertNull("should ignore read while validating", channel.readInbound());

        validationRequest.listener.onResponse(null);
        channel.runPendingTasks();
        assertTrue("http request should pass", channel.readInbound() instanceof HttpRequest);
        assertNull("content should not pass yet, need explicit read", channel.readInbound());

        channel.read();
        asInstanceOf(LastHttpContent.class, channel.readInbound()).release();
    }

    public void testWithFlowControlAndAggregator() {
        channel.pipeline().addFirst(new FlowControlHandler());
        channel.pipeline().addLast(new Netty4HttpAggregator(8192, (req) -> true, new HttpRequestDecoder()));

        channel.writeInbound(newHttpRequest());
        channel.writeInbound(newHttpContent());
        channel.writeInbound(newLastHttpContent());

        channel.read();
        assertNull("should ignore read while validating", channel.readInbound());

        var validationRequest = validatorRequestQueue.poll();
        assertNotNull(validationRequest);
        validationRequest.listener.onResponse(null);
        channel.runPendingTasks();

        asInstanceOf(FullHttpRequest.class, channel.readInbound()).release();
    }

    record ValidationRequest(HttpRequest request, Channel channel, ActionListener<Void> listener) {}
}
