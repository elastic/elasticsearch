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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.http.netty4.internal.HttpValidator;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.hamcrest.Matchers.instanceOf;

public class Netty4HttpHeaderValidatorTests extends ESTestCase {
    private EmbeddedChannel channel;
    private BlockingQueue<ValidationRequest> validatorRequestQueue;
    private HttpValidator httpValidator = (httpRequest, channel, listener) -> validatorRequestQueue.add(
        new ValidationRequest(httpRequest, channel, listener)
    );

    @Override
    public void setUp() throws Exception {
        super.setUp();
        validatorRequestQueue = new LinkedBlockingQueue<>();
        channel = new EmbeddedChannel(
            new Netty4HttpHeaderValidator(
                (httpRequest, channel, listener) -> httpValidator.validate(httpRequest, channel, listener),
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
        // send a valid request so that the buffer is nonempty
        final var validRequest = newHttpRequest();
        channel.writeInbound(validRequest);
        channel.writeInbound(newLastHttpContent());

        // follow it with an invalid request which should be buffered
        final var invalidHttpRequest1 = newHttpRequest();
        invalidHttpRequest1.setDecoderResult(DecoderResult.failure(new Exception("simulated decoder failure 1")));
        channel.writeInbound(invalidHttpRequest1);

        // handle the first request
        if (randomBoolean()) {
            Objects.requireNonNull(validatorRequestQueue.poll()).listener().onResponse(null);
            channel.runPendingTasks();
            assertSame(validRequest, channel.readInbound());
            channel.read();
            asInstanceOf(LastHttpContent.class, channel.readInbound()).release();
        } else {
            Objects.requireNonNull(validatorRequestQueue.poll()).listener().onFailure(new Exception("simulated validation failure"));
            channel.runPendingTasks();
            assertSame(validRequest, channel.readInbound());
        }

        // handle the second request, which is read from the buffer and passed on without validation
        assertNull(channel.readInbound());
        channel.read();
        assertSame(invalidHttpRequest1, channel.readInbound());

        // send another invalid request which is passed straight through
        final var invalidHttpRequest2 = newHttpRequest();
        invalidHttpRequest2.setDecoderResult(DecoderResult.failure(new Exception("simulated decoder failure 2")));
        channel.writeInbound(invalidHttpRequest2);
        if (randomBoolean()) {
            channel.read(); // optional read
        }
        assertSame(invalidHttpRequest2, channel.readInbound());
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
        channel.writeInbound(newHttpRequest());
        channel.writeInbound(newLastHttpContent());
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

    public void testWithAggregator() {
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

    public void testBufferPipelinedRequestsWhenValidating() {
        final var expectedChunks = new ArrayDeque<HttpContent>();
        expectedChunks.addLast(newHttpContent());

        // write one full request and one incomplete request received all at once
        channel.writeInbound(newHttpRequest());
        channel.writeInbound(newLastHttpContent());
        channel.writeInbound(newHttpRequest());
        channel.writeInbound(expectedChunks.peekLast());
        assertNull("nothing should pass yet", channel.readInbound());

        if (randomBoolean()) {
            channel.read();
        }
        var validationRequest = validatorRequestQueue.poll();
        assertNotNull(validationRequest);

        channel.read();
        assertNull("should ignore read while validating", channel.readInbound());

        validationRequest.listener().onResponse(null);
        channel.runPendingTasks();
        assertTrue("http request should pass", channel.readInbound() instanceof HttpRequest);
        assertNull("content should not pass yet, need explicit read", channel.readInbound());

        channel.read();
        asInstanceOf(LastHttpContent.class, channel.readInbound()).release();

        // should have started to validate the next request
        channel.read();
        assertNull("should ignore read while validating", channel.readInbound());
        Objects.requireNonNull(validatorRequestQueue.poll()).listener().onResponse(null);

        channel.runPendingTasks();
        assertThat("next http request should pass", channel.readInbound(), instanceOf(HttpRequest.class));

        // another chunk received and is buffered, nothing is sent downstream
        expectedChunks.addLast(newHttpContent());
        channel.writeInbound(expectedChunks.peekLast());
        assertNull(channel.readInbound());
        assertFalse(channel.hasPendingTasks());

        // the first chunk is now emitted on request
        channel.read();
        var nextChunk = asInstanceOf(HttpContent.class, channel.readInbound());
        assertSame(nextChunk, expectedChunks.pollFirst());
        nextChunk.release();
        assertNull(channel.readInbound());
        assertFalse(channel.hasPendingTasks());

        // and the second chunk
        channel.read();
        nextChunk = asInstanceOf(HttpContent.class, channel.readInbound());
        assertSame(nextChunk, expectedChunks.pollFirst());
        nextChunk.release();
        assertNull(channel.readInbound());
        assertFalse(channel.hasPendingTasks());

        // buffer is now drained, no more chunks available
        if (randomBoolean()) {
            channel.read(); // optional read
        }
        assertNull(channel.readInbound());
        assertTrue(expectedChunks.isEmpty());
        assertFalse(channel.hasPendingTasks());

        // subsequent chunks are passed straight through without another read()
        expectedChunks.addLast(newHttpContent());
        channel.writeInbound(expectedChunks.peekLast());
        nextChunk = asInstanceOf(HttpContent.class, channel.readInbound());
        assertSame(nextChunk, expectedChunks.pollFirst());
        nextChunk.release();
        assertNull(channel.readInbound());
        assertFalse(channel.hasPendingTasks());
    }

    public void testDropChunksOnValidationFailure() {
        // write an incomplete request which will be marked as invalid
        channel.writeInbound(newHttpRequest());
        channel.writeInbound(newHttpContent());
        assertNull("nothing should pass yet", channel.readInbound());

        var validationRequest = validatorRequestQueue.poll();
        assertNotNull(validationRequest);
        validationRequest.listener().onFailure(new Exception("simulated validation failure"));

        // failed request is passed downstream
        channel.runPendingTasks();
        var inboundRequest = asInstanceOf(HttpRequest.class, channel.readInbound());
        assertTrue(inboundRequest.decoderResult().isFailure());
        assertEquals("simulated validation failure", inboundRequest.decoderResult().cause().getMessage());

        // chunk is not emitted (the buffer is now drained)
        assertNull(channel.readInbound());
        if (randomBoolean()) {
            channel.read();
            assertNull(channel.readInbound());
        }

        // next chunk is also not emitted (it is released on receipt, not buffered)
        channel.writeInbound(newLastHttpContent());
        assertNull(channel.readInbound());
        if (randomBoolean()) {
            channel.read();
            assertNull(channel.readInbound());
        }
        assertFalse(channel.hasPendingTasks());

        // next request triggers validation again
        final var nextRequest = newHttpRequest();
        channel.writeInbound(nextRequest);
        Objects.requireNonNull(validatorRequestQueue.poll()).listener().onResponse(null);
        channel.runPendingTasks();

        if (randomBoolean()) {
            channel.read(); // optional read
        }
        assertSame(nextRequest, channel.readInbound());
        assertFalse(channel.hasPendingTasks());
    }

    public void testInlineValidationDoesNotFork() {
        httpValidator = (httpRequest, channel, listener) -> listener.onResponse(null);
        final var httpRequest = newHttpRequest();
        channel.writeInbound(httpRequest);
        assertFalse(channel.hasPendingTasks());
        assertSame(httpRequest, channel.readInbound());
    }

    record ValidationRequest(HttpRequest request, Channel channel, ActionListener<Void> listener) {}
}
