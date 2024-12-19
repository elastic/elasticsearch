/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.http.HttpRequest;
import org.elasticsearch.http.HttpResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.http.HttpBody.ChunkHandler;

public class RestContentAggregator {

    /**
     * Aggregates streamed HTTP content and completes listener with RestRequest with full content.
     * Completes with exception on unexpected HTTP content (ie content-length is 0, but receive
     * non-empty chunk).
     */
    public static void aggregate(RestRequest request, RestChannel channel, AggregateConsumer result) {
        ChunkHandler handler;
        if (request.contentLength() == 0) {
            handler = new NoContent(request, channel, result);
        } else {
            handler = new ChunkAggregator(request, channel, result);
        }
        var stream = request.contentStream();
        stream.setHandler(handler);
        stream.next();
    }

    @FunctionalInterface
    public interface AggregateConsumer {
        void accept(RestRequest request, RestChannel channel) throws Exception;
    }

    /**
     * Wraps streamed {@link HttpRequest} with aggregated content.
     * Does not replace the original content-length header.
     * Since full content is already available, we can use its length instead.
     */
    private static class AggregatedHttpRequest implements HttpRequest {
        final HttpRequest streamedRequest;
        final HttpBody.Full aggregatedContent;
        final AtomicBoolean released = new AtomicBoolean(false);

        private AggregatedHttpRequest(HttpRequest streamedRequest, HttpBody.Full aggregatedContent) {
            this.streamedRequest = streamedRequest;
            this.aggregatedContent = aggregatedContent;
        }

        @Override
        public int contentLength() {
            return aggregatedContent.bytes().length();
        }

        @Override
        public HttpBody body() {
            return aggregatedContent;
        }

        @Override
        public List<String> strictCookies() {
            return streamedRequest.strictCookies();
        }

        @Override
        public HttpVersion protocolVersion() {
            return streamedRequest.protocolVersion();
        }

        @Override
        public HttpRequest removeHeader(String header) {
            var request = streamedRequest.removeHeader(header);
            return new AggregatedHttpRequest(request, aggregatedContent);
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference content) {
            return streamedRequest.createResponse(status, content);
        }

        @Override
        public HttpResponse createResponse(RestStatus status, ChunkedRestResponseBodyPart firstBodyPart) {
            return streamedRequest.createResponse(status, firstBodyPart);
        }

        @Override
        public Exception getInboundException() {
            return streamedRequest.getInboundException();
        }

        @Override
        public void release() {
            if (released.compareAndSet(false, true)) {
                // request is not ref counted, but content is
                aggregatedContent.close();
            }
        }

        @Override
        public RestRequest.Method method() {
            return streamedRequest.method();
        }

        @Override
        public String uri() {
            return streamedRequest.uri();
        }

        @Override
        public Map<String, List<String>> getHeaders() {
            return streamedRequest.getHeaders();
        }
    }

    static final class AggregatedRestRequestChannel extends DelegatingRestChannel {
        private final RestRequest request;

        AggregatedRestRequestChannel(RestChannel delegate, RestRequest request) {
            super(delegate);
            this.request = request;
        }

        @Override
        public RestRequest request() {
            return request;
        }

        @Override
        public void sendResponse(RestResponse response) {
            request.getHttpRequest().release(); // see DefaultRestChannel
            super.sendResponse(response);
        }
    }

    /**
     * A special case aggregator that expects no content.
     * We still wait for the last empty content to proceed with streamedRequest handling.
     */
    static class NoContent implements ChunkHandler {
        private final RestRequest request;
        private final RestChannel channel;
        private final AggregateConsumer result;

        NoContent(RestRequest request, RestChannel channel, AggregateConsumer result) {
            this.request = request;
            this.channel = channel;
            this.result = result;
        }

        @Override
        public void onNext(final ReleasableBytesReference lastEmptyChunk, boolean isLast) throws Exception {
            assert lastEmptyChunk.length() == 0 && isLast;
            var aggReq = new RestRequest(request, new AggregatedHttpRequest(request.getHttpRequest(), HttpBody.empty()));
            var aggChan = new AggregatedRestRequestChannel(channel, aggReq);
            result.accept(aggReq, aggChan);
        }
    }

    static class ChunkAggregator implements ChunkHandler {

        private final RestRequest request;
        private final RestChannel channel;
        private final HttpBody.Stream stream;
        private final AggregateConsumer result;
        private List<ReleasableBytesReference> aggregate;

        ChunkAggregator(RestRequest request, RestChannel channel, AggregateConsumer result) {
            this.request = request;
            this.channel = channel;
            this.stream = request.contentStream();
            this.result = result;
            this.aggregate = new ArrayList<>();
        }

        /**
         * Compose and wrap all chunks into new {@link RestRequest} with full content.
         */
        static RestRequest composeRestRequest(RestRequest streamedRestRequest, List<ReleasableBytesReference> chunks) {
            final var composite = CompositeBytesReference.of(chunks.toArray(new BytesReference[0]));
            final var refCnt = new AbstractRefCounted() {

                @Override
                protected void closeInternal() {
                    Releasables.close(chunks);
                }
            };
            var aggregatedHttpRequest = new AggregatedHttpRequest(
                streamedRestRequest.getHttpRequest(),
                HttpBody.fromReleasableBytesReference(new ReleasableBytesReference(composite, refCnt))
            );
            return new RestRequest(streamedRestRequest, aggregatedHttpRequest);
        }

        @Override
        public void onNext(ReleasableBytesReference chunk, boolean isLast) throws Exception {
            aggregate.add(chunk);
            if (isLast == false) {
                stream.next();
            } else {
                var aggReq = composeRestRequest(request, aggregate);
                var aggChan = new AggregatedRestRequestChannel(channel, aggReq);
                aggregate = List.of();
                result.accept(aggReq, aggChan);
            }
        }

        @Override
        public void close() {
            Releasables.close(aggregate);
            aggregate = List.of();
        }

    }

}
