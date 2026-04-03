/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.IndexingPressure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

/**
 * Accumulates a streamed HTTP request body while tracking memory usage via {@link IndexingPressure}.
 * <p>
 * This is intended for indexing-related REST endpoints that receive opaque request bodies (e.g. protobuf)
 * which must be fully accumulated before processing. It provides backpressure by reserving memory
 * up front and rejecting oversized requests with a 413 status.
 * <p>
 * When {@link #accept(RestChannel)} is called, the aggregator reserves memory via
 * {@link IndexingPressure#markCoordinatingOperationStarted}. If the reservation fails
 * (e.g. {@code EsRejectedExecutionException} under heavy load), the {@link CompletionHandler#onFailure}
 * callback is invoked so the caller can produce a format-appropriate error response (e.g. protobuf).
 * <p>
 * Once all chunks are accumulated, the reservation is lowered to the actual size and the
 * {@link CompletionHandler} is invoked with the aggregated content and the pressure reservation
 * (as a {@link Releasable}) for the caller to release when appropriate.
 */
public class IndexingPressureAwareContentAggregator implements BaseRestHandler.RequestBodyChunkConsumer {

    /**
     * Transforms the accumulated request body before it is handed to the {@link CompletionHandler}.
     * Implementations must release the input reference when they produce new output.
     */
    @FunctionalInterface
    public interface BodyPostProcessor {

        BodyPostProcessor NOOP = (body, size) -> body;

        /**
         * Post-processes the accumulated request body (e.g. decompression).
         *
         * @param body The accumulated raw body to process.
         *             Unless the post-processor returns the same reference, it is responsible for closing it.
         *             The caller must not use this reference after this method returns.
         * @param maxSize The maximum permitted size for the result.
         * @return The post-processed body. Must not exceed {@code maxSize}. The caller is responsible for closing the returned reference.
         * @throws IOException on processing failure
         */
        ReleasableBytesReference process(ReleasableBytesReference body, long maxSize) throws IOException;
    }

    /**
     * Callback for request body accumulation lifecycle events.
     */
    public interface CompletionHandler {
        /**
         * Called when the full request body has been successfully accumulated.
         *
         * @param channel the REST channel for sending the response
         * @param content the aggregated request body
         * @param indexingPressureRelease releases the indexing pressure reservation when closed
         */
        void onComplete(RestChannel channel, ReleasableBytesReference content, Releasable indexingPressureRelease);

        /**
         * Called when a failure occurs during content accumulation, such as the request body
         * exceeding the maximum allowed size or the indexing pressure reservation being rejected.
         *
         * @param channel the REST channel for sending the error response
         * @param e the exception describing the failure
         */
        void onFailure(RestChannel channel, Exception e);
    }

    private final RestRequest request;
    private final IndexingPressure indexingPressure;
    private final long maxRequestSize;
    private final CompletionHandler completionHandler;
    private final BodyPostProcessor bodyPostProcessor;

    private IndexingPressure.Coordinating coordinating;
    private ArrayList<ReleasableBytesReference> chunks;
    private long accumulatedSize;
    private boolean closed;

    public IndexingPressureAwareContentAggregator(
        RestRequest request,
        IndexingPressure indexingPressure,
        long maxRequestSize,
        CompletionHandler completionHandler,
        BodyPostProcessor bodyPostProcessor
    ) {
        this.request = request;
        this.indexingPressure = indexingPressure;
        this.maxRequestSize = maxRequestSize;
        this.completionHandler = completionHandler;
        this.bodyPostProcessor = Objects.requireNonNull(bodyPostProcessor);
    }

    @Override
    public void accept(RestChannel channel) {
        try {
            coordinating = indexingPressure.markCoordinatingOperationStarted(1, maxRequestSize, false);
        } catch (Exception e) {
            closed = true;
            completionHandler.onFailure(channel, e);
            return;
        }
        request.contentStream().next();
    }

    @Override
    public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
        if (closed) {
            chunk.close();
            return;
        }

        accumulatedSize += chunk.length();
        if (failIfAboveLimit(channel, chunk)) {
            return;
        }

        if (isLast == false) {
            if (chunks == null) {
                chunks = new ArrayList<>();
            }
            chunks.add(chunk);
            request.contentStream().next();
        } else {
            ReleasableBytesReference fullBody;
            if (chunks == null) {
                fullBody = chunk;
            } else {
                chunks.add(chunk);
                var composite = CompositeBytesReference.of(chunks.toArray(new ReleasableBytesReference[0]));
                fullBody = new ReleasableBytesReference(composite, Releasables.wrap(chunks));
            }
            chunks = null;

            try {
                fullBody = bodyPostProcessor.process(fullBody, maxRequestSize);
            } catch (Exception e) {
                closeOnFailure(channel, e, fullBody);
                return;
            }
            accumulatedSize = fullBody.length();
            if (failIfAboveLimit(channel, fullBody)) {
                return;
            }

            long excess = maxRequestSize - accumulatedSize;
            if (excess > 0) {
                coordinating.reduceBytes(excess);
            }
            closed = true;
            completionHandler.onComplete(channel, fullBody, coordinating);
        }
    }

    /**
     * @return {@code true} if the limit was exceeded and failure handling was performed, otherwise {@code false}.
     */
    private boolean failIfAboveLimit(RestChannel channel, Releasable releasable) {
        if (accumulatedSize > maxRequestSize) {
            closeOnFailure(
                channel,
                new ElasticsearchStatusException(
                    "request body too large, max [" + maxRequestSize + "] bytes",
                    RestStatus.REQUEST_ENTITY_TOO_LARGE
                ),
                releasable
            );
            return true;
        }
        return false;
    }

    private void closeOnFailure(RestChannel channel, Exception e, Releasable releasable) {
        releasable.close();
        if (chunks != null) {
            Releasables.close(chunks);
            chunks = null;
        }
        closed = true;
        coordinating.close();
        completionHandler.onFailure(channel, e);
    }

    @Override
    public void streamClose() {
        if (closed == false) {
            closed = true;
            if (chunks != null) {
                Releasables.close(chunks);
                chunks = null;
            }
            coordinating.close();
        }
    }
}
