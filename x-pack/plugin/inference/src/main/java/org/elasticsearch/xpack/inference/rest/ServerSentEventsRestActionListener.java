/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.LazyInitializable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.Streams;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.XContentFormattedException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.inference.results.XContentFormattedException.X_CONTENT_PARAM;

/**
 * A version of {@link org.elasticsearch.rest.action.RestChunkedToXContentListener} that reads from a {@link Flow.Publisher} and encodes
 * the response in Server-Sent Events.
 */
public class ServerSentEventsRestActionListener implements ActionListener<InferenceAction.Response> {
    private static final Logger logger = LogManager.getLogger(ServerSentEventsRestActionListener.class);
    private final StreamingSubscriber subscriber = new StreamingSubscriber();
    private final AtomicBoolean isLastPart = new AtomicBoolean(false);
    private final RestChannel channel;
    private final ToXContent.Params params;
    private final SetOnce<ThreadPool> threadPool;

    /**
     * A listener for the first part of the next entry to become available for transmission.
     * Chunks are sent one at a time through the completion of this listener.
     * This listener is initialized in {@link #initializeStream(InferenceAction.Response)} before the first chunk is requested.
     * When a chunk is ready, this listener is completed with the converted {@link ChunkedRestResponseBodyPart}.
     * After transmitting the chunk, {@link #requestNextChunk(ActionListener)} will set the next listener and request the next
     * chunk. This cycle will repeat until this listener completes with the DONE chunk and the stream closes.
     */
    private ActionListener<ChunkedRestResponseBodyPart> nextBodyPartListener;

    public ServerSentEventsRestActionListener(RestChannel channel, SetOnce<ThreadPool> threadPool) {
        this(channel, channel.request(), threadPool);
    }

    public ServerSentEventsRestActionListener(RestChannel channel, ToXContent.Params params, SetOnce<ThreadPool> threadPool) {
        this.channel = channel;
        this.params = new ToXContent.DelegatingMapParams(Map.of(X_CONTENT_PARAM, String.valueOf(channel.detailedErrorsEnabled())), params);
        this.threadPool = Objects.requireNonNull(threadPool);
    }

    @Override
    public void onResponse(InferenceAction.Response response) {
        try {
            ensureOpen();
            if (response.isStreaming()) {
                initializeStream(response);
            } else {
                // if we aren't streaming - if the provider doesn't allow streaming, there will be a single message
                channel.sendResponse(
                    RestResponse.chunked(RestStatus.OK, new SingleServerSentEventBodyPart(ServerSentEvents.MESSAGE, response), () -> {})
                );
            }
        } catch (Exception e) {
            onFailure(e);
        }
    }

    protected void ensureOpen() {
        if (channel.request().getHttpChannel().isOpen() == false) {
            throw new TaskCancelledException("response channel [" + channel.request().getHttpChannel() + "] closed");
        }
    }

    private void initializeStream(InferenceAction.Response response) {
        ActionListener<ChunkedRestResponseBodyPart> chunkedResponseBodyActionListener = ActionListener.wrap(bodyPart -> {
            // this is the first response, so we need to send the RestResponse to open the stream
            // all subsequent bytes will be delivered through the nextBodyPartListener
            channel.sendResponse(RestResponse.chunked(RestStatus.OK, bodyPart, this::release));
        }, e -> {
            assert false : "body part listener's onFailure should never be called";
            // we shouldn't be here, but just in case we are we should close out the stream
            isLastPart.set(true);
            channel.sendResponse(
                RestResponse.chunked(
                    ExceptionsHelper.status(e),
                    new ServerSentEventResponseBodyPart(ServerSentEvents.ERROR, errorChunk(e)),
                    this::release
                )
            );
        });

        nextBodyPartListener = ContextPreservingActionListener.wrapPreservingContext(
            chunkedResponseBodyActionListener,
            threadPool.get().getThreadContext()
        );

        // subscribe will call onSubscribe, which requests the first chunk
        response.publisher().subscribe(subscriber);
    }

    private void release() {
        if (subscriber.subscription != null) {
            subscriber.subscription.cancel();
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            isLastPart.set(true);
            channel.sendResponse(
                RestResponse.chunked(
                    ExceptionsHelper.status(e),
                    new ServerSentEventResponseBodyPart(ServerSentEvents.ERROR, errorChunk(e)),
                    this::release
                )
            );
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error("failed to send failure response", inner);
        }
    }

    private ChunkedToXContent errorChunk(Throwable t) {
        // if we've already formatted it, just return that format
        if (ExceptionsHelper.unwrapCause(t) instanceof XContentFormattedException xContentFormattedException) {
            return xContentFormattedException;
        }

        // else, try to parse the format and return something that the ES client knows how to interpret
        var status = ExceptionsHelper.status(t);

        Exception e;
        if (t instanceof Exception) {
            e = (Exception) t;
        } else {
            // if not exception, then error, and we should not let it escape. rethrow on another thread, and inform the user we're stopping.
            ExceptionsHelper.maybeDieOnAnotherThread(t);
            e = new RuntimeException("Fatal error while streaming response. Please retry the request.");
            logger.error(e.getMessage(), t);
        }
        return params -> Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            Iterators.single((b, p) -> ElasticsearchException.generateFailureXContent(b, p, e, channel.detailedErrorsEnabled())),
            Iterators.single((b, p) -> b.field("status", status.getStatus())),
            ChunkedToXContentHelper.endObject()
        );
    }

    private void requestNextChunk(ActionListener<ChunkedRestResponseBodyPart> listener) {
        nextBodyPartListener = listener;
        subscriber.subscription.request(1);
    }

    /**
     * This subscriber connects the stream of ChunkedToXContent to the RestResponse's ChunkedRestResponseBodyPart.
     * The Subscription maintains the flow of chunks via {@link Flow.Subscription#request(long)}. The publisher is
     * responsible for not blocking the transport thread.
     * If the RestResponse is closed prematurely, we should call {@link Flow.Subscription#cancel()} to notify the
     * publisher to stop sending chunks and to clean up any resources.
     */
    private class StreamingSubscriber implements Flow.Subscriber<ChunkedToXContent> {
        private static final Logger logger = LogManager.getLogger(StreamingSubscriber.class);
        private Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            if (isLastPart.get() == false) {
                this.subscription = subscription;
                subscription.request(1);
            } else {
                subscription.cancel();
            }
        }

        @Override
        public void onNext(ChunkedToXContent item) {
            if (isLastPart.get() == false) {
                nextBodyPartListener().onResponse(new ServerSentEventResponseBodyPart(ServerSentEvents.MESSAGE, item));
            } else {
                subscription.cancel();
            }
        }

        @Override
        public void onError(Throwable throwable) {
            if (isLastPart.compareAndSet(false, true)) {
                logger.warn("A failure occurred in ElasticSearch while streaming the response.", throwable);
                nextBodyPartListener().onResponse(new ServerSentEventResponseBodyPart(ServerSentEvents.ERROR, errorChunk(throwable)));
            }
        }

        @Override
        public void onComplete() {
            if (isLastPart.compareAndSet(false, true)) {
                nextBodyPartListener().onResponse(new ServerSentEventDoneBodyPart());
            }
        }

        private ActionListener<ChunkedRestResponseBodyPart> nextBodyPartListener() {
            assert nextBodyPartListener != null : "Subscriber should only be called when Subscription#request is called.";
            var nextListener = nextBodyPartListener;
            nextBodyPartListener = null;
            return nextListener;
        }
    }

    // from: https://html.spec.whatwg.org/multipage/server-sent-events.html#server-sent-events
    private static class ServerSentEventSpec {
        private static final String MIME_TYPE = "text/event-stream";
        private static final byte[] BOM = "\uFEFF".getBytes(StandardCharsets.UTF_8);
        private static final byte[] DATA = "data: ".getBytes(StandardCharsets.UTF_8);
        private static final byte[] EOL = "\n".getBytes(StandardCharsets.UTF_8);
    }

    // from: https://html.spec.whatwg.org/multipage/server-sent-events.html#server-sent-events
    // we can send "event: " at the start of the message to communicate metadata about what is in "data: "
    // this should let us send mid-stream errors, though we preferably never have them
    private enum ServerSentEvents {
        ERROR("error"),
        MESSAGE("message");

        private final byte[] eventType;

        ServerSentEvents(String eventType) {
            this.eventType = ("event: " + eventType).getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * Copied from {@link ChunkedRestResponseBodyPart#fromXContent(ChunkedToXContent, ToXContent.Params, RestChannel)},
     * notable changes:
     * 1. check if this is the last part, set from the Subscriber's onComplete or onError calls.
     * 2. write the SSE-specific bytes to the output stream before/after the XContent bytes
     * 3. if this is not the last chunk, issue a request for a next chunk
     */
    private class ServerSentEventResponseBodyPart implements ChunkedRestResponseBodyPart {
        private static final Logger logger = LogManager.getLogger(ServerSentEventResponseBodyPart.class);
        private final OutputStream out = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                target.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                target.write(b, off, len);
            }
        };

        private final ServerSentEvents event;
        private final Iterator<? extends ToXContent> serialization;
        private final LazyInitializable<XContentBuilder, IOException> xContentBuilder;
        private final AtomicBoolean isStartOfData = new AtomicBoolean(true);

        private BytesStream target;

        private ServerSentEventResponseBodyPart(ServerSentEvents event, ChunkedToXContent item) {
            this.event = event;
            this.xContentBuilder = new LazyInitializable<>(
                () -> channel.newBuilder(channel.request().getXContentType(), null, true, Streams.noCloseStream(out))
            );
            this.serialization = item.toXContentChunked(channel.request().getRestApiVersion(), params);
        }

        @Override
        public boolean isPartComplete() {
            return serialization.hasNext() == false;
        }

        @Override
        public boolean isLastPart() {
            return isLastPart.get();
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            if (isLastPart()) {
                // this should not have been called
                assert false : "no continuations";
                listener.onFailure(new IllegalStateException("no continuations available"));
            } else {
                requestNextChunk(listener);
            }
        }

        /**
         * after we are finished with this chunk, the entire payload will look like:
         * [bom]event: [event type]\n
         * data: [chunkedXContentJson]\n
         * \n
         */
        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            try {
                final var builder = xContentBuilder.getOrCompute();
                final var chunkStream = new RecyclerBytesStreamOutput(recycler);
                assert target == null;
                target = chunkStream;

                // if this is the first time we are encoding this chunk, write the SSE leading bytes
                if (isStartOfData.compareAndSet(true, false)) {
                    target.write(ServerSentEventSpec.BOM);
                    target.write(event.eventType);
                    target.write(ServerSentEventSpec.EOL);
                    target.write(ServerSentEventSpec.DATA);
                }

                // start or continue writing this chunk
                while (serialization.hasNext()) {
                    serialization.next().toXContent(builder, params);
                    if (chunkStream.size() >= sizeHint) {
                        break;
                    }
                }

                if (serialization.hasNext() == false) {
                    // SSE wants two newlines between messages
                    builder.close();
                    target.write(ServerSentEventSpec.EOL);
                    target.write(ServerSentEventSpec.EOL);
                    target.flush();
                }
                final var result = new ReleasableBytesReference(chunkStream.bytes(), () -> Releasables.closeExpectNoException(chunkStream));
                target = null;
                return result;
            } catch (Exception e) {
                logger.error("failure encoding chunk", e);
                throw e;
            } finally {
                if (target != null) {
                    assert false : "failure encoding chunk";
                    IOUtils.closeWhileHandlingException(target);
                    target = null;
                }
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return ServerSentEventSpec.MIME_TYPE;
        }
    }

    /**
     * <p>Special ChunkedRestResponseBodyPart that writes the done message, formatted as:
     * ```
     * event: message
     * data: [DONE]
     * ```
     * </p>
     * <p>This will be the last message, sent when {@link Flow.Subscriber#onComplete()} is called.</p>
     */
    private static class ServerSentEventDoneBodyPart implements ChunkedRestResponseBodyPart {
        private static final Logger logger = LogManager.getLogger(ServerSentEventDoneBodyPart.class);
        private static final byte[] DONE_BYTES = "[DONE]".getBytes(StandardCharsets.UTF_8);
        private volatile boolean isPartComplete = false;

        @Override
        public boolean isPartComplete() {
            return isPartComplete;
        }

        @Override
        public boolean isLastPart() {
            return true;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            assert false : "no continuations";
            listener.onFailure(new IllegalStateException("no continuations available"));
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final var chunkStream = new RecyclerBytesStreamOutput(recycler);
            try {
                chunkStream.write(ServerSentEventSpec.BOM);
                chunkStream.write(ServerSentEvents.MESSAGE.eventType);
                chunkStream.write(ServerSentEventSpec.EOL);
                chunkStream.write(ServerSentEventSpec.DATA);

                chunkStream.write(DONE_BYTES);

                chunkStream.write(ServerSentEventSpec.EOL);
                chunkStream.write(ServerSentEventSpec.EOL);
                chunkStream.flush();
                isPartComplete = true;
                return new ReleasableBytesReference(chunkStream.bytes(), () -> Releasables.closeExpectNoException(chunkStream));
            } catch (Exception e) {
                logger.error("failure encoding chunk", e);
                throw e;
            } finally {
                if (isPartComplete == false) {
                    assert false : "failure encoding chunk";
                    IOUtils.closeWhileHandlingException(chunkStream);
                }
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return ServerSentEventSpec.MIME_TYPE;
        }
    }

    /**
     * Special ChunkedRestResponseBodyPart that writes a single InferenceAction.Response followed by a DONE chunk.
     * This covers when a provider does not have streaming support but the request asked for it.
     */
    private class SingleServerSentEventBodyPart extends ServerSentEventResponseBodyPart {
        private SingleServerSentEventBodyPart(ServerSentEvents event, InferenceAction.Response item) {
            super(event, item);
        }

        @Override
        public boolean isLastPart() {
            return false;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            listener.onResponse(new ServerSentEventDoneBodyPart());
        }
    }
}
