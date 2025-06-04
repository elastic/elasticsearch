/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslCloseCompletionEvent;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.PromiseCombiner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.network.ThreadWatchdog;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.transport.netty4.Netty4WriteThrottlingHandler;
import org.elasticsearch.transport.netty4.NettyAllocator;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 * This handler also throttles write operations and will not pass any writes to the next handler so long as the channel is not writable.
 */
public class Netty4HttpPipeliningHandler extends ChannelDuplexHandler {

    private static final Logger logger = LogManager.getLogger(Netty4HttpPipeliningHandler.class);

    private final int maxEventsHeld;
    private final ThreadWatchdog.ActivityTracker activityTracker;
    private final PriorityQueue<Tuple<? extends Netty4HttpResponse, ChannelPromise>> outboundHoldingQueue;

    private record ChunkedWrite(PromiseCombiner combiner, ChannelPromise onDone, ChunkedRestResponseBodyPart responseBodyPart) {}

    /**
     * The current {@link ChunkedWrite} if a chunked write is executed at the moment.
     */
    @Nullable
    private ChunkedWrite currentChunkedWrite;

    /**
     * HTTP request content stream for current request, it's null if there is no current request or request is fully-aggregated
     */
    @Nullable
    private Netty4HttpRequestBodyStream currentRequestStream;

    /*
     * The current read and write sequence numbers. Read sequence numbers are attached to requests in the order they are read from the
     * channel, and then transferred to responses. A response is not written to the channel context until its sequence number matches the
     * current write sequence, implying that all preceding messages have been written.
     */
    private int readSequence;
    private int writeSequence;

    /**
     * Queue of pending writes that are flushed as the channel becomes writable. Queuing operations here instead of passing them to
     * {@link ChannelHandlerContext#write} straight away prevents us from allocating buffers for operations that can not be written
     * to the channel at the moment needlessly in case compression is used which creates buffers containing the compressed content
     * in {@link io.netty.handler.codec.http.HttpContentCompressor#write}.
     */
    private final Queue<WriteOperation> queuedWrites = new ArrayDeque<>();

    private final Netty4HttpServerTransport serverTransport;

    /**
     * Construct a new pipelining handler; this handler should be used downstream of HTTP decoding/aggregation.
     *
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel connection; this is
     *                      required as events cannot queue up indefinitely
     */
    public Netty4HttpPipeliningHandler(
        final int maxEventsHeld,
        final Netty4HttpServerTransport serverTransport,
        final ThreadWatchdog.ActivityTracker activityTracker
    ) {
        this.maxEventsHeld = maxEventsHeld;
        this.activityTracker = activityTracker;
        this.outboundHoldingQueue = new PriorityQueue<>(1, Comparator.comparingInt(t -> t.v1().getSequence()));
        this.serverTransport = serverTransport;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        activityTracker.startActivity();
        boolean shouldRead = true;
        try {
            if (msg instanceof HttpRequest request) {
                final Netty4HttpRequest netty4HttpRequest;
                if (request.decoderResult().isFailure()) {
                    final Throwable cause = request.decoderResult().cause();
                    final Exception nonError;
                    if (cause instanceof Error) {
                        ExceptionsHelper.maybeDieOnAnotherThread(cause);
                        nonError = new Exception(cause);
                    } else {
                        nonError = (Exception) cause;
                    }
                    netty4HttpRequest = new Netty4HttpRequest(readSequence++, (FullHttpRequest) request, nonError);
                } else {
                    assert currentRequestStream == null : "current stream must be null for new request";
                    if (request instanceof FullHttpRequest fullHttpRequest) {
                        netty4HttpRequest = new Netty4HttpRequest(readSequence++, fullHttpRequest);
                        currentRequestStream = null;
                    } else {
                        var contentStream = new Netty4HttpRequestBodyStream(ctx, serverTransport.getThreadPool().getThreadContext());
                        currentRequestStream = contentStream;
                        netty4HttpRequest = new Netty4HttpRequest(readSequence++, request, contentStream);
                        shouldRead = false;
                    }
                }
                handlePipelinedRequest(ctx, netty4HttpRequest);
            } else {
                assert msg instanceof HttpContent : "expect HttpContent got " + msg;
                assert currentRequestStream != null : "current stream must exists before handling http content";
                shouldRead = false;
                currentRequestStream.handleNettyContent((HttpContent) msg);
                if (msg instanceof LastHttpContent) {
                    currentRequestStream = null;
                }
            }
        } finally {
            if (shouldRead) {
                ctx.channel().eventLoop().execute(ctx::read);
            }
            activityTracker.stopActivity();
        }
    }

    // protected so tests can override it
    protected void handlePipelinedRequest(ChannelHandlerContext ctx, Netty4HttpRequest pipelinedRequest) {
        final Netty4HttpChannel channel = ctx.channel().attr(Netty4HttpServerTransport.HTTP_CHANNEL_KEY).get();
        boolean success = false;
        assert Transports.assertDefaultThreadContext(serverTransport.getThreadPool().getThreadContext());
        assert Transports.assertTransportThread();
        try {
            serverTransport.incomingRequest(pipelinedRequest, channel);
            success = true;
        } finally {
            if (success == false) {
                pipelinedRequest.release();
            }
        }
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
        assert msg instanceof Netty4HttpResponse : "Invalid message type: " + msg.getClass();
        final Netty4HttpResponse restResponse = (Netty4HttpResponse) msg;
        if (restResponse.getSequence() != writeSequence) {
            // response is not at the current sequence number so we add it to the outbound queue
            enqueuePipelinedResponse(ctx, restResponse, promise);
        } else {
            // response is at the current sequence number and does not need to wait for any other response to be written
            doWrite(ctx, restResponse, promise);
            // see if we have any queued up responses that became writeable due to the above write
            doWriteQueued(ctx);
        }
    }

    private void enqueuePipelinedResponse(ChannelHandlerContext ctx, Netty4HttpResponse restResponse, ChannelPromise promise) {
        assert restResponse instanceof Netty4ChunkedHttpContinuation == false
            : "received out-of-order continuation at [" + restResponse.getSequence() + "], expecting [" + writeSequence + "]";
        assert restResponse.getSequence() > writeSequence
            : "response sequence [" + restResponse.getSequence() + "] we below write sequence [" + writeSequence + "]";
        if (outboundHoldingQueue.size() >= maxEventsHeld) {
            ctx.channel().close();
            promise.tryFailure(new ClosedChannelException());
        } else {
            assert outboundHoldingQueue.stream().noneMatch(t -> t.v1().getSequence() == restResponse.getSequence())
                : "duplicate outbound entries for seqno " + restResponse.getSequence();
            outboundHoldingQueue.add(new Tuple<>(restResponse, promise));
        }
    }

    private void doWriteQueued(ChannelHandlerContext ctx) {
        while (outboundHoldingQueue.isEmpty() == false && outboundHoldingQueue.peek().v1().getSequence() == writeSequence) {
            final Tuple<? extends Netty4HttpResponse, ChannelPromise> top = outboundHoldingQueue.poll();
            assert top != null : "we know the outbound holding queue to not be empty at this point";
            doWrite(ctx, top.v1(), top.v2());
        }
    }

    private static final String DO_NOT_SPLIT = "es.unsafe.do_not_split_http_responses";

    private static final boolean DO_NOT_SPLIT_HTTP_RESPONSES;
    private static final int SPLIT_THRESHOLD;

    static {
        DO_NOT_SPLIT_HTTP_RESPONSES = Booleans.parseBoolean(System.getProperty(DO_NOT_SPLIT), false);
        // Netty will add some header bytes if it compresses this message. So we downsize slightly.
        SPLIT_THRESHOLD = (int) (NettyAllocator.suggestedMaxAllocationSize() * 0.99);
    }

    private void doWrite(ChannelHandlerContext ctx, Netty4HttpResponse readyResponse, ChannelPromise promise) {
        assert currentChunkedWrite == null : "unexpected existing write [" + currentChunkedWrite + "]";
        assert readyResponse != null : "cannot write null response";
        assert readyResponse.getSequence() == writeSequence;
        if (readyResponse instanceof Netty4FullHttpResponse fullResponse) {
            doWriteFullResponse(ctx, fullResponse, promise);
        } else if (readyResponse instanceof Netty4ChunkedHttpResponse chunkedResponse) {
            doWriteChunkedResponse(ctx, chunkedResponse, promise);
        } else if (readyResponse instanceof Netty4ChunkedHttpContinuation chunkedContinuation) {
            doWriteChunkedContinuation(ctx, chunkedContinuation, promise);
        } else {
            assert false : readyResponse.getClass().getCanonicalName();
            throw new IllegalStateException("illegal message type: " + readyResponse.getClass().getCanonicalName());
        }
    }

    /**
     * Split up large responses to prevent batch compression {@link JdkZlibEncoder} down the pipeline.
     */
    private void doWriteFullResponse(ChannelHandlerContext ctx, Netty4FullHttpResponse readyResponse, ChannelPromise promise) {
        if (DO_NOT_SPLIT_HTTP_RESPONSES || readyResponse.content().readableBytes() <= SPLIT_THRESHOLD) {
            enqueueWrite(ctx, readyResponse, promise);
        } else {
            splitAndWrite(ctx, readyResponse, promise);
        }
        writeSequence++;
    }

    private void doWriteChunkedResponse(ChannelHandlerContext ctx, Netty4ChunkedHttpResponse readyResponse, ChannelPromise promise) {
        final PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
        final ChannelPromise first = ctx.newPromise();
        combiner.add((Future<Void>) first);
        final var firstBodyPart = readyResponse.firstBodyPart();
        assert currentChunkedWrite == null;
        currentChunkedWrite = new ChunkedWrite(combiner, promise, firstBodyPart);
        if (enqueueWrite(ctx, readyResponse, first)) {
            // We were able to write out the first chunk directly, try writing out subsequent chunks until the channel becomes unwritable.
            // NB "writable" means there's space in the downstream ChannelOutboundBuffer, we aren't trying to saturate the physical channel.
            while (ctx.channel().isWritable()) {
                if (writeChunk(ctx, currentChunkedWrite)) {
                    finishChunkedWrite();
                    return;
                }
            }
        }
    }

    private void doWriteChunkedContinuation(ChannelHandlerContext ctx, Netty4ChunkedHttpContinuation continuation, ChannelPromise promise) {
        final PromiseCombiner combiner = continuation.combiner();
        assert currentChunkedWrite == null;
        final var bodyPart = continuation.bodyPart();
        assert bodyPart.isPartComplete() == false
            : "response with continuations must have at least one (possibly-empty) chunk in each part";
        currentChunkedWrite = new ChunkedWrite(combiner, promise, bodyPart);
        // NB "writable" means there's space in the downstream ChannelOutboundBuffer, we aren't trying to saturate the physical channel.
        while (ctx.channel().isWritable()) {
            if (writeChunk(ctx, currentChunkedWrite)) {
                finishChunkedWrite();
                return;
            }
        }
    }

    private void finishChunkedWrite() {
        if (currentChunkedWrite == null) {
            // failure during chunked response serialization, we're closing the channel
            return;
        }
        final var finishingWrite = currentChunkedWrite;
        currentChunkedWrite = null;
        final var finishingWriteBodyPart = finishingWrite.responseBodyPart();
        assert finishingWriteBodyPart.isPartComplete();
        final var endOfResponse = finishingWriteBodyPart.isLastPart();
        if (endOfResponse) {
            writeSequence++;
            finishingWrite.combiner().finish(finishingWrite.onDone());
        } else {
            final var threadContext = serverTransport.getThreadPool().getThreadContext();
            assert Transports.assertDefaultThreadContext(threadContext);
            final var channel = finishingWrite.onDone().channel();
            ActionListener.run(
                new ContextPreservingActionListener<>(
                    threadContext.newRestorableContext(false),
                    ActionListener.assertOnce(new ActionListener<>() {
                        @Override
                        public void onResponse(ChunkedRestResponseBodyPart continuation) {
                            // always fork a fresh task to avoid stack overflow
                            assert Transports.assertDefaultThreadContext(threadContext);
                            channel.eventLoop()
                                .execute(
                                    () -> channel.writeAndFlush(
                                        new Netty4ChunkedHttpContinuation(writeSequence, continuation, finishingWrite.combiner()),
                                        finishingWrite.onDone() // pass the terminal listener/promise along the line
                                    )
                                );
                            checkShutdown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assert Transports.assertDefaultThreadContext(threadContext);
                            logger.error(
                                Strings.format("failed to get continuation of HTTP response body for [%s], closing connection", channel),
                                e
                            );
                            Netty4Utils.addListener(channel.close(), f -> {
                                finishingWrite.combiner().add(f.channel().newFailedFuture(e));
                                finishingWrite.combiner().finish(finishingWrite.onDone());
                            });
                            checkShutdown();
                        }

                        private void checkShutdown() {
                            if (channel.eventLoop().isShuttingDown()) {
                                // The event loop is shutting down, and https://github.com/netty/netty/issues/8007 means that we cannot know
                                // if the preceding activity made it onto its queue before shutdown or whether it will just vanish without a
                                // trace, so to avoid a leak we must double-check that the final listener is completed once the event loop
                                // is terminated. Note that the final listener came from Netty4Utils#safeWriteAndFlush so its executor is an
                                // ImmediateEventExecutor which means this completion is not subject to the same issue, it still works even
                                // if the event loop has already terminated.
                                channel.eventLoop()
                                    .terminationFuture()
                                    .addListener(ignored -> finishingWrite.onDone().tryFailure(new ClosedChannelException()));
                            }
                        }

                    })
                ),
                finishingWriteBodyPart::getNextPart
            );
        }
    }

    private void splitAndWrite(ChannelHandlerContext ctx, Netty4FullHttpResponse msg, ChannelPromise promise) {
        final PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
        HttpResponse response = new DefaultHttpResponse(msg.protocolVersion(), msg.status(), msg.headers());
        combiner.add(enqueueWrite(ctx, response));
        ByteBuf content = msg.content();
        while (content.readableBytes() > SPLIT_THRESHOLD) {
            combiner.add(enqueueWrite(ctx, new DefaultHttpContent(content.readRetainedSlice(SPLIT_THRESHOLD))));
        }
        combiner.add(enqueueWrite(ctx, new DefaultLastHttpContent(content.readRetainedSlice(content.readableBytes()))));
        combiner.finish(promise);
    }

    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws IOException {
        if (ctx.channel().isWritable()) {
            doFlush(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws IOException {
        if (doFlush(ctx) == false) {
            ctx.flush();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        doFlush(ctx);
        super.channelInactive(ctx);
    }

    /**
     * @param ctx channel handler context
     *
     * @return true if a call to this method resulted in a call to {@link ChannelHandlerContext#flush()} on the given {@code ctx}
     */
    private boolean doFlush(ChannelHandlerContext ctx) throws IOException {
        assert ctx.executor().inEventLoop();
        final Channel channel = ctx.channel();
        if (channel.isActive() == false) {
            failQueuedWrites(ctx);
            return false;
        }
        while (channel.isWritable()) {
            // NB "writable" means there's space in the downstream ChannelOutboundBuffer, we aren't trying to saturate the physical channel.
            WriteOperation currentWrite = queuedWrites.poll();
            if (currentWrite == null) {
                doWriteQueued(ctx);
                if (channel.isWritable() == false) {
                    break;
                }
                currentWrite = queuedWrites.poll();
            }
            if (currentWrite == null) {
                // no bytes were found queued, check if a chunked message might have become writable
                if (currentChunkedWrite != null) {
                    if (writeChunk(ctx, currentChunkedWrite)) {
                        finishChunkedWrite();
                    }
                    continue;
                }
                break;
            }
            ctx.write(currentWrite.msg, currentWrite.promise);
        }
        ctx.flush();
        if (channel.isActive() == false) {
            failQueuedWrites(ctx);
        }
        return true;
    }

    private boolean writeChunk(ChannelHandlerContext ctx, ChunkedWrite chunkedWrite) {
        final var bodyPart = chunkedWrite.responseBodyPart();
        final var combiner = chunkedWrite.combiner();
        assert bodyPart.isPartComplete() == false : "should not continue to try and serialize once done";
        final ReleasableBytesReference bytes;
        try {
            bytes = bodyPart.encodeChunk(Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE, serverTransport.recycler());
        } catch (Exception e) {
            return handleChunkingFailure(ctx, chunkedWrite, e);
        }
        final ByteBuf content = Netty4Utils.toByteBuf(bytes);
        final boolean isPartComplete = bodyPart.isPartComplete();
        final boolean isBodyComplete = isPartComplete && bodyPart.isLastPart();
        final ChannelFuture f = ctx.write(isBodyComplete ? new DefaultLastHttpContent(content) : new DefaultHttpContent(content));
        Netty4Utils.addListener(f, ignored -> bytes.close());
        combiner.add(f);
        return isPartComplete;
    }

    private boolean handleChunkingFailure(ChannelHandlerContext ctx, ChunkedWrite chunkedWrite, Exception e) {
        logger.error(Strings.format("caught exception while encoding response chunk, closing connection %s", ctx.channel()), e);
        assert currentChunkedWrite == chunkedWrite;
        currentChunkedWrite = null;
        chunkedWrite.combiner().add(ctx.channel().close());
        chunkedWrite.combiner().add(ctx.newFailedFuture(e));
        chunkedWrite.combiner().finish(chunkedWrite.onDone());
        return true;
    }

    private void failQueuedWrites(ChannelHandlerContext ctx) {
        WriteOperation queuedWrite;
        while ((queuedWrite = queuedWrites.poll()) != null) {
            queuedWrite.failAsClosedChannel();
        }
        if (currentChunkedWrite != null) {
            final var chunkedWrite = currentChunkedWrite;
            currentChunkedWrite = null;
            chunkedWrite.combiner().add(ctx.newFailedFuture(new ClosedChannelException()));
            chunkedWrite.combiner().finish(chunkedWrite.onDone());
        }
        Tuple<? extends Netty4HttpResponse, ChannelPromise> pipelinedWrite;
        while ((pipelinedWrite = outboundHoldingQueue.poll()) != null) {
            pipelinedWrite.v2().tryFailure(new ClosedChannelException());
        }
    }

    private Future<Void> enqueueWrite(ChannelHandlerContext ctx, HttpObject msg) {
        final ChannelPromise p = ctx.newPromise();
        enqueueWrite(ctx, msg, p);
        return p;
    }

    // returns true if the write was actually executed and false if it was just queued up
    private boolean enqueueWrite(ChannelHandlerContext ctx, HttpObject msg, ChannelPromise promise) {
        if (ctx.channel().isWritable() && queuedWrites.isEmpty()) {
            ctx.write(msg, promise);
            return true;
        } else {
            queuedWrites.add(new WriteOperation(msg, promise));
            return false;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        assert Transports.assertDefaultThreadContext(serverTransport.getThreadPool().getThreadContext());

        Netty4HttpChannel channel = ctx.channel().attr(Netty4HttpServerTransport.HTTP_CHANNEL_KEY).get();
        if (cause instanceof Error) {
            serverTransport.onException(channel, new Exception(cause));
        } else {
            serverTransport.onException(channel, (Exception) cause);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslCloseCompletionEvent closeEvent) {
            if (closeEvent.isSuccess() && ctx.channel().isActive()) {
                logger.trace("received TLS close_notify, closing connection {}", ctx.channel());
                ctx.channel().close();
            }
        }
    }

    private record WriteOperation(HttpObject msg, ChannelPromise promise) {

        void failAsClosedChannel() {
            promise.tryFailure(new ClosedChannelException());
            ReferenceCountUtil.release(msg);
        }
    }
}
