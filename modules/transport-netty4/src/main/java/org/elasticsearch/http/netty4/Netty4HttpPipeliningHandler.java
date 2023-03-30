/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.PromiseCombiner;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.transport.netty4.Netty4WriteThrottlingHandler;
import org.elasticsearch.transport.netty4.NettyAllocator;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 * This handler also throttles write operations and will not pass any writes to the next handler so long as the channel is not writable.
 */
public class Netty4HttpPipeliningHandler extends ChannelDuplexHandler {

    private final Logger logger;

    private final int maxEventsHeld;
    private final PriorityQueue<Tuple<? extends Netty4RestResponse, ChannelPromise>> outboundHoldingQueue;

    private record ChunkedWrite(PromiseCombiner combiner, ChannelPromise onDone, Netty4ChunkedHttpResponse response) {}

    /**
     * The current {@link ChunkedWrite} if a chunked write is executed at the moment.
     */
    @Nullable
    private ChunkedWrite currentChunkedWrite;

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
     * @param logger        for logging unexpected errors
     * @param maxEventsHeld the maximum number of channel events that will be retained prior to aborting the channel connection; this is
     *                      required as events cannot queue up indefinitely
     */
    public Netty4HttpPipeliningHandler(Logger logger, final int maxEventsHeld, final Netty4HttpServerTransport serverTransport) {
        this.logger = logger;
        this.maxEventsHeld = maxEventsHeld;
        this.outboundHoldingQueue = new PriorityQueue<>(1, Comparator.comparingInt(t -> t.v1().getSequence()));
        this.serverTransport = serverTransport;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
        assert msg instanceof FullHttpRequest : "Should have fully aggregated message already but saw [" + msg + "]";
        final FullHttpRequest fullHttpRequest = (FullHttpRequest) msg;
        final Netty4HttpRequest netty4HttpRequest;
        if (fullHttpRequest.decoderResult().isFailure()) {
            final Throwable cause = fullHttpRequest.decoderResult().cause();
            final Exception nonError;
            if (cause instanceof Error) {
                ExceptionsHelper.maybeDieOnAnotherThread(cause);
                nonError = new Exception(cause);
            } else {
                nonError = (Exception) cause;
            }
            netty4HttpRequest = new Netty4HttpRequest(readSequence++, fullHttpRequest, nonError);
        } else {
            netty4HttpRequest = new Netty4HttpRequest(readSequence++, fullHttpRequest);
        }
        handlePipelinedRequest(ctx, netty4HttpRequest);
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
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws IOException {
        assert msg instanceof Netty4RestResponse : "Invalid message type: " + msg.getClass();
        boolean success = false;
        try {
            final Netty4RestResponse restResponse = (Netty4RestResponse) msg;
            if (restResponse.getSequence() != writeSequence) {
                assert restResponse.getSequence() > writeSequence
                    : "response sequence [" + restResponse.getSequence() + "] we below write sequence [" + writeSequence + "]";
                if (outboundHoldingQueue.size() >= maxEventsHeld) {
                    int eventCount = outboundHoldingQueue.size() + 1;
                    throw new IllegalStateException(
                        "Too many pipelined events [" + eventCount + "]. Max events allowed [" + maxEventsHeld + "]."
                    );
                }
                // response is not at the current sequence number so we add it to the outbound queue and return
                outboundHoldingQueue.add(new Tuple<>(restResponse, promise));
                success = true;
                return;
            }

            // response is at the current sequence number and does not need to wait for any other response to be written so we write
            // it out directly
            doWrite(ctx, restResponse, promise);
            success = true;
            // see if we have any queued up responses that became writeable due to the above write
            doWriteQueued(ctx);
        } catch (IllegalStateException e) {
            ctx.channel().close();
        } finally {
            if (success == false) {
                promise.setFailure(new ClosedChannelException());
            }
        }
    }

    private void doWriteQueued(ChannelHandlerContext ctx) throws IOException {
        while (outboundHoldingQueue.isEmpty() == false && outboundHoldingQueue.peek().v1().getSequence() == writeSequence) {
            final Tuple<? extends Netty4RestResponse, ChannelPromise> top = outboundHoldingQueue.poll();
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

    private void doWrite(ChannelHandlerContext ctx, Netty4RestResponse readyResponse, ChannelPromise promise) throws IOException {
        assert currentChunkedWrite == null : "unexpected existing write [" + currentChunkedWrite + "]";
        if (readyResponse instanceof Netty4HttpResponse) {
            doWrite(ctx, (Netty4HttpResponse) readyResponse, promise);
        } else {
            doWrite(ctx, (Netty4ChunkedHttpResponse) readyResponse, promise);
        }
    }

    /**
     * Split up large responses to prevent batch compression {@link JdkZlibEncoder} down the pipeline.
     */
    private void doWrite(ChannelHandlerContext ctx, Netty4HttpResponse readyResponse, ChannelPromise promise) {
        if (DO_NOT_SPLIT_HTTP_RESPONSES || readyResponse.content().readableBytes() <= SPLIT_THRESHOLD) {
            enqueueWrite(ctx, readyResponse, promise);
        } else {
            splitAndWrite(ctx, readyResponse, promise);
        }
        writeSequence++;
    }

    private void doWrite(ChannelHandlerContext ctx, Netty4ChunkedHttpResponse readyResponse, ChannelPromise promise) throws IOException {
        final PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
        final ChannelPromise first = ctx.newPromise();
        combiner.add((Future<Void>) first);
        currentChunkedWrite = new ChunkedWrite(combiner, promise, readyResponse);
        if (enqueueWrite(ctx, readyResponse, first)) {
            // We were able to write out the first chunk directly, try writing out subsequent chunks until the channel becomes unwritable.
            // NB "writable" means there's space in the downstream ChannelOutboundBuffer, we aren't trying to saturate the physical channel.
            while (ctx.channel().isWritable()) {
                if (writeChunk(ctx, combiner, readyResponse.body())) {
                    finishChunkedWrite();
                    return;
                }
            }
        }
    }

    private void finishChunkedWrite() {
        try {
            currentChunkedWrite.combiner.finish(currentChunkedWrite.onDone);
        } finally {
            currentChunkedWrite = null;
            writeSequence++;
        }
    }

    private void splitAndWrite(ChannelHandlerContext ctx, Netty4HttpResponse msg, ChannelPromise promise) {
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
            failQueuedWrites();
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
                    if (writeChunk(ctx, currentChunkedWrite.combiner, currentChunkedWrite.response.body())) {
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
            failQueuedWrites();
        }
        return true;
    }

    private boolean writeChunk(ChannelHandlerContext ctx, PromiseCombiner combiner, ChunkedRestResponseBody body) throws IOException {
        assert body.isDone() == false : "should not continue to try and serialize once done";
        final ReleasableBytesReference bytes = body.encodeChunk(
            Netty4WriteThrottlingHandler.MAX_BYTES_PER_WRITE,
            serverTransport.recycler()
        );
        assert bytes.length() > 0 : "serialization should not produce empty buffers";
        final ByteBuf content = Netty4Utils.toByteBuf(bytes);
        final boolean done = body.isDone();
        final ChannelFuture f = ctx.write(done ? new DefaultLastHttpContent(content) : new DefaultHttpContent(content));
        f.addListener(ignored -> bytes.close());
        combiner.add(f);
        return done;
    }

    private void failQueuedWrites() {
        WriteOperation queuedWrite;
        while ((queuedWrite = queuedWrites.poll()) != null) {
            queuedWrite.failAsClosedChannel();
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        if (currentChunkedWrite != null) {
            safeFailPromise(currentChunkedWrite.onDone, new ClosedChannelException());
            currentChunkedWrite = null;
        }
        List<Tuple<? extends Netty4RestResponse, ChannelPromise>> inflightResponses = removeAllInflightResponses();

        if (inflightResponses.isEmpty() == false) {
            ClosedChannelException closedChannelException = new ClosedChannelException();
            for (Tuple<? extends Netty4RestResponse, ChannelPromise> inflightResponse : inflightResponses) {
                safeFailPromise(inflightResponse.v2(), closedChannelException);
            }
        }
        ctx.close(promise);
    }

    private void safeFailPromise(ChannelPromise promise, Exception ex) {
        try {
            promise.setFailure(ex);
        } catch (RuntimeException e) {
            logger.error("unexpected error while releasing pipelined http responses", e);
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

    private List<Tuple<? extends Netty4RestResponse, ChannelPromise>> removeAllInflightResponses() {
        ArrayList<Tuple<? extends Netty4RestResponse, ChannelPromise>> responses = new ArrayList<>(outboundHoldingQueue);
        outboundHoldingQueue.clear();
        return responses;
    }

    private record WriteOperation(HttpObject msg, ChannelPromise promise) {

        void failAsClosedChannel() {
            promise.tryFailure(new ClosedChannelException());
            ReferenceCountUtil.release(msg);
        }
    }
}
