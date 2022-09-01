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
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.transport.netty4.NettyAllocator;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Implements HTTP pipelining ordering, ensuring that responses are completely served in the same order as their corresponding requests.
 */
public class Netty4HttpPipeliningHandler extends ChannelDuplexHandler {

    private final Logger logger;

    private final int maxEventsHeld;
    private final PriorityQueue<Tuple<Netty4HttpResponse, ChannelPromise>> outboundHoldingQueue;

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
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) {
        assert msg instanceof Netty4HttpResponse : "Invalid message type: " + msg.getClass();
        boolean success = false;
        try {
            final Netty4HttpResponse response = (Netty4HttpResponse) msg;
            if (response.getSequence() != writeSequence) {
                assert response.getSequence() > writeSequence
                    : "response sequence [" + response.getSequence() + "] we below write sequence [" + writeSequence + "]";
                if (outboundHoldingQueue.size() >= maxEventsHeld) {
                    int eventCount = outboundHoldingQueue.size() + 1;
                    throw new IllegalStateException(
                        "Too many pipelined events [" + eventCount + "]. Max events allowed [" + maxEventsHeld + "]."
                    );
                }
                // response is not at the current sequence number so we add it to the outbound queue and return
                outboundHoldingQueue.add(new Tuple<>(response, promise));
                success = true;
                return;
            }

            // response is at the current sequence number and does not need to wait for any other response to be written so we write
            // it out directly
            doWrite(ctx, response, promise);
            success = true;
            // see if we have any queued up responses that became writeable due to the above write
            while (outboundHoldingQueue.isEmpty() == false && outboundHoldingQueue.peek().v1().getSequence() == writeSequence) {
                final Tuple<Netty4HttpResponse, ChannelPromise> top = outboundHoldingQueue.poll();
                assert top != null : "we know the outbound holding queue to not be empty at this point";
                doWrite(ctx, top.v1(), top.v2());
            }
        } catch (IllegalStateException e) {
            ctx.channel().close();
        } finally {
            if (success == false) {
                promise.setFailure(new ClosedChannelException());
            }
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

    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        if (ctx.channel().isWritable()) {
            doFlush(ctx);
        }
        ctx.fireChannelWritabilityChanged();
    }

    @Override
    public void flush(ChannelHandlerContext ctx) {
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
    private boolean doFlush(ChannelHandlerContext ctx) {
        assert ctx.executor().inEventLoop();
        final Channel channel = ctx.channel();
        if (channel.isActive() == false) {
            failQueuedWrites();
            return false;
        }
        while (channel.isWritable()) {
            final WriteOperation currentWrite = queuedWrites.poll();
            if (currentWrite == null) {
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

    private void failQueuedWrites() {
        WriteOperation queuedWrite;
        while ((queuedWrite = queuedWrites.poll()) != null) {
            queuedWrite.failAsClosedChannel();
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) {
        List<Tuple<Netty4HttpResponse, ChannelPromise>> inflightResponses = removeAllInflightResponses();

        if (inflightResponses.isEmpty() == false) {
            ClosedChannelException closedChannelException = new ClosedChannelException();
            for (Tuple<Netty4HttpResponse, ChannelPromise> inflightResponse : inflightResponses) {
                try {
                    inflightResponse.v2().setFailure(closedChannelException);
                } catch (RuntimeException e) {
                    logger.error("unexpected error while releasing pipelined http responses", e);
                }
            }
        }
        ctx.close(promise);
    }

    private Future<Void> enqueueWrite(ChannelHandlerContext ctx, HttpObject msg) {
        final ChannelPromise p = ctx.newPromise();
        enqueueWrite(ctx, msg, p);
        return p;
    }

    private void enqueueWrite(ChannelHandlerContext ctx, HttpObject msg, ChannelPromise promise) {
        if (ctx.channel().isWritable() && queuedWrites.isEmpty()) {
            ctx.write(msg, promise);
        } else {
            queuedWrites.add(new WriteOperation(msg, promise));
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

    private List<Tuple<Netty4HttpResponse, ChannelPromise>> removeAllInflightResponses() {
        ArrayList<Tuple<Netty4HttpResponse, ChannelPromise>> responses = new ArrayList<>(outboundHoldingQueue);
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
