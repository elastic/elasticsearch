/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.compression.JdkZlibEncoder;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.concurrent.PromiseCombiner;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.transport.Transports;
import org.elasticsearch.transport.netty4.NettyAllocator;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

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
            List<Tuple<Netty4HttpResponse, ChannelPromise>> readyResponses = write((Netty4HttpResponse) msg, promise);
            for (Tuple<Netty4HttpResponse, ChannelPromise> readyResponse : readyResponses) {
                doWrite(ctx, readyResponse.v1(), readyResponse.v2());
            }
            success = true;
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
            ctx.write(readyResponse, promise);
        } else {
            splitAndWrite(ctx, readyResponse, promise);
        }
    }

    private void splitAndWrite(ChannelHandlerContext ctx, Netty4HttpResponse msg, ChannelPromise promise) {
        final PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
        HttpResponse response = new DefaultHttpResponse(msg.protocolVersion(), msg.status(), msg.headers());
        combiner.add(ctx.write(response));
        ByteBuf content = msg.content();
        while (content.readableBytes() > SPLIT_THRESHOLD) {
            combiner.add(ctx.write(new DefaultHttpContent(content.readRetainedSlice(SPLIT_THRESHOLD))));
        }
        combiner.add(ctx.write(new DefaultLastHttpContent(content.readRetainedSlice(content.readableBytes()))));
        combiner.finish(promise);
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

    private List<Tuple<Netty4HttpResponse, ChannelPromise>> write(final Netty4HttpResponse response, ChannelPromise promise) {
        if (outboundHoldingQueue.size() < maxEventsHeld) {
            ArrayList<Tuple<Netty4HttpResponse, ChannelPromise>> readyResponses = new ArrayList<>();
            outboundHoldingQueue.add(new Tuple<>(response, promise));
            while (outboundHoldingQueue.isEmpty() == false) {
                /*
                 * Since the response with the lowest sequence number is the top of the priority queue, we know if its sequence
                 * number does not match the current write sequence number then we have not processed all preceding responses yet.
                 */
                final Tuple<Netty4HttpResponse, ChannelPromise> top = outboundHoldingQueue.peek();

                if (top.v1().getSequence() != writeSequence) {
                    break;
                }
                outboundHoldingQueue.poll();
                readyResponses.add(top);
                writeSequence++;
            }

            return readyResponses;
        } else {
            int eventCount = outboundHoldingQueue.size() + 1;
            throw new IllegalStateException("Too many pipelined events [" + eventCount + "]. Max events allowed [" + maxEventsHeld + "].");
        }
    }

    private List<Tuple<Netty4HttpResponse, ChannelPromise>> removeAllInflightResponses() {
        ArrayList<Tuple<Netty4HttpResponse, ChannelPromise>> responses = new ArrayList<>(outboundHoldingQueue);
        outboundHoldingQueue.clear();
        return responses;
    }
}
