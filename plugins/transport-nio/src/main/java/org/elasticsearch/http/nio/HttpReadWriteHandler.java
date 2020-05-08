/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http.nio;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.http.CorsHandler;
import org.elasticsearch.http.HttpHandlingSettings;
import org.elasticsearch.http.HttpPipelinedRequest;
import org.elasticsearch.http.HttpPipelinedResponse;
import org.elasticsearch.http.HttpReadTimeoutException;
import org.elasticsearch.http.nio.cors.NioCorsHandler;
import org.elasticsearch.nio.FlushOperation;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioChannelHandler;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.nio.TaskScheduler;
import org.elasticsearch.nio.WriteOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

public class HttpReadWriteHandler implements NioChannelHandler {

    private final NettyAdaptor adaptor;
    private final NioHttpChannel nioHttpChannel;
    private final NioHttpServerTransport transport;
    private final TaskScheduler taskScheduler;
    private final LongSupplier nanoClock;
    private final long readTimeoutNanos;
    private boolean channelActive = false;
    private boolean requestSinceReadTimeoutTrigger = false;
    private int inFlightRequests = 0;

    public HttpReadWriteHandler(NioHttpChannel nioHttpChannel, NioHttpServerTransport transport, HttpHandlingSettings settings,
                                CorsHandler.Config corsConfig, TaskScheduler taskScheduler, LongSupplier nanoClock) {
        this.nioHttpChannel = nioHttpChannel;
        this.transport = transport;
        this.taskScheduler = taskScheduler;
        this.nanoClock = nanoClock;
        this.readTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(settings.getReadTimeoutMillis());

        List<ChannelHandler> handlers = new ArrayList<>(8);
        HttpRequestDecoder decoder = new HttpRequestDecoder(settings.getMaxInitialLineLength(), settings.getMaxHeaderSize(),
            settings.getMaxChunkSize());
        decoder.setCumulator(ByteToMessageDecoder.COMPOSITE_CUMULATOR);
        handlers.add(decoder);
        handlers.add(new HttpContentDecompressor());
        handlers.add(new HttpResponseEncoder());
        handlers.add(new HttpObjectAggregator(settings.getMaxContentLength()));
        if (settings.isCompression()) {
            handlers.add(new HttpContentCompressor(settings.getCompressionLevel()));
        }
        handlers.add(new NioHttpRequestCreator());
        if (settings.isCorsEnabled()) {
            handlers.add(new NioCorsHandler(corsConfig));
        }
        handlers.add(new NioHttpPipeliningHandler(transport.getLogger(), settings.getPipeliningMaxEvents()));

        adaptor = new NettyAdaptor(handlers.toArray(new ChannelHandler[0]));
        adaptor.addCloseListener((v, e) -> nioHttpChannel.close());
    }

    @Override
    public void channelActive() {
        channelActive = true;
        if (readTimeoutNanos > 0) {
            scheduleReadTimeout();
        }
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) {
        assert channelActive : "channelActive should have been called";
        int bytesConsumed = adaptor.read(channelBuffer.sliceAndRetainPagesTo(channelBuffer.getIndex()));
        Object message;
        while ((message = adaptor.pollInboundMessage()) != null) {
            ++inFlightRequests;
            requestSinceReadTimeoutTrigger = true;
            handleRequest(message);
        }

        return bytesConsumed;
    }

    @Override
    public WriteOperation createWriteOperation(SocketChannelContext context, Object message, BiConsumer<Void, Exception> listener) {
        assert assertMessageTypes(message);
        return new HttpWriteOperation(context, (HttpPipelinedResponse) message, listener);
    }

    @Override
    public List<FlushOperation> writeToBytes(WriteOperation writeOperation) {
        assert assertMessageTypes(writeOperation.getObject());
        assert channelActive : "channelActive should have been called";
        --inFlightRequests;
        assert inFlightRequests >= 0 : "Inflight requests should never drop below zero, found: " + inFlightRequests;
        adaptor.write(writeOperation);
        return pollFlushOperations();
    }

    @Override
    public List<FlushOperation> pollFlushOperations() {
        ArrayList<FlushOperation> copiedOperations = new ArrayList<>(adaptor.getOutboundCount());
        FlushOperation flushOperation;
        while ((flushOperation = adaptor.pollOutboundOperation()) != null) {
            copiedOperations.add(flushOperation);
        }
        return copiedOperations;
    }

    @Override
    public boolean closeNow() {
        return false;
    }

    @Override
    public void close() throws IOException {
        try {
            adaptor.close();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void handleRequest(Object msg) {
        final HttpPipelinedRequest pipelinedRequest = (HttpPipelinedRequest) msg;
        boolean success = false;
        try {
            transport.incomingRequest(pipelinedRequest, nioHttpChannel);
            success = true;
        } finally {
            if (success == false) {
                pipelinedRequest.release();
            }
        }
    }

    private void maybeReadTimeout() {
        if (requestSinceReadTimeoutTrigger == false && inFlightRequests == 0) {
            transport.onException(nioHttpChannel, new HttpReadTimeoutException(TimeValue.nsecToMSec(readTimeoutNanos)));
        } else {
            requestSinceReadTimeoutTrigger = false;
            scheduleReadTimeout();
        }
    }

    private void scheduleReadTimeout() {
        taskScheduler.scheduleAtRelativeTime(this::maybeReadTimeout, nanoClock.getAsLong() + readTimeoutNanos);
    }

    private static boolean assertMessageTypes(Object message) {
        assert message instanceof HttpPipelinedResponse : "This channel only supports messages that are of type: "
            + HttpPipelinedResponse.class + ". Found type: " + message.getClass() + ".";
        assert ((HttpPipelinedResponse) message).getDelegateRequest() instanceof NioHttpResponse :
            "This channel only pipelined responses with a delegate of type: " + NioHttpResponse.class +
                ". Found type: " + ((HttpPipelinedResponse) message).getDelegateRequest().getClass() + ".";
        return true;
    }
}
