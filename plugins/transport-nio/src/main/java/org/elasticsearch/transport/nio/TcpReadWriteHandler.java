/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.nio.BytesWriteHandler;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.Page;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.InboundPipeline;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.function.Supplier;

public class TcpReadWriteHandler extends BytesWriteHandler {

    private final NioTcpChannel channel;
    private final InboundPipeline pipeline;

    public TcpReadWriteHandler(NioTcpChannel channel, PageCacheRecycler recycler, TcpTransport transport) {
        this.channel = channel;
        final ThreadPool threadPool = transport.getThreadPool();
        final Supplier<CircuitBreaker> breaker = transport.getInflightBreaker();
        final Transport.RequestHandlers requestHandlers = transport.getRequestHandlers();
        this.pipeline = new InboundPipeline(transport.getVersion(), transport.getStatsTracker(), recycler, threadPool::relativeTimeInMillis,
            breaker, requestHandlers::getHandler, transport::inboundMessage);
    }

    @Override
    public int consumeReads(InboundChannelBuffer channelBuffer) throws IOException {
        Page[] pages = channelBuffer.sliceAndRetainPagesTo(channelBuffer.getIndex());
        BytesReference[] references = new BytesReference[pages.length];
        for (int i = 0; i < pages.length; ++i) {
            references[i] = BytesReference.fromByteBuffer(pages[i].byteBuffer());
        }
        Releasable releasable = pages.length == 1 ? pages[0] : () -> Releasables.closeExpectNoException(pages);
        try (ReleasableBytesReference reference = new ReleasableBytesReference(CompositeBytesReference.of(references), releasable)) {
            pipeline.handleBytes(channel, reference);
            return reference.length();
        }
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(pipeline, super::close);
    }
}
