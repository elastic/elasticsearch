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
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.http.HttpBody;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Netty based implementation of {@link HttpBody.Stream}.
 * This implementation utilize {@link io.netty.channel.ChannelConfig#setAutoRead(boolean)} to prevent entire payload buffering.
 * But sometimes upstream can send few chunks of data despite autoRead=off.
 * In this case chunks will be queued until downstream {@link HttpBody.Stream#requestBytes}
 */
public class Netty4HttpRequestBodyStream implements HttpBody.Stream {

    private final Channel channel;
    private final BlockingQueue<Integer> requestQueue = new LinkedBlockingQueue<>();
    private final ChunkQueue chunkQueue = new ChunkQueue();
    private final AtomicBoolean isDone = new AtomicBoolean(false);
    private HttpBody.ChunkHandler handler;

    public Netty4HttpRequestBodyStream(Channel channel) {
        this.channel = channel;
        channel.config().setAutoRead(false);
    }

    @Override
    public ChunkHandler handler() {
        return handler;
    }

    @Override
    public void setHandler(ChunkHandler chunkHandler) {
        this.handler = chunkHandler;
    }

    @Override
    public void requestBytes(int bytes) {
        assert handler != null : "handler must be set before requesting next chunk";
        if (isDone.get()) {
            return;
        }
        requestQueue.add(bytes);
        channel.eventLoop().execute(this::flushQueued); // flush on channel's thread only
        if (requestQueue.isEmpty() == false) {
            channel.read();
        }
    }

    public void handleNettyContent(HttpContent httpContent) {
        assert handler != null : "handler must be set before processing http content";

        var content = httpContent.content();
        var isLast = httpContent instanceof LastHttpContent;
        var currentRequest = requestQueue.peek();
        var availableBytes = chunkQueue.availableBytes() + content.readableBytes();

        if (currentRequest == null || (currentRequest > availableBytes && isLast == false)) {
            // enqueue incoming chunk:
            // 1. There is no ongoing request, it might happen that upstream send a chunk before request.
            // 2. There are not enough accumulated bytes, unless it's a last chunk then we must flush
            chunkQueue.offer(new ContentChunk(content, isLast));
        } else {
            // flush queued chunks:
            // 1. nothing in the queue and current chunk fulfills request, send it right a way
            // 2. there are queued chunks, enqueue and flush
            if (chunkQueue.isEmpty()) {
                this.handler.onNext(Netty4Utils.toReleasableBytesReference(content), isLast);
                requestQueue.poll();
            } else {
                chunkQueue.offer(new ContentChunk(content, isLast));
                flushQueued();
            }
        }

        if (isLast) {
            isDone.set(true);
            channel.config().setAutoRead(true);
        } else if (requestQueue.peek() != null) {
            channel.read();
        }
    }

    // visible for tests
    ChunkQueue contentChunkQueue() {
        return chunkQueue;
    }

    private boolean canFulfillCurrentRequest() {
        return requestQueue.isEmpty() == false && chunkQueue.isEmpty() == false
               // enough queued bytes
               && (chunkQueue.availableBytes() >= requestQueue.peek()
                   // not enough queued bytes, but got last content
                   || (chunkQueue.availableBytes < requestQueue.peek() && chunkQueue.hasLast()));
    }

    private void flushQueued() {
        while (canFulfillCurrentRequest()) {
            var currentRequest = requestQueue.poll();
            assert currentRequest != null;
            ByteBuf buf;
            boolean isLast = false;
            // next chunk is large enough, no need to aggregate
            if (chunkQueue.peekSize() >= currentRequest) {
                var chunk = chunkQueue.poll();
                isLast = chunk.isLast;
                buf = chunk.buf;
            } else {
                // aggregate multiple chunks
                var compBuf = channel.alloc().compositeBuffer();
                while (compBuf.readableBytes() < currentRequest && chunkQueue.isEmpty() == false) {
                    var chunk = chunkQueue.poll();
                    isLast = chunk.isLast;
                    compBuf.addComponent(true, chunk.buf());
                }
                buf = compBuf;
            }
            handler.onNext(Netty4Utils.toReleasableBytesReference(buf), isLast);
        }
    }

    record ContentChunk(ByteBuf buf, boolean isLast) {}

    // Wrapped queue that keeps track of total available bytes of all chunks in the queue
    static class ChunkQueue {
        private final Queue<ContentChunk> queue = new ArrayDeque<>();
        private int availableBytes = 0;
        private boolean hasLast = false;

        void release() {
            while (queue.isEmpty() == false) {
                queue.poll().buf.release();
            }
        }

        boolean isEmpty() {
            return queue.isEmpty();
        }

        int availableBytes() {
            return availableBytes;
        }

        boolean hasLast() {
            return hasLast;
        }

        int size() {
            return queue.size();
        }

        void offer(ContentChunk content) {
            queue.offer(content);
            availableBytes += content.buf.readableBytes();
            hasLast = content.isLast;
        }

        // returns size of next chunk in queue
        int peekSize() {
            var chunk = queue.peek();
            if (chunk == null) {
                return 0;
            } else {
                return chunk.buf.readableBytes();
            }
        }

        ContentChunk poll() {
            var chunk = queue.poll();
            assert chunk != null;
            availableBytes -= chunk.buf().readableBytes();
            return chunk;
        }
    }
}
