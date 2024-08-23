/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.LastHttpContent;

import org.elasticsearch.http.HttpBody;
import org.elasticsearch.transport.netty4.Netty4Utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Netty based implementation of {@link HttpBody.Stream}.
 * This implementation utilize {@link io.netty.channel.ChannelConfig#setAutoRead(boolean)}
 * to prevent entire payload buffering. But sometimes upstream can send few chunks of data despite
 * autoRead=off. In this case chunks will be queued until downstream calls {@link Stream#next()}
 */
public class Netty4HttpRequestBodyStream implements HttpBody.Stream {

    private State state;

    public Netty4HttpRequestBodyStream(Channel channel) {
        this.state = new State.Queueing(channel);
        channel.closeFuture().addListener(f -> onChannelClose());
    }

    private static void releaseQueuedChunks(Queue<HttpContent> chunkQueue) {
        while (chunkQueue.isEmpty() == false) {
            chunkQueue.poll().release();
        }
    }

    @Override
    public void addTracingHandler(ChunkHandler chunkHandler) {
        if (state instanceof State.Queueing q) {
            q.tracingHandlers.add(chunkHandler);
        } else if (state instanceof State.Streaming s) {
            s.tracingHandlers.add(chunkHandler);
        }
    }

    @Override
    public void setConsumingHandler(ChunkHandler chunkHandler) {
        if (isQueueing() == false) {
            throw new IllegalStateException("only queueing state can set consuming handler, got " + stateName());
        }
        state = new State.Streaming((State.Queueing) state, chunkHandler);
    }

    @Override
    public void discard() {
        if (state instanceof State.Queueing q) {
            state = new State.Draining(q);
        } else if (state instanceof State.Streaming s) {
            state = new State.Draining(s);
        }
    }

    @Override
    public void next() {
        if (isStreaming()) {
            var streamingState = state.asStreaming();
            if (streamingState.channel.eventLoop().inEventLoop()) {
                streamingState.sendQueuedOrRead();
            } else {
                streamingState.channel.eventLoop().submit(streamingState::sendQueuedOrRead);
            }
        }
    }

    public void onHttpContent(HttpContent httpContent) {
        var isLast = httpContent instanceof LastHttpContent;
        if (isCompleted()) {
            throw new IllegalStateException("received netty chunk on completed stream");
        } else if (isDraining()) {
            httpContent.release();
            if (isLast) {
                state = new State.Completed(state.asDraining());
            }
        } else if (isQueueing()) {
            state.asQueueing().chunkQueue.add(httpContent);
        } else if (isStreaming()) {
            var streamingState = state.asStreaming();
            if (streamingState.requested && streamingState.chunkQueue.isEmpty()) {
                streamingState.sendChunk(httpContent);
                if (isLast) {
                    state = new State.Completed(streamingState);
                }
            } else {
                streamingState.chunkQueue.add(httpContent);
            }
        } else {
            assert false : "must handle all states, got " + stateName();
        }
    }

    // visible for test
    Channel channel() {
        return null;
    }

    // visible for test
    int queueSize() {
        if (isQueueing()) {
            return state.asQueueing().chunkQueue.size();
        } else if (isStreaming()) {
            return state.asStreaming().chunkQueue.size();
        } else {
            return 0;
        }
    }

    // visible for test
    boolean hasLast() {
        return false;
    }

    private String stateName() {
        return state.getClass().getSimpleName();
    }

    boolean isQueueing() {
        return state instanceof State.Queueing;
    }

    boolean isStreaming() {
        return state instanceof State.Streaming;
    }

    boolean isDraining() {
        return state instanceof State.Draining;
    }

    boolean isCompleted() {
        return state instanceof State.Completed;
    }

    private void onChannelClose() {
        discard();
    }

    private sealed interface State permits State.Completed, State.Draining, State.Queueing, State.Streaming {

        default Queueing asQueueing() {
            assert this instanceof Queueing;
            return (Queueing) this;
        }

        default Streaming asStreaming() {
            assert this instanceof Streaming;
            return (Streaming) this;
        }

        default Draining asDraining() {
            assert this instanceof Draining;
            return (Draining) this;
        }

        final class Queueing implements State {
            final Channel channel;
            final Queue<HttpContent> chunkQueue = new ArrayDeque<>();
            final List<ChunkHandler> tracingHandlers = new ArrayList<>();
            boolean hasLast = false;

            Queueing(Channel channel) {
                this.channel = channel;
                channel.config().setAutoRead(false);
            }
        }

        final class Streaming implements State {
            final Channel channel;
            final Queue<HttpContent> chunkQueue;
            final List<ChunkHandler> tracingHandlers;
            final ChunkHandler consumingHandler;
            boolean hasLast;
            boolean requested;

            Streaming(Queueing queueing, ChunkHandler consumingHandler) {
                this.channel = queueing.channel;
                this.chunkQueue = queueing.chunkQueue;
                this.tracingHandlers = queueing.tracingHandlers;
                this.consumingHandler = consumingHandler;
                this.hasLast = queueing.hasLast;
            }

            void sendChunk(HttpContent httpContent) {
                assert requested;
                requested = false;
                var bytesRef = Netty4Utils.toReleasableBytesReference(httpContent.content());
                var isLast = httpContent instanceof LastHttpContent;
                tracingHandlers.forEach(h -> h.onNext(bytesRef, isLast));
                consumingHandler.onNext(bytesRef, isLast);
            }

            void sendQueuedOrRead() {
                assert channel.eventLoop().inEventLoop();
                requested = true;
                var chunk = chunkQueue.poll();
                if (chunk == null) {
                    channel.read();
                } else {
                    sendChunk(chunk);
                }
            }
        }

        final class Completed implements State {
            Completed(Draining draining) {}

            Completed(Streaming streaming) {
                streaming.channel.config().setAutoRead(true);
            }
        }

        final class Draining implements State {
            Draining(Queueing queueing) {
                queueing.channel.config().setAutoRead(true);
                releaseQueuedChunks(queueing.chunkQueue);
            }

            Draining(Streaming streaming) {
                streaming.channel.config().setAutoRead(true);
                releaseQueuedChunks(streaming.chunkQueue);
            }
        }
    }

}
