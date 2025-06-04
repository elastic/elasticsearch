/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.NettyRuntime;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ImmediateEventExecutor;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.http.HttpBody;
import org.elasticsearch.transport.TransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class Netty4Utils {

    private static final AtomicBoolean isAvailableProcessorsSet = new AtomicBoolean();

    /**
     * Set the number of available processors that Netty uses for sizing various resources (e.g., thread pools).
     *
     * @param availableProcessors the number of available processors
     * @throws IllegalStateException if available processors was set previously and the specified value does not match the already-set value
     */
    public static void setAvailableProcessors(final int availableProcessors) {
        // we set this to false in tests to avoid tests that randomly set processors from stepping on each other
        final boolean set = Booleans.parseBoolean(System.getProperty("es.set.netty.runtime.available.processors", "true"));
        if (set == false) {
            return;
        }

        /*
         * This can be invoked twice, once from Netty4Transport and another time from Netty4HttpServerTransport; however,
         * Netty4Runtime#availableProcessors forbids settings the number of processors twice so we prevent double invocation here.
         */
        if (isAvailableProcessorsSet.compareAndSet(false, true)) {
            NettyRuntime.setAvailableProcessors(availableProcessors);
        } else if (availableProcessors != NettyRuntime.availableProcessors()) {
            /*
             * We have previously set the available processors yet either we are trying to set it to a different value now or there is a bug
             * in Netty and our previous value did not take, bail.
             */
            final String message = String.format(
                Locale.ROOT,
                "available processors value [%d] did not match current value [%d]",
                availableProcessors,
                NettyRuntime.availableProcessors()
            );
            throw new IllegalStateException(message);
        }
    }

    /**
     * Turns the given BytesReference into a ByteBuf. Note: the returned ByteBuf will reference the internal
     * pages of the BytesReference. Don't free the bytes of reference before the ByteBuf goes out of scope.
     */
    public static ByteBuf toByteBuf(final BytesReference reference) {
        if (reference.hasArray()) {
            return Unpooled.wrappedBuffer(reference.array(), reference.arrayOffset(), reference.length());
        }
        return compositeReferenceToByteBuf(reference);
    }

    private static ByteBuf compositeReferenceToByteBuf(BytesReference reference) {
        final BytesRefIterator iterator = reference.iterator();
        // usually we have one, two, or three components from the header, the message, and a buffer
        final List<ByteBuf> buffers = new ArrayList<>(3);
        try {
            BytesRef slice;
            while ((slice = iterator.next()) != null) {
                buffers.add(Unpooled.wrappedBuffer(slice.bytes, slice.offset, slice.length));
            }

            if (buffers.size() == 1) {
                return buffers.get(0);
            } else {
                CompositeByteBuf composite = Unpooled.compositeBuffer(buffers.size());
                composite.addComponents(true, buffers);
                return composite;
            }
        } catch (IOException ex) {
            throw new AssertionError("no IO happens here", ex);
        }
    }

    /**
     * Wraps the given ChannelBuffer with a BytesReference
     */
    public static BytesReference toBytesReference(final ByteBuf buffer) {
        final int readableBytes = buffer.readableBytes();
        if (readableBytes == 0) {
            return BytesArray.EMPTY;
        } else if (buffer.hasArray()) {
            return new BytesArray(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), readableBytes);
        } else {
            final ByteBuffer[] byteBuffers = buffer.nioBuffers();
            return BytesReference.fromByteBuffers(byteBuffers);
        }
    }

    /**
     * Wrap Netty's {@link ByteBuf} into {@link ReleasableBytesReference} and delegating reference count to ByteBuf.
     */
    public static ReleasableBytesReference toReleasableBytesReference(final ByteBuf buffer) {
        return new ReleasableBytesReference(toBytesReference(buffer), toRefCounted(buffer));
    }

    static ByteBufRefCounted toRefCounted(final ByteBuf buf) {
        return new ByteBufRefCounted(buf);
    }

    record ByteBufRefCounted(ByteBuf buffer) implements RefCounted {

        public int refCnt() {
            return buffer.refCnt();
        }

        @Override
        public void incRef() {
            buffer.retain();
        }

        @Override
        public boolean tryIncRef() {
            if (hasReferences() == false) {
                return false;
            }
            try {
                buffer.retain();
            } catch (RuntimeException e) {
                assert hasReferences() == false;
                return false;
            }
            return true;
        }

        @Override
        public boolean decRef() {
            return buffer.release();
        }

        @Override
        public boolean hasReferences() {
            return buffer.refCnt() > 0;
        }
    }

    public static HttpBody.Full fullHttpBodyFrom(final ByteBuf buf) {
        return new HttpBody.ByteRefHttpBody(toReleasableBytesReference(buf));
    }

    public static Recycler<BytesRef> createRecycler(Settings settings) {
        // If this method is called by super ctor the processors will not be set. Accessing NettyAllocator initializes netty's internals
        // setting the processors. We must do it ourselves first just in case.
        setAvailableProcessors(EsExecutors.allocatedProcessors(settings));
        return NettyAllocator.getRecycler();
    }

    /**
     * Calls {@link Channel#writeAndFlush} to write the given message to the given channel, but ensures that the listener is completed even
     * if the event loop is concurrently shutting down since Netty does not offer this guarantee.
     */
    public static void safeWriteAndFlush(Channel channel, Object message, ActionListener<Void> listener) {
        // Use ImmediateEventExecutor.INSTANCE since we want to be able to complete this promise, and any waiting listeners, even if the
        // channel's event loop has shut down. Normally this completion will happen on the channel's event loop anyway because the write op
        // can only be completed by some network event from this point on. However...
        final var promise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        addListener(promise, listener);
        assert assertCorrectPromiseListenerThreading(promise);
        channel.writeAndFlush(message, promise);
        if (channel.eventLoop().isShuttingDown()) {
            // ... if we get here then the event loop may already have terminated, and https://github.com/netty/netty/issues/8007 means that
            // we cannot know if the preceding writeAndFlush made it onto its queue before shutdown or whether it will just vanish without a
            // trace, so to avoid a leak we must double-check that the final listener is completed.
            channel.eventLoop().terminationFuture().addListener(ignored ->
            // NB the promise executor is ImmediateEventExecutor.INSTANCE which means this call to tryFailure() will ensure its completion,
            // and the completion of any waiting listeners, without forking away from the current thread. The current thread might be the
            // thread that was running the event loop since that's where the terminationFuture is completed, or it might be a thread which
            // called (and is still calling) safeWriteAndFlush.
            promise.tryFailure(new TransportException("Cannot send network message, event loop is shutting down.")));
        }
    }

    private static boolean assertCorrectPromiseListenerThreading(ChannelPromise promise) {
        addListener(promise, future -> {
            var eventLoop = future.channel().eventLoop();
            assert eventLoop.inEventLoop() || future.cause() instanceof RejectedExecutionException || eventLoop.isTerminated()
                : future.cause();
        });
        return true;
    }

    /**
     * Subscribes the given {@link ActionListener} to the given {@link Future}.
     */
    public static void addListener(Future<Void> future, ActionListener<Void> listener) {
        future.addListener(f -> {
            if (f.isSuccess()) {
                listener.onResponse(null);
            } else {
                final Throwable cause = f.cause();
                ExceptionsHelper.maybeDieOnAnotherThread(cause);
                if (cause instanceof Exception exception) {
                    listener.onFailure(exception);
                } else {
                    listener.onFailure(new Exception(cause));
                }
            }
        });
    }

    @SuppressForbidden(reason = "single point for adding listeners that enforces use of ChannelFutureListener")
    public static void addListener(ChannelFuture channelFuture, ChannelFutureListener listener) {
        channelFuture.addListener(listener);
    }
}
