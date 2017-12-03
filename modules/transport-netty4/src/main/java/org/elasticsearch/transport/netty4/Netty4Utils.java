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

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.NettyRuntime;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.InternalLoggerFactory;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class Netty4Utils {

    static {
        InternalLoggerFactory.setDefaultFactory(new InternalLoggerFactory() {

            @Override
            public InternalLogger newInstance(final String name) {
                return new Netty4InternalESLogger(name);
            }

        });
    }

    public static void setup() {

    }

    private static AtomicBoolean isAvailableProcessorsSet = new AtomicBoolean();

    /**
     * Set the number of available processors that Netty uses for sizing various resources (e.g., thread pools).
     *
     * @param availableProcessors the number of available processors
     * @throws IllegalStateException if available processors was set previously and the specified value does not match the already-set value
     */
    public static void setAvailableProcessors(final int availableProcessors) {
        // we set this to false in tests to avoid tests that randomly set processors from stepping on each other
        final boolean set = Booleans.parseBoolean(System.getProperty("es.set.netty.runtime.available.processors", "true"));
        if (!set) {
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
                    NettyRuntime.availableProcessors());
            throw new IllegalStateException(message);
        }
    }

    /**
     * Turns the given BytesReference into a ByteBuf. Note: the returned ByteBuf will reference the internal
     * pages of the BytesReference. Don't free the bytes of reference before the ByteBuf goes out of scope.
     */
    public static ByteBuf toByteBuf(final BytesReference reference) {
        if (reference.length() == 0) {
            return Unpooled.EMPTY_BUFFER;
        }
        if (reference instanceof ByteBufBytesReference) {
            return ((ByteBufBytesReference) reference).toByteBuf();
        } else {
            final BytesRefIterator iterator = reference.iterator();
            // usually we have one, two, or three components from the header, the message, and a buffer
            final List<ByteBuf> buffers = new ArrayList<>(3);
            try {
                BytesRef slice;
                while ((slice = iterator.next()) != null) {
                    buffers.add(Unpooled.wrappedBuffer(slice.bytes, slice.offset, slice.length));
                }
                final CompositeByteBuf composite = Unpooled.compositeBuffer(buffers.size());
                composite.addComponents(true, buffers);
                return composite;
            } catch (IOException ex) {
                throw new AssertionError("no IO happens here", ex);
            }
        }
    }

    /**
     * Wraps the given ChannelBuffer with a BytesReference
     */
    public static BytesReference toBytesReference(final ByteBuf buffer) {
        return toBytesReference(buffer, buffer.readableBytes());
    }

    /**
     * Wraps the given ChannelBuffer with a BytesReference of a given size
     */
    static BytesReference toBytesReference(final ByteBuf buffer, final int size) {
        return new ByteBufBytesReference(buffer, size);
    }

    public static void closeChannels(final Collection<Channel> channels) throws IOException {
        IOException closingExceptions = null;
        final List<ChannelFuture> futures = new ArrayList<>();
        for (final Channel channel : channels) {
            try {
                if (channel != null && channel.isOpen()) {
                    futures.add(channel.close());
                }
            } catch (Exception e) {
                if (closingExceptions == null) {
                    closingExceptions = new IOException("failed to close channels");
                }
                closingExceptions.addSuppressed(e);
            }
        }
        for (final ChannelFuture future : futures) {
            future.awaitUninterruptibly();
        }

        if (closingExceptions != null) {
            throw closingExceptions;
        }
    }

    /**
     * If the specified cause is an unrecoverable error, this method will rethrow the cause on a separate thread so that it can not be
     * caught and bubbles up to the uncaught exception handler.
     *
     * @param cause the throwable to test
     */
    public static void maybeDie(final Throwable cause) {
        final Optional<Error> maybeError = maybeError(cause);
        if (maybeError.isPresent()) {
            /*
             * Here be dragons. We want to rethrow this so that it bubbles up to the uncaught exception handler. Yet, Netty wraps too many
             * invocations of user-code in try/catch blocks that swallow all throwables. This means that a rethrow here will not bubble up
             * to where we want it to. So, we fork a thread and throw the exception from there where Netty can not get to it. We do not wrap
             * the exception so as to not lose the original cause during exit.
             */
            try {
                // try to log the current stack trace
                final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                final String formatted = Arrays.stream(stackTrace).skip(1).map(e -> "\tat " + e).collect(Collectors.joining("\n"));
                final Logger logger = ESLoggerFactory.getLogger(Netty4Utils.class);
                logger.error("fatal error on the network layer\n{}", formatted);
            } finally {
                new Thread(
                        () -> {
                            throw maybeError.get();
                        })
                        .start();
            }
        }
    }

    static final int MAX_ITERATIONS = 1024;

    /**
     * Unwrap the specified throwable looking for any suppressed errors or errors as a root cause of the specified throwable.
     *
     * @param cause the root throwable
     *
     * @return an optional error if one is found suppressed or a root cause in the tree rooted at the specified throwable
     */
    static Optional<Error> maybeError(final Throwable cause) {
        // early terminate if the cause is already an error
        if (cause instanceof Error) {
            return Optional.of((Error) cause);
        }

        final Queue<Throwable> queue = new LinkedList<>();
        queue.add(cause);
        int iterations = 0;
        while (!queue.isEmpty()) {
            iterations++;
            if (iterations > MAX_ITERATIONS) {
                ESLoggerFactory.getLogger(Netty4Utils.class).warn("giving up looking for fatal errors on the network layer", cause);
                break;
            }
            final Throwable current = queue.remove();
            if (current instanceof Error) {
                return Optional.of((Error) current);
            }
            Collections.addAll(queue, current.getSuppressed());
            if (current.getCause() != null) {
                queue.add(current.getCause());
            }
        }
        return Optional.empty();
    }

}
