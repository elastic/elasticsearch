/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xcontent.ToXContentObject;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A "side channel" for the {@link Operator}s running on one node. These
 * nodes must run on the same JVM, but will not run in the same {@link Driver}.
 * <p>
 *     {@linkplain SideChannel}s are {@link RefCounted} so they can release
 *     resources when the last {@linkplain Operator} referencing them
 *     is {@link Operator#close}d.
 * </p>
 * <p>
 *     {@linkplain SideChannel}s are configured during physical planning.
 *     And the physical plan nodes are <strong>not</strong> {@link RefCounted}.
 *     So we can't make the actual {@linkplain SideChannel} implementations
 *     during planning. Instead, we configure a supplier that builds the
 *     {@linkplain SideChannel} when building the first {@link Operator}
 *     instance.
 * </p>
 * <p>
 *     This implements {@link Releasable} as a convenience that delegates to
 *     {@link #decRef}.
 * </p>
 */
public abstract class SideChannel extends AbstractRefCounted implements Releasable {
    /**
     * Builds the {@link SideChannel} on first use and returns a shared reference
     * on the next use.
     */
    public abstract static class Supplier<T extends SideChannel> {
        private final AtomicReference<T> v = new AtomicReference<>();

        protected abstract T build();

        /**
         * Get the {@link SideChannel}, building it if this is the first call and
         * incrementing its reference it if it has already been built.
         * <p>
         *     This will only ever build one channel at a time. There are 3
         *     possible states:
         * </p>
         * <ol>
         *     <li>{@code v} contains {@code null} - create a new channel</li>
         *     <li>
         *         {@code v} contains a side channel, but it is closed - the thread that
         *         closed it will clear {@code v}. Loop until {@code v} contains
         *         {@code null}.
         *     </li>
         *     <li>
         *         {@code v} contains a side channel and successfully {@link #tryIncRef}
         *         it - return it.
         *     </li>
         * </ol>
         */
        public final T get() {
            while (true) {
                T channel = v.get();
                if (channel != null && channel.tryIncRef()) {
                    return channel;
                }
                synchronized (v) {
                    channel = v.get();
                    if (channel != null) {
                        if (channel.tryIncRef()) {
                            return channel;
                        } else {
                            continue;
                        }
                    }
                    channel = build();
                    v.set(channel);
                    return channel;
                }
            }
        }
    }

    private final Supplier<?> mySupplier;

    protected SideChannel(Supplier<?> mySupplier) {
        this.mySupplier = mySupplier;
    }

    @Override
    public final void close() {
        decRef();
    }

    @Override
    protected final void closeInternal() {
        closeSideChannel();
        /*
         * Clear the supplier so that this can be GCed. And, in the rare case
         * that we need to get another SideChannel, we'll build an entirely new one.
         */
        mySupplier.v.set(null);
    }

    protected abstract void closeSideChannel();

    // TODO status? it's concurrent.
    public interface Status extends ToXContentObject, VersionedNamedWriteable {

    }
}
