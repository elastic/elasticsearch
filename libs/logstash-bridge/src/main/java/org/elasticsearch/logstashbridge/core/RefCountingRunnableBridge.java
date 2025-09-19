/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.core;

import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

import java.io.Closeable;

/**
 * A {@link StableBridgeAPI} for {@link RefCountingRunnable}
 */
public interface RefCountingRunnableBridge extends StableBridgeAPI<RefCountingRunnable>, Closeable {

    @Override // only RuntimeException
    void close();

    ReleasableBridge acquire();

    /**
     * An API-stable factory method for {@link RefCountingRunnableBridge}
     * @param delegate the {@link Runnable} to execute when all refs are closed
     * @return a {@link RefCountingRunnableBridge} that will execute the provided
     * block when all refs are closed
     */
    static RefCountingRunnableBridge create(final Runnable delegate) {
        final RefCountingRunnable refCountingRunnable = new RefCountingRunnable(delegate);
        return new ProxyInternal(refCountingRunnable);
    }

    /**
     * An implementation of {@link RefCountingRunnableBridge} that proxies calls through
     * to an internal {@link RefCountingRunnable}.
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<RefCountingRunnable> implements RefCountingRunnableBridge {
        private ProxyInternal(final RefCountingRunnable delegate) {
            super(delegate);
        }

        @Override
        public void close() {
            toInternal().close();
        }

        @Override
        public ReleasableBridge acquire() {
            @SuppressWarnings("resource")
            final Releasable releasable = toInternal().acquire();
            return ReleasableBridge.fromInternal(releasable);
        }
    }
}
