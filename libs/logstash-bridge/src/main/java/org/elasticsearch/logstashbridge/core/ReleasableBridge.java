/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.core;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

import java.io.Closeable;

/**
 * A {@link StableBridgeAPI} for {@link Releasable} for use with {@link RefCountingRunnableBridge}
 */
public interface ReleasableBridge extends StableBridgeAPI<Releasable>, Closeable {

    @Override // only RuntimeException
    void close();

    static ReleasableBridge fromInternal(Releasable releasable) {
        return new ProxyInternal(releasable);
    }

    /**
     * An implementation of {@link ReleasableBridge} that proxies calls through
     * to an internal {@link Releasable}.
     * @see StableBridgeAPI.ProxyInternal
     */
    final class ProxyInternal extends StableBridgeAPI.ProxyInternal<Releasable> implements ReleasableBridge {

        private ProxyInternal(final Releasable delegate) {
            super(delegate);
        }

        @Override
        public void close() {
            toInternal().close();
        }
    }
}
