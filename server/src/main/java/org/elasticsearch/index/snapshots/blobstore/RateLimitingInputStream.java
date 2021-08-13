/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.snapshots.blobstore;

import org.apache.lucene.store.RateLimiter;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

/**
 * Rate limiting wrapper for InputStream
 */
public class RateLimitingInputStream extends FilterInputStream {

    private final Supplier<RateLimiter> rateLimiterSupplier;

    private final Listener listener;

    private long bytesSinceLastRateLimit;

    public interface Listener {
        void onPause(long nanos);
    }

    public RateLimitingInputStream(InputStream delegate, Supplier<RateLimiter> rateLimiterSupplier, Listener listener) {
        super(delegate);
        this.rateLimiterSupplier = rateLimiterSupplier;
        this.listener = listener;
    }

    private void maybePause(int bytes) throws IOException {
        bytesSinceLastRateLimit += bytes;
        final RateLimiter rateLimiter = rateLimiterSupplier.get();
        if (rateLimiter != null) {
            if (bytesSinceLastRateLimit >= rateLimiter.getMinPauseCheckBytes()) {
                long pause = rateLimiter.pause(bytesSinceLastRateLimit);
                bytesSinceLastRateLimit = 0;
                if (pause > 0) {
                    listener.onPause(pause);
                }
            }
        }
    }

    @Override
    public int read() throws IOException {
        int b = super.read();
        maybePause(1);
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = super.read(b, off, len);
        if (n > 0) {
            maybePause(n);
        }
        return n;
    }
}
