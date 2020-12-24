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
