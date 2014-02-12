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

import java.io.IOException;
import java.io.InputStream;

/**
 * Rate limiting wrapper for InputStream
 */
public class RateLimitingInputStream extends InputStream {

    private final InputStream delegate;

    private final RateLimiter rateLimiter;

    private final Listener listener;

    public interface Listener {
        void onPause(long nanos);
    }

    public RateLimitingInputStream(InputStream delegate, RateLimiter rateLimiter, Listener listener) {
        this.delegate = delegate;
        this.rateLimiter = rateLimiter;
        this.listener = listener;
    }

    @Override
    public int read() throws IOException {
        int b = delegate.read();
        long pause = rateLimiter.pause(1);
        if (pause > 0) {
            listener.onPause(pause);
        }
        return b;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int n = delegate.read(b, off, len);
        if (n > 0) {
            listener.onPause(rateLimiter.pause(n));
        }
        return n;
    }

    @Override
    public long skip(long n) throws IOException {
        return delegate.skip(n);
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }
}
