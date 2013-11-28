/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.apache.lucene.store;

import org.apache.lucene.store.IOContext.Context;

import java.io.IOException;

public final class RateLimitedFSDirectory extends FilterDirectory{

    private final StoreRateLimiting.Provider rateLimitingProvider;

    private final StoreRateLimiting.Listener rateListener;

    public RateLimitedFSDirectory(FSDirectory wrapped, StoreRateLimiting.Provider rateLimitingProvider,
                                  StoreRateLimiting.Listener rateListener) {
        super(wrapped);
        this.rateLimitingProvider = rateLimitingProvider;
        this.rateListener = rateListener;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        final IndexOutput output = in.createOutput(name, context);

        StoreRateLimiting rateLimiting = rateLimitingProvider.rateLimiting();
        StoreRateLimiting.Type type = rateLimiting.getType();
        RateLimiter limiter = rateLimiting.getRateLimiter();
        if (type == StoreRateLimiting.Type.NONE || limiter == null) {
            return output;
        }
        if (context.context == Context.MERGE) {
            // we are mering, and type is either MERGE or ALL, rate limit...
            return new RateLimitedIndexOutput(limiter, rateListener, output);
        }
        if (type == StoreRateLimiting.Type.ALL) {
            return new RateLimitedIndexOutput(limiter, rateListener, output);
        }
        // we shouldn't really get here...
        return output;
    }


    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public String toString() {
        StoreRateLimiting rateLimiting = rateLimitingProvider.rateLimiting();
        StoreRateLimiting.Type type = rateLimiting.getType();
        RateLimiter limiter = rateLimiting.getRateLimiter();
        if (type == StoreRateLimiting.Type.NONE || limiter == null) {
            return StoreUtils.toString(in);
        } else {
            return "rate_limited(" + StoreUtils.toString(in) + ", type=" + type.name() + ", rate=" + limiter.getMbPerSec() + ")";
        }
    }


    static final class RateLimitedIndexOutput extends BufferedIndexOutput {

        private final IndexOutput delegate;
        private final BufferedIndexOutput bufferedDelegate;
        private final RateLimiter rateLimiter;
        private final StoreRateLimiting.Listener rateListener;

        RateLimitedIndexOutput(final RateLimiter rateLimiter, final StoreRateLimiting.Listener rateListener, final IndexOutput delegate) {
            // TODO if Lucene exposed in BufferedIndexOutput#getBufferSize, we could initialize it if the delegate is buffered
            if (delegate instanceof BufferedIndexOutput) {
                bufferedDelegate = (BufferedIndexOutput) delegate;
                this.delegate = delegate;
            } else {
                this.delegate = delegate;
                bufferedDelegate = null;
            }
            this.rateLimiter = rateLimiter;
            this.rateListener = rateListener;
        }

        @Override
        protected void flushBuffer(byte[] b, int offset, int len) throws IOException {
            rateListener.onPause(rateLimiter.pause(len));
            if (bufferedDelegate != null) {
                bufferedDelegate.flushBuffer(b, offset, len);
            } else {
                delegate.writeBytes(b, offset, len);
            }

        }

        @Override
        public long length() throws IOException {
            return delegate.length();
        }

        @Override
        public void seek(long pos) throws IOException {
            flush();
            delegate.seek(pos);
        }

        @Override
        public void flush() throws IOException {
            try {
                super.flush();
            } finally {
                delegate.flush();
            }
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                delegate.close();
            }
        }
    }
}
