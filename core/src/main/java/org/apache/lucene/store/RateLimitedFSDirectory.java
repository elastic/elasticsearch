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
package org.apache.lucene.store;

import org.apache.lucene.store.IOContext.Context;

import java.io.IOException;

public final class RateLimitedFSDirectory extends FilterDirectory {

    private final StoreRateLimiting.Provider rateLimitingProvider;

    private final StoreRateLimiting.Listener rateListener;

    public RateLimitedFSDirectory(Directory wrapped, StoreRateLimiting.Provider rateLimitingProvider,
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
        if (context.context == Context.MERGE || type == StoreRateLimiting.Type.ALL) {
            // we are merging, and type is either MERGE or ALL, rate limit...
            return new RateLimitedIndexOutput(new RateLimiterWrapper(limiter, rateListener), output);
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
            return "rate_limited(" + StoreUtils.toString(in) + ", type=" + type.name() + ", rate=" + limiter.getMBPerSec() + ")";
        }
    }

    // we wrap the limiter to notify our store if we limited to get statistics
    static final class RateLimiterWrapper extends RateLimiter {
        private final RateLimiter delegate;
        private final StoreRateLimiting.Listener rateListener;

        RateLimiterWrapper(RateLimiter delegate, StoreRateLimiting.Listener rateListener) {
            this.delegate = delegate;
            this.rateListener = rateListener;
        }

        @Override
        public void setMBPerSec(double mbPerSec) {
            delegate.setMBPerSec(mbPerSec);
        }

        @Override
        public double getMBPerSec() {
            return delegate.getMBPerSec();
        }

        @Override
        public long pause(long bytes) throws IOException {
            long pause = delegate.pause(bytes);
            rateListener.onPause(pause);
            return pause;
        }

        @Override
        public long getMinPauseCheckBytes() {
            return delegate.getMinPauseCheckBytes();
        }
    }
}
