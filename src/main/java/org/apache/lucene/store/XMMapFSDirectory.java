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

import org.apache.lucene.index.TrackingMergeScheduler;
import org.elasticsearch.common.RateLimiter;

import java.io.File;
import java.io.IOException;

/**
 */
public class XMMapFSDirectory extends NIOFSDirectory {

    private final StoreRateLimiting.Provider rateLimitingProvider;

    private final StoreRateLimiting.Listener rateListener;

    public XMMapFSDirectory(File path, LockFactory lockFactory, StoreRateLimiting.Provider rateLimitingProvider, StoreRateLimiting.Listener rateListener) throws IOException {
        super(path, lockFactory);
        this.rateLimitingProvider = rateLimitingProvider;
        this.rateListener = rateListener;
    }

    @Override
    public IndexOutput createOutput(String name) throws IOException {
        StoreRateLimiting rateLimiting = rateLimitingProvider.rateLimiting();
        StoreRateLimiting.Type type = rateLimiting.getType();
        RateLimiter limiter = rateLimiting.getRateLimiter();
        if (type == StoreRateLimiting.Type.NONE || limiter == null) {
            return super.createOutput(name);
        }
        if (TrackingMergeScheduler.getCurrentMerge() != null) {
            // we are mering, and type is either MERGE or ALL, rate limit...
            ensureOpen();
            ensureCanWrite(name);
            return new XFSIndexOutput(this, name, limiter, rateListener);
        }
        if (type == StoreRateLimiting.Type.ALL) {
            ensureOpen();
            ensureCanWrite(name);
            return new XFSIndexOutput(this, name, limiter, rateListener);
        }
        // we shouldn't really get here...
        return super.createOutput(name);
    }
}
