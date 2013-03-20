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
import java.util.Collection;

public final class RateLimitedFSDirectory extends Directory {
    private final FSDirectory delegate;

    private final StoreRateLimiting.Provider rateLimitingProvider;

    private final StoreRateLimiting.Listener rateListener;

    public RateLimitedFSDirectory(FSDirectory wrapped, StoreRateLimiting.Provider rateLimitingProvider,
                                  StoreRateLimiting.Listener rateListener) {
        this.delegate = wrapped;
        this.rateLimitingProvider = rateLimitingProvider;
        this.rateListener = rateListener;
    }

    public FSDirectory wrappedDirectory() {
        return this.delegate;
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return delegate.listAll();
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        ensureOpen();
        return delegate.fileExists(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        ensureOpen();
        delegate.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        ensureOpen();
        return delegate.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        ensureOpen();
        final IndexOutput output = delegate.createOutput(name, context);

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
    public void sync(Collection<String> names) throws IOException {
        ensureOpen();
        delegate.sync(names);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        return delegate.openInput(name, context);
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
        delegate.close();
    }

    @Override
    public IndexInputSlicer createSlicer(String name, IOContext context) throws IOException {
        ensureOpen();
        return delegate.createSlicer(name, context);
    }

    @Override
    public Lock makeLock(String name) {
        ensureOpen();
        return delegate.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        ensureOpen();
        delegate.clearLock(name);
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        ensureOpen();
        delegate.setLockFactory(lockFactory);
    }

    @Override
    public LockFactory getLockFactory() {
        ensureOpen();
        return delegate.getLockFactory();
    }

    @Override
    public String getLockID() {
        ensureOpen();
        return delegate.getLockID();
    }

    @Override
    public String toString() {
        return "RateLimitedDirectoryWrapper(" + delegate.toString() + ")";
    }

    @Override
    public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
        ensureOpen();
        delegate.copy(to, src, dest, context);
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
