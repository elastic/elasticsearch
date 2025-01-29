/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.io.UncheckedIOException;

public final class ByteSizeCachingDirectory extends ByteSizeDirectory {

    private static class SizeAndModCount {
        final long size;
        final long modCount;
        final boolean pendingWrite;

        SizeAndModCount(long length, long modCount, boolean pendingWrite) {
            this.size = length;
            this.modCount = modCount;
            this.pendingWrite = pendingWrite;
        }
    }

    private final SingleObjectCache<SizeAndModCount> size;
    // Both these variables need to be accessed under `this` lock.
    private long modCount = 0;
    private long numOpenOutputs = 0;

    ByteSizeCachingDirectory(Directory in, TimeValue refreshInterval) {
        super(in);
        size = new SingleObjectCache<>(refreshInterval, new SizeAndModCount(0L, -1L, true)) {
            @Override
            protected SizeAndModCount refresh() {
                // It is ok for the size of the directory to be more recent than
                // the mod count, we would just recompute the size of the
                // directory on the next call as well. However the opposite
                // would be bad as we would potentially have a stale cache
                // entry for a long time. So we fetch the values of modCount and
                // numOpenOutputs BEFORE computing the size of the directory.
                final long modCount;
                final boolean pendingWrite;
                synchronized (ByteSizeCachingDirectory.this) {
                    modCount = ByteSizeCachingDirectory.this.modCount;
                    pendingWrite = ByteSizeCachingDirectory.this.numOpenOutputs != 0;
                }
                final long size;
                try {
                    // Compute this OUTSIDE of the lock
                    size = estimateSizeInBytes(getDelegate());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return new SizeAndModCount(size, modCount, pendingWrite);
            }

            @Override
            protected boolean needsRefresh() {
                if (super.needsRefresh() == false) {
                    // The size was computed recently, don't recompute
                    return false;
                }
                SizeAndModCount cached = getNoRefresh();
                if (cached.pendingWrite) {
                    // The cached entry was generated while there were pending
                    // writes, so the size might be stale: recompute.
                    return true;
                }
                synchronized (ByteSizeCachingDirectory.this) {
                    // If there are pending writes or if new files have been
                    // written/deleted since last time: recompute
                    return numOpenOutputs != 0 || cached.modCount != modCount;
                }
            }
        };
    }

    @Override
    public long estimateSizeInBytes() throws IOException {
        try {
            return size.getOrRefresh().size;
        } catch (UncheckedIOException e) {
            // we wrapped in the cache and unwrap here
            throw e.getCause();
        }
    }

    @Override
    public long estimateDataSetSizeInBytes() throws IOException {
        return estimateSizeInBytes(); // data set size is equal to directory size for most implementations
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return wrapIndexOutput(super.createOutput(name, context));
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return wrapIndexOutput(super.createTempOutput(prefix, suffix, context));
    }

    private IndexOutput wrapIndexOutput(IndexOutput out) {
        synchronized (this) {
            numOpenOutputs++;
        }
        return new FilterIndexOutput(out.toString(), out) {
            private boolean closed;

            @Override
            public void writeBytes(byte[] b, int length) throws IOException {
                // Don't write to atomicXXX here since it might be called in
                // tight loops and memory barriers are costly
                super.writeBytes(b, length);
            }

            @Override
            public void writeByte(byte b) throws IOException {
                // Don't write to atomicXXX here since it might be called in
                // tight loops and memory barriers are costly
                super.writeByte(b);
            }

            @Override
            public void writeInt(int i) throws IOException {
                // Delegate primitive for possible performance enhancement
                out.writeInt(i);
            }

            @Override
            public void writeShort(short s) throws IOException {
                // Delegate primitive for possible performance enhancement
                out.writeShort(s);
            }

            @Override
            public void writeLong(long l) throws IOException {
                // Delegate primitive for possible performance enhancement
                out.writeLong(l);
            }

            @Override
            public void close() throws IOException {
                // Close might cause some data to be flushed from in-memory buffers, so
                // increment the modification counter too.
                try {
                    super.close();
                } finally {
                    synchronized (ByteSizeCachingDirectory.this) {
                        if (closed == false) {
                            closed = true;
                            numOpenOutputs--;
                            modCount++;
                        }
                    }
                }
            }
        };
    }

    @Override
    public void deleteFile(String name) throws IOException {
        try {
            super.deleteFile(name);
        } finally {
            markEstimatedSizeAsStale();
        }
    }

    /**
     * Mark the cached size as stale so that it is guaranteed to be refreshed the next time.
     */
    public void markEstimatedSizeAsStale() {
        synchronized (this) {
            modCount++;
        }
    }

    public static ByteSizeCachingDirectory unwrapDirectory(Directory dir) {
        while (dir != null) {
            if (dir instanceof ByteSizeCachingDirectory) {
                return (ByteSizeCachingDirectory) dir;
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        return null;
    }
}
