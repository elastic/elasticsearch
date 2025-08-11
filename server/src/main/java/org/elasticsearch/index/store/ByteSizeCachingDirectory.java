/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.common.util.SingleObjectCache;
import org.elasticsearch.core.TimeValue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.util.Set;

final class ByteSizeCachingDirectory extends FilterDirectory {

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

    private static long estimateSizeInBytes(Directory directory) throws IOException {
        long estimatedSize = 0;
        String[] files = directory.listAll();
        for (String file : files) {
            try {
                estimatedSize += directory.fileLength(file);
            } catch (NoSuchFileException | FileNotFoundException | AccessDeniedException e) {
                // ignore, the file is not there no more; on Windows, if one thread concurrently deletes a file while
                // calling Files.size, you can also sometimes hit AccessDeniedException
            }
        }
        return estimatedSize;
    }

    private final SingleObjectCache<SizeAndModCount> size;
    // Both these variables need to be accessed under `this` lock.
    private long modCount = 0;
    private long numOpenOutputs = 0;

    ByteSizeCachingDirectory(Directory in, TimeValue refreshInterval) {
        super(in);
        size = new SingleObjectCache<SizeAndModCount>(refreshInterval, new SizeAndModCount(0L, -1L, true)) {
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

    /** Return the cumulative size of all files in this directory. */
    long estimateSizeInBytes() throws IOException {
        try {
            return size.getOrRefresh().size;
        } catch (UncheckedIOException e) {
            // we wrapped in the cache and unwrap here
            throw e.getCause();
        }
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
            synchronized (this) {
                modCount++;
            }
        }
    }

    // temporary override until LUCENE-8735 is integrated
    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return in.getPendingDeletions();
    }
}
