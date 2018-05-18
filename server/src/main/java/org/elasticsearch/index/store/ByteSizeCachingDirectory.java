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

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.lucene.store.FilterIndexOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.SingleObjectCache;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.util.concurrent.atomic.AtomicLong;

final class ByteSizeCachingDirectory extends FilterDirectory {

    private static class SizeAndModCount {
        final long size;
        final long modCount;

        SizeAndModCount(long length, long modCount) {
            this.size = length;
            this.modCount = modCount;
        }
    }

    private static long estimateSize(Directory directory) throws IOException {
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

    private final AtomicLong modCount = new AtomicLong();
    private final SingleObjectCache<SizeAndModCount> size;

    ByteSizeCachingDirectory(Directory in, TimeValue refreshInterval) {
        super(in);
        size = new SingleObjectCache<SizeAndModCount>(refreshInterval, new SizeAndModCount(0L, -1L)) {
            @Override
            protected SizeAndModCount refresh() {
                // Compute modCount first so that updates that happen while the size
                // is being computed invalidate the length
                final long modCount = ByteSizeCachingDirectory.this.modCount.get();
                final long size;
                try {
                    size = estimateSize(getDelegate());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return new SizeAndModCount(size, modCount);
            }

            @Override
            protected boolean needsRefresh() {
                if (getNoRefresh().modCount == modCount.get()) {
                    // no updates to the directory since the last refresh
                    return false;
                }
                return super.needsRefresh();
            }
        };
    }

    /** Return the cumulative size of all files in this directory. */
    long estimateSize() throws IOException {
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
        return new FilterIndexOutput(out.toString(), out) {
            @Override
            public void writeBytes(byte[] b, int length) throws IOException {
                super.writeBytes(b, length);
                modCount.incrementAndGet();
            }

            @Override
            public void writeByte(byte b) throws IOException {
                super.writeByte(b);
                modCount.incrementAndGet();
            }

            @Override
            public void close() throws IOException {
                // Close might cause some data to be flushed from in-memory buffers, so
                // increment the modification counter too.
                super.close();
                modCount.incrementAndGet();
            }
        };
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(name);
        modCount.incrementAndGet();
    }

}
