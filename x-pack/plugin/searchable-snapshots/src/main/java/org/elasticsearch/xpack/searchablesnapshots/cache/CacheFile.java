/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.concurrent.RefCounted;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Objects;
import java.util.Set;

class CacheFile implements RefCounted {

    private final Set<StandardOpenOption> options;
    private final SparseFileTracker tracker;
    private final int sizeOfRange;
    private final String name;
    private final Path file;

    private volatile FileChannel channel;
    private volatile boolean evicted;
    private volatile int refCount;

    CacheFile(String name, long length, Path file, Set<StandardOpenOption> options, int sizeOfRange) {
        this.tracker = new SparseFileTracker(file.toString(), length);
        this.options = Objects.requireNonNull(options);
        this.name = Objects.requireNonNull(name);
        this.file = Objects.requireNonNull(file);
        this.sizeOfRange = sizeOfRange;
        this.evicted = false;
        this.refCount = 1;
    }

    public String getName() {
        return name;
    }

    public long getLength() {
        return tracker.getLength();
    }

    public synchronized void markAsEvicted() {
        if (evicted == false) {
            evicted = true;
            decRef();
        }
    }

    @Override
    public void incRef() {
        if (tryIncRef() == false) {
            throw new IllegalStateException("Failed to increment reference counter, cache entry is already evicted");
        }
    }

    @Override
    public synchronized boolean tryIncRef() {
        assert evicted || refCount > 0;
        if (evicted) {
            return false;
        }
        if (refCount == 1) {
            assert channel == null || channel.isOpen() == false;
            try {
                channel = FileChannel.open(file, options);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        refCount += 1;
        return true;
    }

    @Override
    public synchronized void decRef() {
        assert refCount > 0;
        refCount -= 1;
        if (refCount == 1){
            assert channel != null;
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (refCount == 0) {
            assert evicted;
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    synchronized int refCount() {
        return refCount;
    }

    @Override
    public String toString() {
        return "CacheFile{" +
            "name='" + name + '\'' +
            ", length=" + tracker.getLength() +
            ", evicted=" + evicted +
            ", refCount=" + refCount +
            ", channel=" + (channel == null ? "no" : (channel.isOpen() ? "open" : "closed")) +
            ", tracker=" + tracker +
            '}';
    }

    /**
     * Computes the start and the end of a range to which the given {@code position} belongs.
     *
     * @param position the reading position
     * @return the start and end range positions to fetch
     */
    public Tuple<Long, Long> computeRange(final long position) {
        final long start = (position / sizeOfRange) * sizeOfRange;
        return Tuple.tuple(start, Math.min(start + sizeOfRange, tracker.getLength()));
    }

    public List<SparseFileTracker.Gap> waitForRange(final long start, final long end, final ActionListener<Void> listener) {
        return tracker.waitForRange(start, end, listener);
    }

    /**
     * Read from the cache file on disk into a byte buffer, starting at a certain position.
     */
    int readFrom(final ByteBuffer dst, final long position) throws IOException {
        incRef();
        try {
            return Channels.readFromFileChannel(channel, position, dst);
        } finally {
            decRef();
        }
    }

    /**
     * Write a sequence of bytes to the cache file on disk from the given buffer, starting at the given file position.v
     */
    @SuppressForbidden(reason = "Use Channel positional writes")
    int writeTo(final ByteBuffer src, final long position) throws IOException {
        incRef();
        try {
            return channel.write(src, position);
        } finally {
            decRef();
        }
    }
}
