/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

class CacheFile implements Releasable {

    @FunctionalInterface
    public interface EvictionListener {
        void onEviction(CacheFile evictedCacheFile);
    }

    private static final int RANGE_SIZE = 1 << 15;

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[]{
        StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SPARSE
    };

    private final AbstractRefCounted refCounter = new AbstractRefCounted("CacheFile") {
        @Override
        protected void closeInternal() {
            CacheFile.this.closeInternal();
        }
    };

    private final SparseFileTracker tracker;
    private final String name;
    private final Path file;

    private volatile FileChannelRefCounted channelRefCounter;
    private volatile Set<EvictionListener> listeners;
    private volatile boolean closed;

    CacheFile(String name, long length, Path file) {
        this.tracker = new SparseFileTracker(file.toString(), length);
        this.name = Objects.requireNonNull(name);
        this.file = Objects.requireNonNull(file);
        this.listeners = new HashSet<>();
        this.closed = false;
        assert invariant();
    }

    public String getName() {
        return name;
    }

    public long getLength() {
        return tracker.getLength();
    }

    public Path getFile() {
        return file;
    }

    @Nullable
    public FileChannelRefCounted getChannelRefCounter() {
        return channelRefCounter;
    }

    public boolean acquire(final EvictionListener listener) throws IOException {
        ensureOpen();
        boolean success = false;
        if (refCounter.tryIncRef()) {
            synchronized (this) {
                try {
                    ensureOpen();
                    final Set<EvictionListener> newListeners = new HashSet<>(listeners);
                    if (newListeners.add(Objects.requireNonNull(listener)) == false) {
                        throw new IllegalStateException("Cannot add the same listener twice");
                    }
                    maybeOpenFileChannel(newListeners);
                    listeners = Collections.unmodifiableSet(newListeners);
                    success = true;
                } finally {
                    if (success == false) {
                        refCounter.decRef();
                    }
                }
            }
        }
        assert invariant();
        return success;
    }

    public boolean release(final EvictionListener listener) {
        boolean success = false;
        synchronized (this) {
            try {
                final Set<EvictionListener> newListeners = new HashSet<>(listeners);
                boolean removed = newListeners.remove(Objects.requireNonNull(listener));
                maybeCloseFileChannel(newListeners);
                listeners = Collections.unmodifiableSet(newListeners);
                success = removed;
            } finally {
                if (success) {
                    refCounter.decRef();
                }
            }
        }
        assert invariant();
        return success;
    }

    private void closeInternal() {
        assert Thread.holdsLock(this);
        if (channelRefCounter != null) {
            channelRefCounter.deleteAfterClose();
        } else {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public void close() {
        if (closed == false) {
            final Set<EvictionListener> evictionListeners = new HashSet<>();
            synchronized (this) {
                if (closed == false) {
                    closed = true;
                    evictionListeners.addAll(listeners);
                    refCounter.decRef();
                }
            }
            if (evictionListeners != null) {
                evictionListeners.forEach(listener -> listener.onEviction(this));
            }
        }
        assert invariant();
    }

    private void maybeOpenFileChannel(Set<EvictionListener> listeners) throws IOException {
        assert Thread.holdsLock(this);
        if (listeners.size() == 1) {
            assert channelRefCounter == null;
            channelRefCounter = new FileChannelRefCounted(file);
        }
    }

    private void maybeCloseFileChannel(Set<EvictionListener> listeners) {
        assert Thread.holdsLock(this);
        if (listeners.size() == 0) {
            final FileChannelRefCounted oldFileChannel = channelRefCounter;
            channelRefCounter = null;
            if (oldFileChannel != null) {
                oldFileChannel.decRef();
            }
        }
    }

    private boolean invariant() {
        assert listeners != null;
        if (listeners.isEmpty()) {
            assert channelRefCounter == null;
        } else {
            assert channelRefCounter != null;
            assert channelRefCounter.refCount() > 0;
            assert channelRefCounter.channel != null;
            assert channelRefCounter.channel.isOpen();
        }
        return true;
    }

    @Override
    public String toString() {
        return "CacheFile{" +
            "name='" + name + '\'' +
            ", file=" + file +
            ", length=" + tracker.getLength() +
            ", channel=" + (channelRefCounter != null ? channelRefCounter : "null") +
            ", tracker=" + tracker +
            '}';
    }

    private void ensureOpen() {
        if (closed) {
            throw new AlreadyClosedException("Cache file is closed");
        }
    }

    CompletableFuture<Integer> fetchRange(long position,
                                          CheckedBiFunction<Long, Long, Integer, IOException> onRangeAvailable,
                                          CheckedBiFunction<Long, Long, Integer, IOException> onRangeMissing) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            if (position < 0 || position > tracker.getLength()) {
                throw new IllegalArgumentException("Wrong read position [" + position + "]");
            }

            ensureOpen();
            final long rangeStart = (position / RANGE_SIZE) * RANGE_SIZE;
            final long rangeEnd = Math.min(rangeStart + RANGE_SIZE, tracker.getLength());

            final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(rangeStart, rangeEnd,
                ActionListener.wrap(
                    rangeReady -> future.complete(onRangeAvailable.apply(rangeStart, rangeEnd)),
                    rangeFailure -> future.completeExceptionally(rangeFailure)));

            if (gaps.size() > 0) {
                final SparseFileTracker.Gap range = gaps.get(0);
                assert gaps.size() == 1 : "expected 1 range to fetch but got " + gaps.size();
                assert range.start == rangeStart
                    : "range/gap start mismatch (" + range.start + ',' + rangeStart + ')';
                assert range.end == rangeEnd
                    : "range/gap end mismatch (" + range.end + ',' + rangeEnd + ')';

                try {
                    ensureOpen();
                    onRangeMissing.apply(rangeStart, rangeEnd);
                    range.onResponse(null);
                } catch (Exception e) {
                    range.onFailure(e);
                }
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    public static class FileChannelRefCounted extends AbstractRefCounted {

        private final AtomicBoolean deleteAfterClose;
        private final FileChannel channel;
        private final Path file;

        private FileChannelRefCounted(final Path file) throws IOException {
            super("FileChannelRefCounted(" + file + ")");
            this.channel = FileChannel.open(file, OPEN_OPTIONS);
            this.deleteAfterClose = new AtomicBoolean(false);
            this.file = Objects.requireNonNull(file);
        }

        FileChannel getChannel() {
            return channel;
        }

        @Override
        protected void closeInternal() {
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Exception when closing channel", e);
            }
            if (deleteAfterClose.get()) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    throw new UncheckedIOException("Exception when deleting file", e);
                }
            }
        }

        @Override
        public String toString() {
            return getName() +
                "[refcount=" + refCount() +
                ", channel=" + (channel.isOpen() ? "open" : "closed") +
                ']';
        }

        void deleteAfterClose() {
            final boolean delete = deleteAfterClose.compareAndSet(false, true);
            assert delete : "delete after close flag is already set";
        }
    }
}
