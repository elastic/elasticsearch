/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.ReleasableLock;

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
import java.util.concurrent.locks.ReentrantReadWriteLock;

class CacheFile {

    @FunctionalInterface
    public interface EvictionListener {
        void onEviction(CacheFile evictedCacheFile);
    }

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[]{
        StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.SPARSE
    };

    private final AbstractRefCounted refCounter = new AbstractRefCounted("CacheFile") {
        @Override
        protected void closeInternal() {
            CacheFile.this.finishEviction();
        }
    };

    private final ReleasableLock evictionLock;
    private final ReleasableLock readLock;

    private final SparseFileTracker tracker;
    private final int rangeSize;
    private final String description;
    private final Path file;

    private volatile Set<EvictionListener> listeners;
    private volatile boolean evicted;

    @Nullable // if evicted, or there are no listeners
    private volatile FileChannel channel;

    CacheFile(String description, long length, Path file, int rangeSize) {
        this.tracker = new SparseFileTracker(file.toString(), length);
        this.description = Objects.requireNonNull(description);
        this.file = Objects.requireNonNull(file);
        this.listeners = new HashSet<>();
        this.rangeSize = rangeSize;
        this.evicted = false;

        final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
        this.evictionLock = new ReleasableLock(cacheLock.writeLock());
        this.readLock = new ReleasableLock(cacheLock.readLock());

        assert invariant();
    }

    public long getLength() {
        return tracker.getLength();
    }

    public Path getFile() {
        return file;
    }

    public ReleasableLock fileLock() {
        try (ReleasableLock ignored = evictionLock.acquire()) {
            ensureOpen();
            // check if we have a channel under eviction lock
            if (channel == null) {
                throw new AlreadyClosedException("Cache file channel has been released and closed");
            }
            // acquire next read lock while holding the eviction lock
            // makes sure that channel won't be closed until this
            // read lock is released
            return readLock.acquire();
        }
    }

    @Nullable
    public FileChannel getChannel() {
        return channel;
    }

    public boolean acquire(final EvictionListener listener) throws IOException {
        assert listener != null;

        ensureOpen();
        boolean success = false;
        if (refCounter.tryIncRef()) {
            try (ReleasableLock ignored = evictionLock.acquire()) {
                try {
                    ensureOpen();
                    final Set<EvictionListener> newListeners = new HashSet<>(listeners);
                    final boolean added = newListeners.add(listener);
                    assert added : "listener already exists " + listener;
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
        assert listener != null;

        boolean success = false;
        try (ReleasableLock ignored = evictionLock.acquire()) {
            try {
                final Set<EvictionListener> newListeners = new HashSet<>(listeners);
                final boolean removed = newListeners.remove(Objects.requireNonNull(listener));
                assert removed : "listener does not exist " + listener;
                if (removed == false) {
                    throw new IllegalStateException("Cannot remove an unknown listener");
                }
                maybeCloseFileChannel(newListeners);
                listeners = Collections.unmodifiableSet(newListeners);
                success = true;
            } finally {
                if (success) {
                    refCounter.decRef();
                }
            }
        }
        assert invariant();
        return success;
    }

    private void finishEviction() {
        assert evictionLock.isHeldByCurrentThread();
        assert listeners.isEmpty();
        assert channel == null;
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void startEviction() {
        if (evicted == false) {
            final Set<EvictionListener> evictionListeners = new HashSet<>();
            try (ReleasableLock ignored = evictionLock.acquire()) {
                if (evicted == false) {
                    evicted = true;
                    evictionListeners.addAll(listeners);
                    refCounter.decRef();
                }
            }
            evictionListeners.forEach(listener -> listener.onEviction(this));
        }
        assert invariant();
    }

    private void maybeOpenFileChannel(Set<EvictionListener> listeners) throws IOException {
        assert evictionLock.isHeldByCurrentThread();
        if (listeners.size() == 1) {
            assert channel == null;
            channel = FileChannel.open(file, OPEN_OPTIONS);
        }
    }

    private void maybeCloseFileChannel(Set<EvictionListener> listeners) {
        assert evictionLock.isHeldByCurrentThread();
        if (listeners.size() == 0) {
            assert channel != null;
            try {
                channel.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Exception when closing channel", e);
            } finally {
                channel = null;
            }
        }
    }

    private boolean invariant() {
        try (ReleasableLock ignored = readLock.acquire()) {
            assert listeners != null;
            if (listeners.isEmpty()) {
                assert channel == null;
                assert evicted == false || refCounter.refCount() != 0 || Files.notExists(file);
            } else {
                assert channel != null;
                assert refCounter.refCount() > 0;
                assert channel.isOpen();
                assert Files.exists(file);
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "CacheFile{" +
            "desc='" + description + '\'' +
            ", file=" + file +
            ", length=" + tracker.getLength() +
            ", channel=" + (channel != null ? "yes" : "no") +
            ", listeners=" + listeners.size() +
            ", evicted=" + evicted +
            ", tracker=" + tracker +
            '}';
    }

    private void ensureOpen() {
        if (evicted) {
            throw new AlreadyClosedException("Cache file is evicted");
        }
    }

    CompletableFuture<Integer> fetchRange(long position,
                                          CheckedBiFunction<Long, Long, Integer, IOException> onRangeAvailable,
                                          CheckedBiConsumer<Long, Long, IOException> onRangeMissing) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            if (position < 0 || position > tracker.getLength()) {
                throw new IllegalArgumentException("Wrong read position [" + position + "]");
            }

            ensureOpen();
            final long rangeStart = (position / rangeSize) * rangeSize;
            final long rangeEnd = Math.min(rangeStart + rangeSize, tracker.getLength());

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
                    onRangeMissing.accept(rangeStart, rangeEnd);
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
}
