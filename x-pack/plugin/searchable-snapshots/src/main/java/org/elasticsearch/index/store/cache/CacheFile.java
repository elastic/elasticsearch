/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CacheFile {

    @FunctionalInterface
    public interface EvictionListener {
        void onEviction(CacheFile evictedCacheFile);
    }

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[] {
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE,
        StandardOpenOption.SPARSE };

    private final AbstractRefCounted refCounter = new AbstractRefCounted("CacheFile") {
        @Override
        protected void closeInternal() {
            CacheFile.this.finishEviction();
        }
    };

    private final ReentrantReadWriteLock.WriteLock evictionLock;
    private final ReentrantReadWriteLock.ReadLock readLock;

    private final SparseFileTracker tracker;
    private final String description;
    private final Path file;

    private volatile Set<EvictionListener> listeners;
    private volatile boolean evicted;

    @Nullable // if evicted, or there are no listeners
    private volatile FileChannel channel;

    public CacheFile(String description, long length, Path file) {
        this.tracker = new SparseFileTracker(file.toString(), length);
        this.description = Objects.requireNonNull(description);
        this.file = Objects.requireNonNull(file);
        this.listeners = new HashSet<>();
        this.evicted = false;

        final ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock();
        this.evictionLock = cacheLock.writeLock();
        this.readLock = cacheLock.readLock();

        assert invariant();
    }

    public long getLength() {
        return tracker.getLength();
    }

    public Path getFile() {
        return file;
    }

    Releasable fileLock() {
        boolean success = false;
        readLock.lock();
        try {
            ensureOpen();
            // check if we have a channel while holding the read lock
            if (channel == null) {
                throw new AlreadyClosedException("Cache file channel has been released and closed");
            }
            success = true;
            return readLock::unlock;
        } finally {
            if (success == false) {
                readLock.unlock();
            }
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
            evictionLock.lock();
            try {
                ensureOpen();
                final Set<EvictionListener> newListeners = new HashSet<>(listeners);
                final boolean added = newListeners.add(listener);
                assert added : "listener already exists " + listener;
                maybeOpenFileChannel(newListeners);
                listeners = Collections.unmodifiableSet(newListeners);
                success = true;
            } finally {
                try {
                    if (success == false) {
                        refCounter.decRef();
                    }
                } finally {
                    evictionLock.unlock();
                }
            }
        }
        assert invariant();
        return success;
    }

    public boolean release(final EvictionListener listener) {
        assert listener != null;

        boolean success = false;
        evictionLock.lock();
        try {
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
        } finally {
            evictionLock.unlock();
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
            evictionLock.lock();
            try {
                if (evicted == false) {
                    evicted = true;
                    evictionListeners.addAll(listeners);
                    refCounter.decRef();
                }
            } finally {
                evictionLock.unlock();
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
        readLock.lock();
        try {
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
        } finally {
            readLock.unlock();
        }
        return true;
    }

    @Override
    public String toString() {
        return "CacheFile{"
            + "desc='"
            + description
            + "', file="
            + file
            + ", length="
            + tracker.getLength()
            + ", channel="
            + (channel != null ? "yes" : "no")
            + ", listeners="
            + listeners.size()
            + ", evicted="
            + evicted
            + ", tracker="
            + tracker
            + '}';
    }

    private void ensureOpen() {
        if (evicted) {
            throw new AlreadyClosedException("Cache file is evicted");
        }
    }

    CompletableFuture<Integer> fetchRange(
        long start,
        long end,
        CheckedBiFunction<Long, Long, Integer, IOException> onRangeAvailable,
        CheckedBiConsumer<Long, Long, IOException> onRangeMissing
    ) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        try {
            if (start < 0 || start > tracker.getLength() || start > end || end > tracker.getLength()) {
                throw new IllegalArgumentException(
                    "Invalid range [start=" + start + ", end=" + end + "] for length [" + tracker.getLength() + ']'
                );
            }
            ensureOpen();
            final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(
                start,
                end,
                ActionListener.wrap(
                    rangeReady -> future.complete(onRangeAvailable.apply(start, end)),
                    rangeFailure -> future.completeExceptionally(rangeFailure)
                )
            );

            for (SparseFileTracker.Gap gap : gaps) {
                try {
                    ensureOpen();
                    onRangeMissing.accept(gap.start, gap.end);
                    gap.onResponse(null);
                } catch (Exception e) {
                    gap.onFailure(e);
                }
            }
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    public Tuple<Long, Long> getAbsentRangeWithin(long start, long end) {
        ensureOpen();
        return tracker.getAbsentRangeWithin(start, end);
    }
}
