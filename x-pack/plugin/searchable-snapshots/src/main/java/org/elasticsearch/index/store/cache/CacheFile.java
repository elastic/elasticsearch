/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.concurrent.AbstractRefCounted;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.internal.io.IOUtils;

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
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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

    /**
     * Reference counter that counts the number of eviction listeners referencing this cache file plus the number of open file channels
     * for it. Once this instance has been evicted, all listeners notified and all {@link FileChannelReference} for it released,
     * it makes sure to delete the physical file backing this cache.
     */
    private final AbstractRefCounted refCounter = new AbstractRefCounted("CacheFile") {
        @Override
        protected void closeInternal() {
            assert evicted.get();
            assert assertNoPendingListeners();
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    };

    private final SparseFileTracker tracker;
    private final String description;
    private final Path file;

    private final Set<EvictionListener> listeners = new HashSet<>();

    /**
     * Indicates whether the cache file requires to be synchronized with the storage device that contains it in order to persist in a
     * durable manner its ranges of cached data. An empty cache file does not need to be fsync; and writing new data to the cache file
     * will toggle the flag to {@code true}.
     **/
    private final AtomicBoolean needsFsync = new AtomicBoolean();

    /**
     * A runnable that is executed every time the {@link #needsFsync} flag is toggled to {@code true}, which indicates that the cache file
     * has been updated. See {@link #markAsNeedsFSync()} method.
     */
    private final Runnable needsFsyncRunnable;

    /**
     * A reference counted holder for the current channel to the physical file backing this cache file instance.
     * By guarding access to the file channel by ref-counting and giving the channel its own life-cycle we remove all need for
     * locking when dealing with file-channel closing and opening as this file is referenced and de-referenced via {@link #acquire}
     * and {@link #release}.
     * Background operations running for index inputs that get closed concurrently are tied to a specific instance of this reference and
     * will simply fail once all references to the channel have been released since they won't be able to acquire a reference to the
     * channel again.
     * Each instance of this class also increments the count in {@link #refCounter} by one when instantiated and decrements it by one
     * again when it is closed. This is done to ensure that the file backing this cache file instance is only deleted after all channels
     * to it have been closed.
     */
    private final class FileChannelReference extends AbstractRefCounted {

        private final FileChannel fileChannel;

        FileChannelReference() throws IOException {
            super("FileChannel[" + file + "]");
            this.fileChannel = FileChannel.open(file, OPEN_OPTIONS);
            refCounter.incRef();
        }

        @Override
        protected void closeInternal() {
            try {
                fileChannel.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                decrementRefCount();
            }
        }
    }

    // If true this file has been evicted from the cache and should not be used any more
    private final AtomicBoolean evicted = new AtomicBoolean(false);

    @Nullable
    private volatile FileChannelReference channelRef;

    public CacheFile(String description, long length, Path file, Runnable onNeedFSync) {
        this.tracker = new SparseFileTracker(file.toString(), length);
        this.description = Objects.requireNonNull(description);
        this.file = Objects.requireNonNull(file);
        this.needsFsyncRunnable = Objects.requireNonNull(onNeedFSync);
        assert invariant();
    }

    public long getLength() {
        return tracker.getLength();
    }

    public Path getFile() {
        return file;
    }

    // Only used in tests
    @Nullable
    FileChannel getChannel() {
        final FileChannelReference reference = channelRef;
        return reference == null ? null : reference.fileChannel;
    }

    public void acquire(final EvictionListener listener) throws IOException {
        assert listener != null;

        ensureOpen();
        boolean success = false;
        if (refCounter.tryIncRef()) {
            try {
                synchronized (listeners) {
                    ensureOpen();
                    if (listeners.isEmpty()) {
                        assert channelRef == null;
                        channelRef = new FileChannelReference();
                    }
                    final boolean added = listeners.add(listener);
                    assert added : "listener already exists " + listener;
                }
                success = true;
            } finally {
                if (success == false) {
                    decrementRefCount();
                }
            }
        } else {
            assert evicted.get();
            throwAlreadyEvicted();
        }
        assert invariant();
    }

    public void release(final EvictionListener listener) {
        assert listener != null;

        boolean success = false;
        try {
            synchronized (listeners) {
                final boolean removed = listeners.remove(Objects.requireNonNull(listener));
                assert removed : "listener does not exist " + listener;
                if (removed == false) {
                    throw new IllegalStateException("Cannot remove an unknown listener");
                }
                if (listeners.isEmpty()) {
                    // nobody is using this file so we close the channel
                    channelRef.decRef();
                    channelRef = null;
                }
            }
            success = true;
        } finally {
            if (success) {
                decrementRefCount();
            }
        }
        assert invariant();
    }

    private boolean assertNoPendingListeners() {
        synchronized (listeners) {
            assert listeners.isEmpty();
            assert channelRef == null;
        }
        return true;
    }

    private void decrementRefCount() {
        final boolean released = refCounter.decRef();
        assert released == false || (evicted.get() && Files.notExists(file));
    }

    /**
     * Evicts this file from the cache. Once this method has been called, subsequent use of this class with throw exceptions.
     */
    public void startEviction() {
        if (evicted.compareAndSet(false, true)) {
            final Set<EvictionListener> evictionListeners;
            synchronized (listeners) {
                evictionListeners = new HashSet<>(listeners);
            }
            decrementRefCount();
            evictionListeners.forEach(listener -> listener.onEviction(this));
        }
        assert invariant();
    }

    private boolean invariant() {
        synchronized (listeners) {
            if (listeners.isEmpty()) {
                assert channelRef == null;
            } else {
                assert channelRef != null;
                assert refCounter.refCount() > 0;
                assert channelRef.refCount() > 0;
                assert Files.exists(file);
            }
        }
        return true;
    }

    @Override
    public String toString() {
        synchronized (listeners) {
            return "CacheFile{"
                + "desc='"
                + description
                + "', file="
                + file
                + ", length="
                + tracker.getLength()
                + ", channel="
                + (channelRef != null ? "yes" : "no")
                + ", listeners="
                + listeners.size()
                + ", evicted="
                + evicted
                + ", tracker="
                + tracker
                + '}';
        }
    }

    private void ensureOpen() {
        if (evicted.get()) {
            throwAlreadyEvicted();
        }
    }

    private static void throwAlreadyEvicted() {
        throw new AlreadyClosedException("Cache file is evicted");
    }

    @FunctionalInterface
    interface RangeAvailableHandler {
        int onRangeAvailable(FileChannel channel) throws IOException;
    }

    @FunctionalInterface
    interface RangeMissingHandler {
        void fillCacheRange(FileChannel channel, long from, long to, Consumer<Long> progressUpdater) throws IOException;
    }

    /**
     * Populates any missing ranges within {@code rangeToWrite} using the {@link RangeMissingHandler}, and notifies the
     * {@link RangeAvailableHandler} when {@code rangeToRead} is available to read from the file. If {@code rangeToRead} is already
     * available then the {@link RangeAvailableHandler} is called synchronously by this method; if not then the given {@link Executor}
     * processes the missing ranges and notifies the {@link RangeAvailableHandler}.
     *
     * @return a future which returns the result of the {@link RangeAvailableHandler} once it has completed.
     */
    Future<Integer> populateAndRead(
        final Tuple<Long, Long> rangeToWrite,
        final Tuple<Long, Long> rangeToRead,
        final RangeAvailableHandler reader,
        final RangeMissingHandler writer,
        final Executor executor
    ) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        Releasable decrementRef = null;
        try {
            final FileChannelReference reference = acquireFileChannelReference();
            decrementRef = Releasables.releaseOnce(reference::decRef);
            final List<SparseFileTracker.Gap> gaps = tracker.waitForRange(
                rangeToWrite,
                rangeToRead,
                rangeListener(rangeToRead, reader, future, reference, decrementRef)
            );

            for (SparseFileTracker.Gap gap : gaps) {
                executor.execute(new AbstractRunnable() {

                    @Override
                    protected void doRun() throws Exception {
                        if (reference.tryIncRef() == false) {
                            assert false : "expected a non-closed channel reference";
                            throw new AlreadyClosedException("Cache file channel has been released and closed");
                        }
                        try {
                            ensureOpen();
                            writer.fillCacheRange(reference.fileChannel, gap.start(), gap.end(), gap::onProgress);
                        } finally {
                            reference.decRef();
                        }
                        gap.onCompletion();
                        markAsNeedsFSync();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        gap.onFailure(e);
                    }
                });
            }
        } catch (Exception e) {
            releaseAndFail(future, decrementRef, e);
        }
        return future;
    }

    /**
     * Notifies the {@link RangeAvailableHandler} when {@code rangeToRead} is available to read from the file. If {@code rangeToRead} is
     * already available then the {@link RangeAvailableHandler} is called synchronously by this method; if not, but it is pending, then the
     * {@link RangeAvailableHandler} is notified when the pending ranges have completed. If it contains gaps that are not currently pending
     * then no listeners are registered and this method returns {@code null}.
     *
     * @return a future which returns the result of the {@link RangeAvailableHandler} once it has completed, or {@code null} if the
     *         target range is neither available nor pending.
     */
    @Nullable
    Future<Integer> readIfAvailableOrPending(final Tuple<Long, Long> rangeToRead, final RangeAvailableHandler reader) {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        Releasable decrementRef = null;
        try {
            final FileChannelReference reference = acquireFileChannelReference();
            decrementRef = Releasables.releaseOnce(reference::decRef);
            if (tracker.waitForRangeIfPending(rangeToRead, rangeListener(rangeToRead, reader, future, reference, decrementRef))) {
                return future;
            } else {
                decrementRef.close();
                return null;
            }
        } catch (Exception e) {
            releaseAndFail(future, decrementRef, e);
            return future;
        }
    }

    private static void releaseAndFail(CompletableFuture<Integer> future, Releasable decrementRef, Exception e) {
        try {
            Releasables.close(decrementRef);
        } catch (Exception ex) {
            e.addSuppressed(ex);
        }
        future.completeExceptionally(e);
    }

    private static ActionListener<Void> rangeListener(
        Tuple<Long, Long> rangeToRead,
        RangeAvailableHandler reader,
        CompletableFuture<Integer> future,
        FileChannelReference reference,
        Releasable releasable
    ) {
        return ActionListener.runAfter(ActionListener.wrap(success -> {
            final int read = reader.onRangeAvailable(reference.fileChannel);
            assert read == rangeToRead.v2() - rangeToRead.v1() : "partial read ["
                + read
                + "] does not match the range to read ["
                + rangeToRead.v2()
                + '-'
                + rangeToRead.v1()
                + ']';
            future.complete(read);
        }, future::completeExceptionally), releasable::close);
    }

    /**
     * Get the reference to the currently open file channel for this cache file for a read operation
     *
     * @return file channel reference
     */
    private FileChannelReference acquireFileChannelReference() {
        final FileChannelReference reference;
        synchronized (listeners) {
            ensureOpen();
            reference = channelRef;
            assert reference != null
                && reference.refCount() > 0 : "impossible to run into a fully released channel reference under the listeners mutex";
            assert refCounter.refCount() > 0 : "file should not be fully released";
            reference.incRef();
        }
        return reference;
    }

    public Tuple<Long, Long> getAbsentRangeWithin(long start, long end) {
        ensureOpen();
        return tracker.getAbsentRangeWithin(start, end);
    }

    // used in tests
    boolean needsFsync() {
        return needsFsync.get();
    }

    /**
     * Marks the current cache file as "fsync needed" and notifies the corresponding listener.
     */
    private void markAsNeedsFSync() {
        if (needsFsync.getAndSet(true) == false) {
            needsFsyncRunnable.run();
        }
    }

    /**
     * Ensure that all ranges of data written to the cache file are written to the storage device that contains it. This method performs
     * synchronization only if data has been written to the cache since the last time the method was called. If calling this method
     * resulted in performing a synchronization, a sorted set of all successfully written ranges of data since the creation of the cache
     * file is returned. If the cache file is evicted or if a concurrent thread is already fsyncing the file this method returns an empty
     * set of ranges.
     *
     * @return a sorted set of ranges of data available in cache iff calling this method resulted in performing a fsync
     * @throws IOException                       if the cache file failed to be fsync
     * @throws java.nio.file.NoSuchFileException if the cache file does not exist
     */
    public SortedSet<Tuple<Long, Long>> fsync() throws IOException {
        if (refCounter.tryIncRef()) {
            try {
                if (needsFsync.compareAndSet(true, false)) {
                    boolean success = false;
                    try {
                        // Capture the completed ranges before fsyncing; ranges that are completed after this point won't be considered as
                        // persisted on disk by the caller of this method, even if they are fully written to disk at the time the file
                        // fsync is effectively executed
                        final SortedSet<Tuple<Long, Long>> completedRanges = tracker.getCompletedRanges();
                        assert completedRanges != null;
                        assert completedRanges.isEmpty() == false;

                        IOUtils.fsync(file, false, false);
                        success = true;
                        return completedRanges;
                    } finally {
                        if (success == false) {
                            markAsNeedsFSync();
                        }
                    }
                }
            } finally {
                refCounter.decRef();
            }
        } else {
            assert evicted.get();
        }
        return Collections.emptySortedSet();
    }
}
