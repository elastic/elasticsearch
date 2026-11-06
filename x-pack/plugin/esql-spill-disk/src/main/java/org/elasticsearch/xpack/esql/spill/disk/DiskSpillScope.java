/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.spill.disk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.spill.spi.SpillWriter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One query's directory. Operations count themselves as pending while they run on the I/O pool; cancellation makes
 * every later cancellable operation fail at its start, and the close waits for the pending ones, then deletes the
 * directory and returns the bytes of its files, those it could not delete going into the store's cleanup debt. A close
 * whose task the pool refused leaves the scope registered, so that the next close, the shutdown's at the latest, tries
 * again.
 */
final class DiskSpillScope implements SpillScope {

    private final DiskSpillStore store;
    private final String queryId;
    private final Path directory;
    private final CircuitBreaker breaker;
    private final AtomicLong counter = new AtomicLong();
    private final AtomicLong bytes = new AtomicLong();
    private final AtomicInteger pending = new AtomicInteger();
    private final Set<DiskSpillWriter> writers = ConcurrentHashMap.newKeySet();
    private final Set<DiskSpillObject> objects = ConcurrentHashMap.newKeySet();
    private volatile boolean cancelled;
    private volatile boolean closing;
    private SubscribableListener<Void> closeOutcome;
    private boolean closeStarted;

    DiskSpillScope(DiskSpillStore store, String queryId, Path directory, CircuitBreaker breaker) {
        this.store = store;
        this.queryId = queryId;
        this.directory = directory;
        this.breaker = breaker;
    }

    String queryId() {
        return queryId;
    }

    Path directory() {
        return directory;
    }

    CircuitBreaker breaker() {
        return breaker;
    }

    DiskSpillStore store() {
        return store;
    }

    boolean cancelled() {
        return cancelled;
    }

    void addBytes(long delta) {
        bytes.addAndGet(delta);
    }

    /**
     * Runs a cancellable operation on the I/O pool, counted as pending until its listener has completed: one issued after
     * the close is refused at once, one queued before a cancellation fails when its turn comes.
     */
    <T> void io(ActionListener<T> listener, CheckedSupplier<T, Exception> work) {
        if (closing) {
            listener.onFailure(new IllegalStateException("spill scope [" + queryId + "] is closed"));
            return;
        }
        run(listener, () -> {
            if (cancelled) {
                throw new TaskCancelledException("spill scope [" + queryId + "] cancelled");
            }
            return work.get();
        });
    }

    /**
     * Runs a cleanup operation on the I/O pool, counted as pending so that the close waits for it, never refused and
     * never cancelled: abort, delete and the closing of a reader's channel.
     */
    <T> void ioUncancellable(ActionListener<T> listener, CheckedSupplier<T, Exception> work) {
        run(listener, work);
    }

    private <T> void run(ActionListener<T> listener, CheckedSupplier<T, Exception> work) {
        pending.incrementAndGet();
        store.onIo(ActionListener.runAfter(listener, () -> {
            pending.decrementAndGet();
            maybeStartClose();
        }), work);
    }

    @Override
    public void createWriter(String nameHint, ActionListener<SpillWriter> listener) {
        if (closing) {
            listener.onFailure(new IllegalStateException("spill scope [" + queryId + "] is closed"));
            return;
        }
        io(listener, () -> {
            Files.createDirectories(directory);
            Path path = directory.resolve(counter.incrementAndGet() + ".run");
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            DiskSpillWriter writer = new DiskSpillWriter(this, path, nameHint, channel);
            writers.add(writer);
            return writer;
        });
    }

    void writerDone(DiskSpillWriter writer) {
        writers.remove(writer);
    }

    void objectPublished(DiskSpillObject object) {
        objects.add(object);
    }

    void objectGone(DiskSpillObject object) {
        objects.remove(object);
    }

    @Override
    public void cancel() {
        cancelled = true;
    }

    @Override
    public void close(ActionListener<Void> listener) {
        cancelled = true;
        closing = true;
        SubscribableListener<Void> outcome;
        synchronized (this) {
            if (closeOutcome == null) {
                closeOutcome = new SubscribableListener<>();
            }
            outcome = closeOutcome;
        }
        // outside the monitor: a completed outcome runs the listener inline
        outcome.addListener(listener);
        maybeStartClose();
    }

    private void maybeStartClose() {
        SubscribableListener<Void> outcome;
        synchronized (this) {
            if (closeOutcome == null || closeStarted || pending.get() > 0) {
                return;
            }
            closeStarted = true;
            outcome = closeOutcome;
        }
        store.onIo(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                // the store drops the scope and retries its cleanup debt before the close is reported, so a caller that
                // checks the accounting right after sees the retry's result
                store.scopeClosed(DiskSpillScope.this);
                outcome.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                // the pool refused the task, or listing the directory failed: the files and their bytes stay, and the
                // scope stays registered so that a later close, the shutdown's at the latest, tries again with a fresh
                // outcome; the discards it repeats are idempotent
                synchronized (DiskSpillScope.this) {
                    closeStarted = false;
                    closeOutcome = null;
                }
                outcome.onFailure(e);
            }
        }, () -> {
            for (DiskSpillWriter writer : writers) {
                writer.discard();
            }
            for (DiskSpillObject object : objects) {
                object.discard();
            }
            deleteDirectory();
            return null;
        });
    }

    /**
     * Removes whatever is left in the directory; files still there are already in the store's debt.
     */
    private void deleteDirectory() throws IOException {
        if (Files.isDirectory(directory) == false) {
            return;
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
            for (Path entry : entries) {
                try {
                    IOUtils.rm(entry);
                } catch (IOException e) {
                    // the objects and writers of this scope account for their own files; a file that stays is in the debt
                }
            }
        }
        try {
            Files.deleteIfExists(directory);
        } catch (IOException e) {
            // a file that could not be deleted keeps the directory; it is in the debt and retried later
        }
    }

    @Override
    public long bytesInUse() {
        return bytes.get();
    }
}
