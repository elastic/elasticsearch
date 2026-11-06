/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.spill.spi.SpillLimitExceededException;
import org.elasticsearch.compute.spill.spi.SpillObject;
import org.elasticsearch.compute.spill.spi.SpillRead;
import org.elasticsearch.compute.spill.spi.SpillReader;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.spill.spi.SpillWriter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A deterministic in-memory backend for the spill SPI, for operator tests and for the conformance suite. Every
 * operation decides its outcome and makes its state transition at the call, outside any monitor by the time the
 * callback runs, then delivers the callback according to the {@link Completion} mode: inline, held until the test
 * releases it, or on an executor, whose rejection becomes the operation's failure. Stored bytes stay outside any
 * breaker, as a backend's disk would; the references an append holds while deferred and the buffers a read returns are
 * charged to the scope's breaker, so a leak of either shows up as a breaker that is not back at zero. Cancellation
 * turns the success of a cancellable operation into a cancellation failure when it is delivered, the way interrupted
 * I/O would, and a failure delivered to a writer or a reader poisons it.
 */
public final class MemorySpillStore implements SpillStore {

    public enum Completion {
        INLINE,
        DEFERRED,
        EXECUTOR
    }

    private final Completion completion;
    @Nullable
    private final Executor executor;
    private final String limitSetting;
    private final Random random;
    private volatile long limitBytes;
    private volatile boolean shortReads = true;
    private volatile Function<String, Exception> failureInjection = operation -> null;
    private volatile boolean shutdown;

    private final Object heldLock = new Object();
    private final Deque<Runnable> held = new ArrayDeque<>();
    private final Map<String, MemoryScope> scopes = new ConcurrentHashMap<>();
    private final AtomicLong bytesInUse = new AtomicLong();
    private final AtomicInteger pending = new AtomicInteger();
    private final AtomicInteger liveReaders = new AtomicInteger();
    private final AtomicInteger operations = new AtomicInteger();

    /**
     * @param executor used in {@link Completion#EXECUTOR} mode only
     * @param limitSetting the setting name reported by {@link SpillLimitExceededException}
     * @param random the store's own source of randomness, for short reads; never the test's, since reads complete on
     *               executor threads
     */
    public MemorySpillStore(Completion completion, @Nullable Executor executor, long limitBytes, String limitSetting, Random random) {
        this.completion = completion;
        this.executor = executor;
        this.limitBytes = limitBytes;
        this.limitSetting = limitSetting;
        this.random = random;
    }

    // ---- test controls

    /**
     * Delivers up to {@code n} held callbacks, oldest first; returns how many were delivered.
     */
    public int release(int n) {
        int delivered = 0;
        while (delivered < n) {
            Runnable next;
            synchronized (heldLock) {
                next = held.pollFirst();
            }
            if (next == null) {
                break;
            }
            next.run();
            delivered++;
        }
        return delivered;
    }

    /**
     * Delivers one held callback chosen at random; returns false when none is held.
     */
    public boolean releaseRandom() {
        Runnable chosen;
        synchronized (heldLock) {
            if (held.isEmpty()) {
                return false;
            }
            List<Runnable> all = new ArrayList<>(held);
            chosen = all.remove(random.nextInt(all.size()));
            held.clear();
            held.addAll(all);
        }
        chosen.run();
        return true;
    }

    public int releaseAll() {
        int delivered = 0;
        while (releaseRandom()) {
            delivered++;
        }
        return delivered;
    }

    public int heldOperations() {
        synchronized (heldLock) {
            return held.size();
        }
    }

    /**
     * Operations accepted and not yet delivered, held or on the executor.
     */
    public int pendingOperations() {
        return pending.get();
    }

    public int operationsAccepted() {
        return operations.get();
    }

    public int liveScopes() {
        return scopes.size();
    }

    public int liveReaders() {
        return liveReaders.get();
    }

    public void setLimitBytes(long limitBytes) {
        this.limitBytes = limitBytes;
    }

    /**
     * Flips bits of one byte of a stored object, for corruption tests, so the byte always changes; the object must be
     * one of this store's and {@code mask} must not be zero.
     */
    public void corrupt(SpillObject object, long offset, byte mask) {
        if (mask == 0) {
            throw new IllegalArgumentException("a zero mask changes nothing");
        }
        ((MemoryObject) object).data[Math.toIntExact(offset)] ^= mask;
    }

    /**
     * Cuts a stored object to {@code length} bytes, for truncation tests. Readers opened before see the shorter object
     * from their next read; the store's accounting follows.
     */
    public void truncate(SpillObject object, long length) {
        MemoryObject memory = (MemoryObject) object;
        synchronized (memory) {
            long removed = memory.data.length - length;
            memory.data = Arrays.copyOf(memory.data, Math.toIntExact(length));
            memory.scope.addBytes(-removed);
        }
    }

    public void setShortReads(boolean shortReads) {
        this.shortReads = shortReads;
    }

    /**
     * A failure to deliver instead of the outcome, by operation name: append, finish, abort, createWriter, openRange,
     * read, closeReader, delete, close, shutdown. Returning null leaves the outcome alone.
     */
    public void setFailureInjection(Function<String, Exception> failureInjection) {
        this.failureInjection = failureInjection;
    }

    // ---- the store

    @Override
    public SpillScope newScope(String queryId, CircuitBreaker breaker) {
        if (shutdown) {
            throw new IllegalStateException("store is shut down");
        }
        MemoryScope scope = new MemoryScope(queryId, breaker);
        if (scopes.putIfAbsent(queryId, scope) != null) {
            throw new IllegalArgumentException("scope [" + queryId + "] exists");
        }
        return scope;
    }

    @Override
    public long bytesInUse() {
        return bytesInUse.get();
    }

    @Override
    public long bytesLimit() {
        return limitBytes;
    }

    @Override
    public void shutdown(ActionListener<Void> listener) {
        shutdown = true;
        List<MemoryScope> open = new ArrayList<>(scopes.values());
        if (open.isEmpty()) {
            deliver(null, "shutdown", Outcome.success(null), null, null, listener);
            return;
        }
        GroupedActionListener<Void> allClosed = new GroupedActionListener<>(
            open.size(),
            ActionListener.wrap(ignored -> deliver(null, "shutdown", Outcome.success(null), null, null, listener), listener::onFailure)
        );
        for (MemoryScope scope : open) {
            scope.close(allClosed);
        }
    }

    // ---- delivery

    private record Outcome<T>(@Nullable T result, @Nullable Exception failure) {
        static <T> Outcome<T> success(T result) {
            return new Outcome<>(result, null);
        }

        static <T> Outcome<T> failure(Exception e) {
            return new Outcome<>(null, e);
        }
    }

    private static boolean cancellable(String name) {
        return switch (name) {
            case "append", "finish", "createWriter", "openRange", "read" -> true;
            case "abort", "closeReader", "delete", "close", "shutdown" -> false;
            default -> throw new IllegalArgumentException("unknown operation [" + name + "]");
        };
    }

    /**
     * Delivers an outcome decided at the call. {@code beforeCallback} releases what the operation borrowed and runs
     * before the terminal callback on every path; {@code onFailure} lets the owner poison itself when the delivered
     * outcome is a failure, including an injected one, a cancellation or an executor rejection. Neither is called
     * under a monitor.
     */
    private <T> void deliver(
        @Nullable MemoryScope scope,
        String name,
        Outcome<T> decided,
        @Nullable Runnable beforeCallback,
        @Nullable Consumer<Exception> onFailure,
        ActionListener<T> listener
    ) {
        operations.incrementAndGet();
        pending.incrementAndGet();
        if (scope != null) {
            scope.pending.incrementAndGet();
        }
        Runnable callback = () -> {
            Outcome<T> outcome = decided;
            Exception injected = failureInjection.apply(name);
            if (injected != null) {
                outcome = Outcome.failure(injected);
            } else if (outcome.failure() == null && scope != null && scope.cancelled && cancellable(name)) {
                outcome = Outcome.failure(new TaskCancelledException("spill scope [" + scope.id + "] cancelled during " + name));
            }
            complete(scope, outcome, beforeCallback, onFailure, listener);
        };
        switch (completion) {
            case INLINE -> callback.run();
            case DEFERRED -> {
                synchronized (heldLock) {
                    held.addLast(callback);
                }
            }
            case EXECUTOR -> {
                AbstractRunnable task = new AbstractRunnable() {
                    @Override
                    protected void doRun() {
                        callback.run();
                    }

                    @Override
                    public void onRejection(Exception e) {
                        complete(scope, Outcome.failure(e), beforeCallback, onFailure, listener);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assert false : e;
                    }
                };
                try {
                    executor.execute(task);
                } catch (RejectedExecutionException e) {
                    // an Elasticsearch pool routes a rejection to onRejection itself; a plain executor throws it here
                    task.onRejection(e);
                }
            }
        }
    }

    /**
     * The terminal callback; a scope's close, which waits for its pending operations, is started only after it.
     */
    private <T> void complete(
        @Nullable MemoryScope scope,
        Outcome<T> outcome,
        @Nullable Runnable beforeCallback,
        @Nullable Consumer<Exception> onFailure,
        ActionListener<T> listener
    ) {
        try {
            if (outcome.failure() != null && onFailure != null) {
                onFailure.accept(outcome.failure());
            }
            if (beforeCallback != null) {
                beforeCallback.run();
            }
            if (outcome.failure() == null) {
                listener.onResponse(outcome.result());
            } else {
                listener.onFailure(outcome.failure());
            }
        } finally {
            pending.decrementAndGet();
            if (scope != null) {
                scope.pending.decrementAndGet();
                scope.maybeStartClose();
            }
        }
    }

    // ---- scopes

    private final class MemoryScope implements SpillScope {
        private final String id;
        private final CircuitBreaker breaker;
        private final AtomicInteger pending = new AtomicInteger();
        private final AtomicLong bytes = new AtomicLong();
        private final Set<MemoryWriter> writers = ConcurrentHashMap.newKeySet();
        private final Set<MemoryObject> objects = ConcurrentHashMap.newKeySet();
        private volatile boolean cancelled;
        private volatile boolean closing;
        @Nullable
        private SubscribableListener<Void> closeOutcome;
        private boolean closeStarted;

        MemoryScope(String id, CircuitBreaker breaker) {
            this.id = id;
            this.breaker = breaker;
        }

        @Override
        public void createWriter(String nameHint, ActionListener<SpillWriter> listener) {
            Outcome<SpillWriter> outcome;
            if (closing) {
                outcome = Outcome.failure(new IllegalStateException("spill scope [" + id + "] is closed"));
            } else {
                MemoryWriter writer = new MemoryWriter(this, nameHint);
                writers.add(writer);
                outcome = Outcome.success(writer);
            }
            deliver(this, "createWriter", outcome, null, null, listener);
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        @Override
        public void close(ActionListener<Void> listener) {
            cancelled = true;
            closing = true;
            synchronized (this) {
                if (closeOutcome == null) {
                    closeOutcome = new SubscribableListener<>();
                }
                closeOutcome.addListener(listener);
            }
            maybeStartClose();
        }

        /**
         * The close runs once no operation of the scope is pending, exactly once, and is itself delivered like any
         * operation. The discards run outside the scope's monitor, since a writer's own monitor is taken in them.
         */
        void maybeStartClose() {
            List<MemoryWriter> toDiscard;
            List<MemoryObject> toDelete;
            SubscribableListener<Void> outcome;
            synchronized (this) {
                if (closeOutcome == null || closeStarted || pending.get() > 0) {
                    return;
                }
                closeStarted = true;
                outcome = closeOutcome;
                toDiscard = new ArrayList<>(writers);
                toDelete = new ArrayList<>(objects);
            }
            for (MemoryWriter writer : toDiscard) {
                writer.discard();
            }
            for (MemoryObject object : toDelete) {
                object.discard();
            }
            scopes.remove(id);
            deliver(null, "close", Outcome.success(null), null, null, outcome);
        }

        @Override
        public long bytesInUse() {
            return bytes.get();
        }

        void addBytes(long delta) {
            bytes.addAndGet(delta);
            bytesInUse.addAndGet(delta);
        }
    }

    // ---- writers

    private enum WriterState {
        OPEN,
        FINISHING,
        FINISHED,
        ABORTED,
        FAILED
    }

    private final class MemoryWriter implements SpillWriter {
        private final MemoryScope scope;
        private final String nameHint;
        private WriterState state = WriterState.OPEN;
        private boolean outstanding;
        private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private long appended;
        @Nullable
        private MemoryObject finishing;

        MemoryWriter(MemoryScope scope, String nameHint) {
            this.scope = scope;
            this.nameHint = nameHint;
        }

        @Override
        public void append(ReleasableBytesReference chunk, ActionListener<Void> listener) {
            final int length = chunk.length();
            Outcome<Void> outcome;
            Runnable beforeCallback = null;
            synchronized (this) {
                if (state != WriterState.OPEN) {
                    outcome = Outcome.failure(new IllegalStateException("writer [" + nameHint + "] is " + state));
                } else if (outstanding) {
                    state = WriterState.FAILED;
                    outcome = Outcome.failure(new IllegalStateException("writer [" + nameHint + "] has an append outstanding"));
                } else if (chunk.hasReferences() == false) {
                    state = WriterState.FAILED;
                    outcome = Outcome.failure(new IllegalStateException("chunk appended without a live reference"));
                } else {
                    ReleasableBytesReference mine = chunk.retain();
                    boolean charged = false;
                    try {
                        scope.breaker.addEstimateBytesAndMaybeBreak(length, "memory spill append");
                        charged = true;
                    } catch (CircuitBreakingException e) {
                        mine.close();
                        state = WriterState.FAILED;
                        outcome = Outcome.failure(e);
                    }
                    if (charged) {
                        outstanding = true;
                        beforeCallback = () -> {
                            mine.close();
                            scope.breaker.addWithoutBreaking(-length);
                            synchronized (this) {
                                outstanding = false;
                            }
                        };
                        long inUse = bytesInUse.get();
                        if (inUse + length > limitBytes) {
                            state = WriterState.FAILED;
                            outcome = Outcome.failure(new SpillLimitExceededException(limitSetting, limitBytes, inUse, length));
                        } else {
                            buffer.write(BytesReference.toBytes(chunk), 0, length);
                            appended += length;
                            scope.addBytes(length);
                            outcome = Outcome.success(null);
                        }
                    } else {
                        outcome = Outcome.failure(new IllegalStateException("unreachable"));
                        assert false : "charged must be true or the breaker threw";
                    }
                }
            }
            deliver(scope, "append", outcome, beforeCallback, this::poison, listener);
        }

        @Override
        public void finish(ActionListener<SpillObject> listener) {
            Outcome<SpillObject> outcome;
            Runnable beforeCallback = null;
            synchronized (this) {
                if (state != WriterState.OPEN) {
                    outcome = Outcome.failure(new IllegalStateException("writer [" + nameHint + "] is " + state));
                } else if (outstanding) {
                    state = WriterState.FAILED;
                    outcome = Outcome.failure(new IllegalStateException("finish while an append is outstanding"));
                } else {
                    // the object exists from here and owns the bytes; a failure delivered for this finish discards it again.
                    // It is registered before the writer is deregistered, so a close racing this call sees one of them.
                    state = WriterState.FINISHING;
                    MemoryObject object = new MemoryObject(scope, buffer.toByteArray());
                    buffer = null;
                    appended = 0;
                    finishing = object;
                    scope.objects.add(object);
                    scope.writers.remove(this);
                    beforeCallback = () -> {
                        synchronized (this) {
                            if (state == WriterState.FINISHING) {
                                state = WriterState.FINISHED;
                                finishing = null;
                            }
                        }
                    };
                    outcome = Outcome.success(object);
                }
            }
            deliver(scope, "finish", outcome, beforeCallback, this::poison, listener);
        }

        @Override
        public void abort(ActionListener<Void> listener) {
            discard();
            deliver(scope, "abort", Outcome.success(null), null, null, listener);
        }

        /**
         * Returns the bytes of an unfinished or failed writer; a no-op once finished or aborted.
         */
        void discard() {
            synchronized (this) {
                if (state == WriterState.FINISHED || state == WriterState.ABORTED || state == WriterState.FINISHING) {
                    return;
                }
                state = WriterState.ABORTED;
                scope.addBytes(-appended);
                appended = 0;
                buffer = null;
            }
            scope.writers.remove(this);
        }

        /**
         * A failure delivered to an append or a finish: the writer is unusable, and an object a finish had published is
         * taken back.
         */
        private void poison(Exception e) {
            MemoryObject orphan = null;
            synchronized (this) {
                if (state == WriterState.OPEN) {
                    state = WriterState.FAILED;
                } else if (state == WriterState.FINISHING) {
                    state = WriterState.FAILED;
                    orphan = finishing;
                    finishing = null;
                }
            }
            if (orphan != null) {
                orphan.discard();
            }
        }
    }

    // ---- objects and readers

    private final class MemoryObject implements SpillObject {
        private final MemoryScope scope;
        private volatile byte[] data;
        private volatile boolean deleted;

        MemoryObject(MemoryScope scope, byte[] data) {
            this.scope = scope;
            this.data = data;
        }

        @Override
        public long sizeInBytes() {
            return data.length;
        }

        @Override
        public void openRange(long offset, long length, ActionListener<SpillReader> listener) {
            Outcome<SpillReader> outcome;
            Consumer<Exception> onFailure = null;
            if (offset < 0 || length < 0 || offset + length > data.length) {
                outcome = Outcome.failure(
                    new IllegalArgumentException(
                        "range [" + offset + ", " + (offset + length) + ") outside object of " + data.length + " bytes"
                    )
                );
            } else if (deleted) {
                outcome = Outcome.failure(new IllegalStateException("object is deleted"));
            } else {
                liveReaders.incrementAndGet();
                MemoryReader reader = new MemoryReader(this, (int) offset, (int) (offset + length));
                // a reader whose opening is converted to a failure at delivery is never seen, so it is closed here
                onFailure = e -> reader.closeQuietly();
                outcome = Outcome.success(reader);
            }
            deliver(scope, "openRange", outcome, null, onFailure, listener);
        }

        @Override
        public void delete(ActionListener<Void> listener) {
            discard();
            deliver(scope, "delete", Outcome.success(null), null, null, listener);
        }

        void discard() {
            boolean first;
            synchronized (this) {
                first = deleted == false;
                deleted = true;
            }
            if (first) {
                scope.objects.remove(this);
                scope.addBytes(-data.length);
            }
        }
    }

    private final class MemoryReader implements SpillReader {
        private final MemoryObject object;
        private int position;
        private final int end;
        private boolean outstanding;
        private boolean failed;
        private boolean closed;

        MemoryReader(MemoryObject object, int start, int end) {
            this.object = object;
            this.position = start;
            this.end = end;
        }

        @Override
        public void read(int maxBytes, ActionListener<SpillRead> listener) {
            Outcome<SpillRead> outcome;
            Runnable beforeCallback = null;
            Consumer<Exception> onFailure = null;
            synchronized (this) {
                if (maxBytes <= 0) {
                    outcome = Outcome.failure(new IllegalArgumentException("maxBytes must be positive, got [" + maxBytes + "]"));
                } else if (closed) {
                    outcome = Outcome.failure(new IllegalStateException("reader is closed"));
                } else if (failed) {
                    outcome = Outcome.failure(new IllegalStateException("reader failed earlier"));
                } else if (outstanding) {
                    failed = true;
                    outcome = Outcome.failure(new IllegalStateException("a read is outstanding"));
                } else if (object.deleted) {
                    outcome = Outcome.failure(new IllegalStateException("object is deleted"));
                } else if (position == end) {
                    outcome = Outcome.success(SpillRead.eof());
                } else {
                    int n = Math.min(maxBytes, end - position);
                    if (shortReads && n > 1 && random.nextBoolean()) {
                        n = 1 + random.nextInt(n);
                    }
                    final int charged = n;
                    Outcome<SpillRead> decided = null;
                    try {
                        object.scope.breaker.addEstimateBytesAndMaybeBreak(charged, "memory spill read");
                    } catch (CircuitBreakingException e) {
                        // nothing was advanced and the reader is not poisoned: the caller may retry once memory is back
                        decided = Outcome.failure(e);
                    }
                    if (decided != null) {
                        outcome = decided;
                    } else {
                        byte[] copy = new byte[n];
                        System.arraycopy(object.data, position, copy, 0, n);
                        position += n;
                        outstanding = true;
                        ReleasableBytesReference data = new ReleasableBytesReference(
                            new BytesArray(copy),
                            () -> object.scope.breaker.addWithoutBreaking(-charged)
                        );
                        beforeCallback = () -> {
                            synchronized (this) {
                                outstanding = false;
                            }
                        };
                        // a read whose outcome is converted to a failure at delivery releases the buffer it had decided
                        // to return, and the reader is poisoned, since bytes were consumed from the range
                        onFailure = e -> {
                            data.close();
                            synchronized (this) {
                                failed = true;
                            }
                        };
                        outcome = Outcome.success(SpillRead.of(data));
                    }
                }
            }
            deliver(object.scope, "read", outcome, beforeCallback, onFailure, listener);
        }

        @Override
        public void close(ActionListener<Void> listener) {
            closeQuietly();
            deliver(object.scope, "closeReader", Outcome.success(null), null, null, listener);
        }

        void closeQuietly() {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
            }
            liveReaders.decrementAndGet();
        }
    }
}
