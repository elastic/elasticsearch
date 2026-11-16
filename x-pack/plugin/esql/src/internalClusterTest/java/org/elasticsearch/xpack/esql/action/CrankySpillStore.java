/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.compute.spill.spi.SpillLimitExceededException;
import org.elasticsearch.compute.spill.spi.SpillObject;
import org.elasticsearch.compute.spill.spi.SpillRead;
import org.elasticsearch.compute.spill.spi.SpillReader;
import org.elasticsearch.compute.spill.spi.SpillScope;
import org.elasticsearch.compute.spill.spi.SpillStore;
import org.elasticsearch.compute.spill.spi.SpillStoreServices;
import org.elasticsearch.compute.spill.spi.SpillWriter;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.EnumMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Delegates every call to the disk store and, at the configured rate, misbehaves instead: a failure before or after
 * a write, a refused cap, a failed seal, a failed open or read, a short read, a skipped delete, a failed writer
 * creation, and a delayed completion, so that real drivers block and resume through the listener. Every injection is
 * counted by kind and logged with the seed and the call's ordinal, so a failure reproduces from the log. The wrapper
 * also checks what the in-memory store checks of its callers, never two operations outstanding on one writer or reader
 * and nothing after a finish or a close, and counts the violations for the test to assert there were none.
 */
public final class CrankySpillStore implements SpillStore {
    private static final Logger logger = LogManager.getLogger(CrankySpillStore.class);

    public enum Kind {
        CREATE_WRITER,
        WRITE_BEFORE,
        WRITE_AFTER,
        CAP,
        FINISH,
        OPEN,
        READ,
        READ_SHORT,
        DELETE
    }

    private final SpillStore delegate;
    private final Executor io;
    private final String nodeId;
    private final Runnable onShutdown;
    private volatile double rate;
    private volatile long seed;
    private volatile long maxDelayMillis;
    private Random random;
    private final AtomicLong calls = new AtomicLong();
    private final Map<Kind, LongAdder> injected = new EnumMap<>(Kind.class);
    private final LongAdder delays = new LongAdder();
    private final LongAdder violations = new LongAdder();

    CrankySpillStore(SpillStore delegate, SpillStoreServices services, Runnable onShutdown) {
        this.delegate = delegate;
        this.io = services.ioExecutor();
        this.nodeId = services.nodeId();
        this.onShutdown = onShutdown;
        for (Kind kind : Kind.values()) {
            injected.put(kind, new LongAdder());
        }
        services.clusterSettings().initializeAndWatch(CrankySpillStorePlugin.FAILURE_RATE, v -> rate = v);
        services.clusterSettings().initializeAndWatch(CrankySpillStorePlugin.SEED, this::reseed);
        services.clusterSettings().initializeAndWatch(CrankySpillStorePlugin.MAX_DELAY, v -> maxDelayMillis = v.millis());
    }

    private synchronized void reseed(long seed) {
        this.seed = seed;
        this.random = new Random(seed ^ nodeId.hashCode());
    }

    /**
     * One draw per operation: at the rate, the operation misbehaves in one of the ways it can, chosen evenly; the
     * rate is then the share of operations that misbehave, whatever their kind.
     */
    @Nullable
    private synchronized Kind pick(Kind... candidates) {
        long call = calls.incrementAndGet();
        if (rate == 0 || random.nextDouble() >= rate) {
            return null;
        }
        Kind kind = candidates[random.nextInt(candidates.length)];
        injected.get(kind).increment();
        logger.info("cranky spill: [{}] at call [{}] on node [{}] with seed [{}]", kind, call, nodeId, seed);
        return kind;
    }

    private synchronized long delay() {
        return maxDelayMillis == 0 ? 0 : random.nextLong(maxDelayMillis + 1);
    }

    private synchronized int shortRead(int maxBytes) {
        return Math.max(1, random.nextInt(maxBytes));
    }

    private void violation(String what) {
        violations.increment();
        logger.warn("cranky spill: contract violation: {}", what);
    }

    /**
     * Completes on the I/O pool after a random delay up to the maximum, so the driver really waits and really resumes.
     * A pool that refuses the task completes the listener at once: every listener completes, delayed or not.
     */
    private <T> ActionListener<T> maybeDelayed(ActionListener<T> listener) {
        long delay = delay();
        if (delay == 0) {
            return listener;
        }
        delays.increment();
        return new ActionListener<>() {
            @Override
            public void onResponse(T value) {
                later(delay, () -> listener.onResponse(value));
            }

            @Override
            public void onFailure(Exception e) {
                later(delay, () -> listener.onFailure(e));
            }
        };
    }

    private void later(long delay, Runnable completion) {
        io.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                pause(delay);
                completion.run();
            }

            @Override
            public void onRejection(Exception e) {
                completion.run();
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("cranky spill: a delayed completion failed", e);
            }
        });
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Injections of every kind, on this node. */
    public long injections() {
        long sum = 0;
        for (LongAdder adder : injected.values()) {
            sum += adder.sum();
        }
        return sum;
    }

    public long injections(Kind kind) {
        return injected.get(kind).sum();
    }

    /** Completions held back on this node. */
    public long delays() {
        return delays.sum();
    }

    /** Contract violations by the store's callers, seen on this node. */
    public long violations() {
        return violations.sum();
    }

    public SpillStore delegate() {
        return delegate;
    }

    @Override
    public SpillScope newScope(String queryId, CircuitBreaker breaker) {
        return new CrankyScope(delegate.newScope(queryId, breaker), queryId);
    }

    @Override
    public long bytesInUse() {
        return delegate.bytesInUse();
    }

    @Override
    public long bytesLimit() {
        return delegate.bytesLimit();
    }

    @Override
    public long cleanupDebtBytes() {
        return delegate.cleanupDebtBytes();
    }

    @Override
    public void shutdown(ActionListener<Void> listener) {
        onShutdown.run();
        delegate.shutdown(listener);
    }

    private final class CrankyScope implements SpillScope {
        private final SpillScope delegate;
        private final String queryId;

        CrankyScope(SpillScope delegate, String queryId) {
            this.delegate = delegate;
            this.queryId = queryId;
        }

        @Override
        public void createWriter(String nameHint, ActionListener<SpillWriter> listener) {
            if (pick(Kind.CREATE_WRITER) != null) {
                listener.onFailure(new ElasticsearchException("cranky spill: writer creation refused for [" + nameHint + "]"));
                return;
            }
            delegate.createWriter(nameHint, maybeDelayed(listener.map(writer -> new CrankyWriter(writer, nameHint))));
        }

        @Override
        public void cancel() {
            delegate.cancel();
        }

        @Override
        public void close(ActionListener<Void> listener) {
            delegate.close(maybeDelayed(listener));
        }

        @Override
        public long bytesInUse() {
            return delegate.bytesInUse();
        }

        @Override
        public String toString() {
            return "cranky scope of [" + queryId + "]";
        }
    }

    private final class CrankyWriter implements SpillWriter {
        private final SpillWriter delegate;
        private final String name;
        private final AtomicBoolean outstanding = new AtomicBoolean();
        private volatile boolean finished;
        /** Set by an injected failure: a later append or finish is the caller's violation, as it would be on a failed writer. */
        private volatile boolean failed;

        CrankyWriter(SpillWriter delegate, String name) {
            this.delegate = delegate;
            this.name = name;
        }

        private ActionListener<Void> done(ActionListener<Void> listener) {
            return ActionListener.runBefore(listener, () -> outstanding.set(false));
        }

        @Override
        public void append(ReleasableBytesReference chunk, ActionListener<Void> listener) {
            if (finished) {
                violation("append to the finished writer [" + name + "]");
            }
            if (outstanding.getAndSet(true)) {
                violation("append with one outstanding on writer [" + name + "]");
            }
            if (failed) {
                violation("append to the failed writer [" + name + "]");
                done(listener).onFailure(new IllegalStateException("cranky spill: writer [" + name + "] failed earlier"));
                return;
            }
            Kind kind = pick(Kind.CAP, Kind.WRITE_BEFORE, Kind.WRITE_AFTER);
            if (kind == Kind.CAP) {
                failed = true;
                done(listener).onFailure(
                    new SpillLimitExceededException("esql.spill.cranky.failure_rate", 0, 0, chunk.length(), "cranky spill: cap refused")
                );
                return;
            }
            if (kind == Kind.WRITE_BEFORE) {
                // nothing reached the disk
                failed = true;
                done(listener).onFailure(new ElasticsearchException("cranky spill: write refused on [" + name + "]"));
                return;
            }
            boolean failAfter = kind == Kind.WRITE_AFTER;
            delegate.append(chunk, maybeDelayed(done(listener).delegateFailureAndWrap((l, v) -> {
                if (failAfter) {
                    // the bytes are on the disk and the listener fails: the cleanup path of the writer's owner
                    failed = true;
                    l.onFailure(new ElasticsearchException("cranky spill: write failed after the fact on [" + name + "]"));
                } else {
                    l.onResponse(v);
                }
            })));
        }

        @Override
        public void finish(ActionListener<SpillObject> listener) {
            if (outstanding.get()) {
                violation("finish with an append outstanding on writer [" + name + "]");
            }
            if (failed) {
                violation("finish of the failed writer [" + name + "]");
                listener.onFailure(new IllegalStateException("cranky spill: writer [" + name + "] failed earlier"));
                return;
            }
            finished = true;
            if (pick(Kind.FINISH) != null) {
                // as a seal that failed in the store: the writer is done for, its bytes discarded, which the store does
                // itself on such a failure; what the writer's owner holds beyond that is its own to release
                failed = true;
                delegate.abort(ActionListener.wrap(ignored -> {}, e -> {}));
                listener.onFailure(new ElasticsearchException("cranky spill: seal refused on [" + name + "]"));
                return;
            }
            delegate.finish(maybeDelayed(listener.map(object -> new CrankyObject(object, name))));
        }

        @Override
        public void abort(ActionListener<Void> listener) {
            delegate.abort(maybeDelayed(listener));
        }
    }

    private final class CrankyObject implements SpillObject {
        private final SpillObject delegate;
        private final String name;

        CrankyObject(SpillObject delegate, String name) {
            this.delegate = delegate;
            this.name = name;
        }

        @Override
        public long sizeInBytes() {
            return delegate.sizeInBytes();
        }

        @Override
        public void openRange(long offset, long length, ActionListener<SpillReader> listener) {
            if (pick(Kind.OPEN) != null) {
                listener.onFailure(new ElasticsearchException("cranky spill: open refused on [" + name + "]"));
                return;
            }
            delegate.openRange(offset, length, maybeDelayed(listener.map(reader -> new CrankyReader(reader, name))));
        }

        @Override
        public void delete(ActionListener<Void> listener) {
            if (pick(Kind.DELETE) != null) {
                // left to the scope's close, which is the guarantee; a handle's delete is a convenience
                listener.onResponse(null);
                return;
            }
            delegate.delete(maybeDelayed(listener));
        }
    }

    private final class CrankyReader implements SpillReader {
        private final SpillReader delegate;
        private final String name;
        private final AtomicBoolean outstanding = new AtomicBoolean();
        private volatile boolean closed;

        CrankyReader(SpillReader delegate, String name) {
            this.delegate = delegate;
            this.name = name;
        }

        @Override
        public void read(int maxBytes, ActionListener<SpillRead> listener) {
            if (closed) {
                violation("read on the closed reader of [" + name + "]");
            }
            if (outstanding.getAndSet(true)) {
                violation("read with one outstanding on the reader of [" + name + "]");
            }
            ActionListener<SpillRead> done = ActionListener.runBefore(listener, () -> outstanding.set(false));
            Kind kind = maxBytes > 1 ? pick(Kind.READ, Kind.READ_SHORT) : pick(Kind.READ);
            if (kind == Kind.READ) {
                done.onFailure(new ElasticsearchException("cranky spill: read refused on [" + name + "]"));
                return;
            }
            // a short read makes frames straddle chunk boundaries on real drivers, not only in the unit harness
            int ask = kind == Kind.READ_SHORT ? shortRead(maxBytes) : maxBytes;
            delegate.read(ask, maybeDelayed(done));
        }

        @Override
        public void close(ActionListener<Void> listener) {
            closed = true;
            delegate.close(maybeDelayed(listener));
        }
    }
}
