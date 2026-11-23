/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * The {@link UsageReporter} that runs the cycle: at every wall-clock multiple of the period it drains the {@link UsageAccumulator},
 * builds the records, adds them to the {@link PendingStore} and publishes them in batches. It only depends on the interfaces of the
 * pieces, so each of them can be replaced without touching the schedule.
 * <p>
 * The invariants that keep the count safe: a snapshot is drained once and its records are in the store before anything is published,
 * so a failure of the publisher loses nothing that the store holds (a failure of the builder or the store after the drain loses that
 * period, with a warning); a record leaves the store only when it was accepted or rejected for good, so everything else is sent again
 * with the same id; and a failure of any piece ends the cycle but never the schedule. Whatever is lost on the way can only under-count.
 * <p>
 * {@link #start} is called once, by the node.
 */
public final class ScheduledUsageReporter implements UsageReporter {

    private static final Logger logger = LogManager.getLogger(ScheduledUsageReporter.class);

    private final UsageAccumulator accumulator;
    private final UsageRecordBuilder builder;
    private final PendingStore store;
    private final UsagePublisher publisher;
    private final SourceIdentity identity;
    private final Duration period;
    private final int batchSize;
    private final TimeValue flushTimeout;
    private final ThreadPool threadPool;
    private final Clock clock;

    /** Single flight: the one permit is held by the cycle that is running, or by the final flush. */
    private final Semaphore cycle = new Semaphore(1);
    private final AtomicBoolean closed = new AtomicBoolean();
    private Scheduler.Cancellable scheduled; // guarded by this

    private final AtomicLong recordsSent = new AtomicLong();
    private final AtomicLong ticksSkipped = new AtomicLong();

    public ScheduledUsageReporter(
        UsageReportingSettings settings,
        UsageAccumulator accumulator,
        UsageRecordBuilder builder,
        PendingStore store,
        UsagePublisher publisher,
        SourceIdentity identity,
        ThreadPool threadPool,
        Clock clock
    ) {
        this.accumulator = accumulator;
        this.builder = builder;
        this.store = store;
        this.publisher = publisher;
        this.identity = identity;
        this.period = Duration.ofMillis(settings.period().millis());
        this.batchSize = settings.batchSize();
        this.flushTimeout = settings.requestTimeout();
        this.threadPool = threadPool;
        this.clock = clock;
    }

    @Override
    public void start() {
        scheduleAfter(clock.instant());
    }

    /**
     * Runs on the generic executor at the boundary it was scheduled for. The boundary, not the instant at which the timer fired, is the
     * time of the cycle, so that the ids of the records are aligned and do not depend on the delay of the timer.
     */
    private void run(Instant boundary) {
        if (closed.get()) {
            return;
        }
        try {
            tick(boundary);
        } finally {
            // From the later of the two instants: a timer that fires a little early must not schedule the same boundary again.
            Instant now = clock.instant();
            try {
                scheduleAfter(now.isAfter(boundary) ? now : boundary);
            } catch (Exception e) {
                // Typically a thread pool that is shutting down; it must not replace what tick threw.
                logger.warn("failed to schedule the next usage cycle: usage is not reported until the node restarts", e);
            }
        }
    }

    private synchronized void scheduleAfter(Instant after) {
        if (closed.get()) {
            return;
        }
        Instant boundary = nextBoundary(after, period);
        long delayMillis = Math.max(0, Duration.between(clock.instant(), boundary).toMillis());
        scheduled = threadPool.schedule(() -> run(boundary), TimeValue.timeValueMillis(delayMillis), threadPool.generic());
    }

    /**
     * The first multiple of {@code period} after {@code instant}, counted from the epoch. Strictly after: at exactly a boundary the
     * next one is a period away, so that the cycle that has just run is never repeated with the same time, and therefore the same ids.
     * The period divides an hour, so the multiples fall on the same wall-clock minutes in every time zone.
     */
    static Instant nextBoundary(Instant instant, Duration period) {
        long periodMillis = period.toMillis();
        return Instant.ofEpochMilli((Math.floorDiv(instant.toEpochMilli(), periodMillis) + 1) * periodMillis);
    }

    @Override
    public void tick(Instant now) {
        if (cycle.tryAcquire() == false) {
            ticksSkipped.incrementAndGet();
            logger.debug("skipping the usage cycle of [{}]: the previous one is still running", now);
            return;
        }
        try {
            // A cycle that races with close() must not run after the final flush, when the publisher is about to be closed.
            if (closed.get() == false) {
                drainAndPublish(now, () -> closed.get() == false);
            }
        } finally {
            cycle.release();
        }
    }

    /**
     * One cycle. A failure of a piece is logged once and ends the cycle: what was drained is already in the store, and what was not
     * published stays there to be sent again with the same ids.
     */
    private void drainAndPublish(Instant now, BooleanSupplier mayContinue) {
        try {
            UsageSnapshot snapshot = accumulator.drain(now);
            if (snapshot.isEmpty() == false) {
                store.add(builder.build(snapshot, identity, period, now));
            }
        } catch (Exception e) {
            logger.warn("failed to build the usage records of the period ending at [{}]", now, e);
        }
        try {
            publishPending(mayContinue);
        } catch (Exception e) {
            logger.warn("failed to publish usage records: [{}] stay pending", store.size(), e);
        }
    }

    /**
     * Publishes batches until the store is empty, a failure says to stop, or a batch makes no progress. Progress is measured on the
     * store, not on what the publisher reports, so that a publisher that accepts nothing without a failure, or that names ids the store
     * does not hold, cannot make this loop ask for the same batch forever.
     */
    private void publishPending(BooleanSupplier mayContinue) {
        while (mayContinue.getAsBoolean()) {
            List<UsageRecord> batch = store.next(batchSize);
            if (batch.isEmpty()) {
                return;
            }
            int before = store.size();
            PublishResult result = publisher.publish(batch);
            // Only ids of this batch: a record that was not sent cannot have been accepted, whatever the publisher says.
            Set<String> batchIds = Set.copyOf(batch.stream().map(UsageRecord::id).toList());
            store.acknowledge(result.acceptedIds().stream().filter(batchIds::contains).collect(Collectors.toUnmodifiableSet()));
            recordsSent.addAndGet(before - store.size());
            boolean stop = false;
            for (PublishResult.Failure failure : result.failures()) {
                switch (failure.failureClass()) {
                    case INVALID -> store.drop(idsOf(failure, batchIds), failure.message());
                    case RETRYABLE -> {
                        logger.debug("publishing usage records failed, to be retried: {}", failure.message());
                        stop = true;
                    }
                    case AUTH -> {
                        // Lasts until someone fixes the identity or the certificate, so it must be visible at the default level.
                        logger.warn("the Usage API refused the usage records, which stay pending: {}", failure.message());
                        stop = true;
                    }
                }
            }
            if (stop || store.size() >= before) {
                return;
            }
        }
    }

    /** A failure without ids is a failure of the whole batch; ids of other records are ignored, as they were not sent. */
    private static Set<String> idsOf(PublishResult.Failure failure, Set<String> batchIds) {
        if (failure.recordIds().isEmpty()) {
            return batchIds;
        }
        return failure.recordIds().stream().filter(batchIds::contains).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Stops the schedule and makes one last cycle, so that the usage of the partial period and what is pending is not lost on a
     * restart. The flush waits at most the request timeout for a cycle that is still running, and stops starting batches after the same
     * timeout from the start of the close; a batch already in flight is bounded by the publisher's own request timeout, so the close
     * can take about twice as long. What is not sent is lost, which can only under-count, and is reported with a warning.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        synchronized (this) {
            if (scheduled != null) {
                scheduled.cancel();
            }
        }
        long deadline = System.nanoTime() + flushTimeout.nanos();
        boolean acquired = false;
        try {
            acquired = cycle.tryAcquire(flushTimeout.nanos(), TimeUnit.NANOSECONDS);
            if (acquired) {
                drainAndPublish(clock.instant(), () -> System.nanoTime() - deadline < 0);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (acquired) {
                cycle.release();
            }
        }
        int pending = store.size();
        if (acquired == false) {
            logger.warn("usage may be lost: the cycle that was running did not finish, so the final flush did not run");
        } else if (pending > 0) {
            logger.warn("usage may be lost: [{}] records are still pending after the final flush", pending);
        }
    }

    long recordsSent() {
        return recordsSent.get();
    }

    int recordsPending() {
        return store.size();
    }

    long ticksSkipped() {
        return ticksSkipped.get();
    }
}
